// bp35c2-collector opens a BP35C2 USB Wi-SUN dongle, joins the
// B-route, and periodically polls a low-voltage smart electric meter,
// fanning measurements out to stdout / InfluxDB / Prometheus sinks.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"go.bug.st/serial"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
	"github.com/Alicey0719/bp35c2-collector/internal/broute"
	"github.com/Alicey0719/bp35c2-collector/internal/collector"
	"github.com/Alicey0719/bp35c2-collector/internal/config"
	"github.com/Alicey0719/bp35c2-collector/internal/meter"
	"github.com/Alicey0719/bp35c2-collector/internal/metrics"
	"github.com/Alicey0719/bp35c2-collector/internal/sink"
)

// Populated at link time via -ldflags "-X main.version=... -X main.commit=..."
var (
	version = "dev"
	commit  = "none"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfgPath     string
		showVersion bool
	)
	flag.StringVar(&cfgPath, "config", "/etc/bp35c2-collector/config.yaml", "path to config YAML")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		fmt.Printf("bp35c2-collector %s (commit %s)\n", version, commit)
		return nil
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}

	log := newLogger(cfg.Log.Level)
	log.Info("bp35c2-collector starting",
		"config", cfgPath,
		"device", cfg.Serial.Device)

	// Root context: cancelled on SIGINT/SIGTERM.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Open serial port.
	port, err := openSerial(cfg.Serial.Device, cfg.Serial.Baud)
	if err != nil {
		return fmt.Errorf("open serial: %w", err)
	}
	defer port.Close()

	// 2. Bring up UART driver.
	drv := bp35c2.New(port, log, 64)
	defer drv.Close()

	// The SKSTACK-IP module is line-oriented; a "hard reset" step is
	// unnecessary here because broute.connectOnce starts with SKRESET
	// as part of the join sequence. Any state left over from a
	// previous session is wiped by that command.

	// 3. Set up Prometheus sink first so we can register self-metrics
	//    into its registry.
	sinks := []sink.Sink{}
	var promSink *sink.Prometheus
	var m *metrics.Metrics
	if cfg.Sinks.Prometheus.Enabled {
		p, err := sink.NewPrometheus(sink.PrometheusConfig{
			Listen: cfg.Sinks.Prometheus.Listen,
			CounterNames: []string{
				"smartmeter_energy_forward_kwh",
				"smartmeter_energy_reverse_kwh",
			},
		})
		if err != nil {
			return fmt.Errorf("prometheus sink: %w", err)
		}
		promSink = p
		m = metrics.New(p.Registry())
		sinks = append(sinks, p)
		log.Info("prometheus listener up", "addr", cfg.Sinks.Prometheus.Listen)
	}

	if cfg.Sinks.Stdout.Enabled {
		sinks = append(sinks, sink.NewStdout())
	}
	if cfg.Sinks.InfluxDB.Enabled {
		i, err := sink.NewInfluxDB(sink.InfluxDBConfig{
			URL:           cfg.Sinks.InfluxDB.URL,
			Token:         cfg.Sinks.InfluxDB.Token,
			Org:           cfg.Sinks.InfluxDB.Org,
			Bucket:        cfg.Sinks.InfluxDB.Bucket,
			Measurement:   cfg.Sinks.InfluxDB.Measurement,
			BatchSize:     cfg.Sinks.InfluxDB.BatchSize,
			FlushInterval: cfg.Sinks.InfluxDB.FlushInterval,
		}, func(err error) {
			log.Warn("influxdb write error", "err", err)
			if m != nil {
				m.SinkWriteErrors.WithLabelValues("influxdb").Inc()
			}
		})
		if err != nil {
			return fmt.Errorf("influxdb sink: %w", err)
		}
		var influxSink sink.Sink = i
		if cfg.Sinks.InfluxDB.Downsample.Window > 0 {
			agg := sink.Aggregation(cfg.Sinks.InfluxDB.Downsample.Aggregation)
			if agg == "" {
				agg = sink.AggMean
			}
			ds, err := sink.NewDownsampler(i, sink.DownsampleConfig{
				Window:      cfg.Sinks.InfluxDB.Downsample.Window,
				Aggregation: agg,
				CounterNames: []string{
					"smartmeter_energy_forward_kwh",
					"smartmeter_energy_reverse_kwh",
				},
			})
			if err != nil {
				return fmt.Errorf("influxdb downsampler: %w", err)
			}
			influxSink = ds
			log.Info("influxdb downsampling enabled",
				"window", cfg.Sinks.InfluxDB.Downsample.Window,
				"aggregation", agg)
		}
		sinks = append(sinks, influxSink)
	}
	if len(sinks) == 0 {
		return errors.New("no sinks enabled — check config")
	}
	fanout := sink.NewMulti(sinks, 500*time.Millisecond)
	fanout.OnError = func(name string, err error) {
		log.Warn("sink error", "sink", name, "err", err)
		if m != nil {
			m.SinkWriteErrors.WithLabelValues(name).Inc()
		}
	}

	// 4. B-route manager.
	bmgr := broute.NewManager(drv, broute.Config{
		BRouteID:       cfg.BRoute.ID,
		BRoutePassword: cfg.BRoute.Password,
		ScanDuration:   byte(cfg.BRoute.ScanTimeExp),
		ChannelMask:    cfg.BRoute.ChannelMask,
		JoinTimeout:    cfg.BRoute.PANAAuthTimeout,
		CommandTimeout: cfg.BRoute.CommandTimeout,
		InitialBackoff: cfg.BRoute.InitialBackoff,
		MaxBackoff:     cfg.BRoute.MaxBackoff,
	}, log)
	if m != nil {
		bmgr.OnReconnect = func() { m.Reconnects.Inc() }
		bmgr.OnAuthFailure = func() { m.PANAAuthFailures.Inc() }
		bmgr.OnStateChange = func(s broute.State) { m.SetState(int(s)) }
	}

	// 5. Meter client — dispatches ECHONET Lite responses.
	mc := meter.NewClient(bmgr, log)
	mc.GetTimeout = cfg.Poll.EchonetTimeout

	// 6. Collector.
	col := collector.New(mc, fanout, collector.Config{
		InstantInterval:    cfg.Poll.InstantInterval,
		CumulativeInterval: cfg.Poll.CumulativeInterval,
		ScheduledInterval:  cfg.Poll.ScheduledInterval,
		GetTimeout:         cfg.Poll.EchonetTimeout,
	}, log)
	if m != nil {
		col.OnGetError = func(_ byte, _ error) { m.GetErrors.WithLabelValues("batch").Inc() }
		col.OnGetSuccess = func() { m.MarkResponse() }
	}

	// 7. systemd notification (READY=1 + watchdog).
	sendReady(log)
	watchdogCancel := startWatchdog(ctx, log)
	defer watchdogCancel()

	// If the driver's read loop dies (serial fd broken, USB unplug,
	// scanner error) we can't recover in place — cancel the root
	// context so every goroutine tears down and the process exits.
	// systemd Restart=always brings us back with a fresh serial open.
	go func() {
		select {
		case <-drv.Done():
			if err := drv.ReadError(); err != nil {
				log.Error("bp35c2 driver terminated — exiting for restart", "err", err)
			}
			cancel()
		case <-ctx.Done():
		}
	}()

	// 8. Fan out goroutines.
	errCh := make(chan error, 3)
	go func() { errCh <- bmgr.Run(ctx) }()
	go func() { errCh <- mc.Run(ctx) }()
	go func() { errCh <- col.Run(ctx) }()

	// 9. Wait for the first goroutine to exit (usually ctx cancellation).
	firstErr := <-errCh
	log.Info("run loop exiting", "err", firstErr)
	// Signal others to stop.
	cancel()
	// Give them a moment to unwind.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer drainCancel()
	for range 2 {
		select {
		case <-errCh:
		case <-drainCtx.Done():
		}
	}
	// Flush sinks.
	_ = fanout.Close(drainCtx)
	if promSink != nil {
		_ = promSink.Close(drainCtx)
	}
	// STOPPING=1 for systemd.
	_, _ = daemon.SdNotify(false, daemon.SdNotifyStopping)
	return nil
}

func openSerial(device string, baud int) (bp35c2.ReadWriteCloser, error) {
	port, err := serial.Open(device, &serial.Mode{
		BaudRate: baud,
		DataBits: 8,
		Parity:   serial.NoParity,
		StopBits: serial.OneStopBit,
	})
	if err != nil {
		return nil, err
	}
	// Block Read until data arrives. A finite timeout produces
	// (0, nil) returns that bufio.Scanner interprets as "reader
	// broken", killing the driver mid-scan. Close of the underlying
	// fd is what interrupts a blocked Read at shutdown.
	_ = port.SetReadTimeout(serial.NoTimeout)
	return port, nil
}

func sendReady(log *slog.Logger) {
	sent, err := daemon.SdNotify(false, daemon.SdNotifyReady)
	if err != nil {
		log.Warn("sd_notify READY failed", "err", err)
	}
	if !sent {
		// Not launched under systemd — that's fine.
		log.Debug("sd_notify: not running under systemd")
	}
}

func startWatchdog(ctx context.Context, log *slog.Logger) context.CancelFunc {
	interval, err := daemon.SdWatchdogEnabled(false)
	if err != nil {
		log.Warn("sd_watchdog check failed", "err", err)
	}
	if interval == 0 {
		return func() {}
	}
	wdCtx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(interval / 2)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_, _ = daemon.SdNotify(false, daemon.SdNotifyWatchdog)
			case <-wdCtx.Done():
				return
			}
		}
	}()
	return cancel
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

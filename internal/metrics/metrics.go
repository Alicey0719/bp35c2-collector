// Package metrics defines the internal (self-monitoring) Prometheus
// metrics the daemon reports about itself. These are registered into
// the same registry the sink.Prometheus package hosts, so scrapes see
// smart-meter data and daemon health together.
package metrics

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the bundle of counters/gauges the daemon updates.
type Metrics struct {
	Reconnects        prometheus.Counter
	PANAAuthFailures  prometheus.Counter
	GetErrors         *prometheus.CounterVec // labels: epc
	SinkWriteErrors   *prometheus.CounterVec // labels: sink
	SessionState      prometheus.Gauge
	FrameChecksumErrs prometheus.Counter

	lastResponseUnix atomic.Int64
}

// New constructs and registers the metric family into reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bp35c2_reconnect_total",
			Help: "Total number of times the daemon initiated a B-route reconnect.",
		}),
		PANAAuthFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bp35c2_pana_auth_failures_total",
			Help: "Total number of PANA authentication failures (initial or auto-re-auth).",
		}),
		GetErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bp35c2_get_errors_total",
			Help: "Total ECHONET Lite Get failures, labelled by EPC or 'batch'.",
		}, []string{"epc"}),
		SinkWriteErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bp35c2_sink_write_errors_total",
			Help: "Total sink write errors, labelled by sink name.",
		}, []string{"sink"}),
		SessionState: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bp35c2_session_state",
			Help: "Current B-route session state (0=disconnected,1=init,2=scan,3=join,4=connected,5=reconnecting).",
		}),
		FrameChecksumErrs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bp35c2_frame_checksum_errors_total",
			Help: "Total UART frame checksum errors observed by the driver.",
		}),
	}
	reg.MustRegister(m.Reconnects)
	reg.MustRegister(m.PANAAuthFailures)
	reg.MustRegister(m.GetErrors)
	reg.MustRegister(m.SinkWriteErrors)
	reg.MustRegister(m.SessionState)
	reg.MustRegister(m.FrameChecksumErrs)

	// last_response_seconds is derived at scrape time so we don't need
	// a background updater.
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "bp35c2_last_response_seconds",
		Help: "Seconds since the meter last successfully replied. Large values indicate liveness trouble.",
	}, func() float64 {
		ts := m.lastResponseUnix.Load()
		if ts == 0 {
			return -1
		}
		return time.Since(time.Unix(0, ts)).Seconds()
	}))
	return m
}

// MarkResponse records that the meter just responded.
func (m *Metrics) MarkResponse() { m.lastResponseUnix.Store(time.Now().UnixNano()) }

// SetState updates the session-state gauge.
func (m *Metrics) SetState(s int) { m.SessionState.Set(float64(s)) }

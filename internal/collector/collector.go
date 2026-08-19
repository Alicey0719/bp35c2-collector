// Package collector is the tick scheduler and EPC batch dispatcher.
//
// Three tickers drive polling:
//
//   - InstantInterval    (default 10s): E7 + E8 + E9 in one Get.
//   - CumulativeInterval (default 60s): E0 + E3 + 88 in one Get.
//   - ScheduledInterval  (default 30m): EA + EB in one Get.
//
// Startup performs a one-shot probe: read the Get-property map (0x9F)
// to trim the batch to what the meter supports, plus 0xD3 (coefficient)
// and 0xE1 (unit) which are constant per meter.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/echonet"
	"github.com/Alicey0719/bp35c2-collector/internal/meter"
	"github.com/Alicey0719/bp35c2-collector/internal/sink"
)

// Config bundles polling intervals.
type Config struct {
	InstantInterval    time.Duration
	CumulativeInterval time.Duration
	ScheduledInterval  time.Duration
	GetTimeout         time.Duration
}

// Collector runs the polling loop.
type Collector struct {
	cli  *meter.Client
	sink sink.Sink
	cfg  Config
	log  *slog.Logger

	// oneShot properties.
	oneShotMu sync.RWMutex
	coeff     uint32
	unitByte  byte
	epcMap    map[byte]struct{}
	haveOneShot bool

	// unsupported EPCs discovered at runtime (PDC=0 in a Get response).
	// Skipped from future batches so we don't spam warnings for props
	// the meter never returns.
	unsupportedMu sync.RWMutex
	unsupported   map[byte]struct{}

	// hooks
	OnGetError    func(epc byte, err error)
	OnGetSuccess  func()
}

// New constructs a Collector.
func New(cli *meter.Client, s sink.Sink, cfg Config, log *slog.Logger) *Collector {
	if log == nil {
		log = slog.Default()
	}
	if cfg.InstantInterval == 0 {
		cfg.InstantInterval = 10 * time.Second
	}
	if cfg.CumulativeInterval == 0 {
		cfg.CumulativeInterval = 60 * time.Second
	}
	if cfg.ScheduledInterval == 0 {
		cfg.ScheduledInterval = 30 * time.Minute
	}
	if cfg.GetTimeout == 0 {
		cfg.GetTimeout = 5 * time.Second
	}
	return &Collector{cli: cli, sink: s, cfg: cfg, log: log.With("component", "collector")}
}

// Run drives the polling loop until ctx expires.
func (c *Collector) Run(ctx context.Context) error {
	// Probe retries in the background: the meter isn't reachable until
	// broute.Manager joins, and until we've read D3 (coefficient) and
	// E1 (unit) the cumulative kWh values scale wrong. Never give up
	// until ctx expires — a metered value that says "222355 kWh" is
	// worse than "no value yet".
	go c.probeUntilSuccess(ctx)

	instant := time.NewTicker(c.cfg.InstantInterval)
	cumulative := time.NewTicker(c.cfg.CumulativeInterval)
	scheduled := time.NewTicker(c.cfg.ScheduledInterval)
	defer instant.Stop()
	defer cumulative.Stop()
	defer scheduled.Stop()

	// Fire once immediately, then on tick.
	c.collectInstant(ctx)
	c.collectCumulative(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-instant.C:
			c.collectInstant(ctx)
		case <-cumulative.C:
			c.collectCumulative(ctx)
		case <-scheduled.C:
			c.collectScheduled(ctx)
		}
	}
}

// probeUntilSuccess keeps retrying the one-shot probe (property map +
// coefficient + unit) until it succeeds or ctx is cancelled. Retries
// use a fixed 15s gap so we don't spam the meter, but we never give up
// — cumulative kWh values are meaningless without the unit byte.
func (c *Collector) probeUntilSuccess(ctx context.Context) {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := c.probeOnce(ctx); err != nil {
			c.log.Warn("meter probe failed — retrying", "attempt", attempt, "err", err)
			select {
			case <-time.After(15 * time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		c.log.Info("meter probe succeeded", "attempts", attempt)
		return
	}
}

func (c *Collector) probeOnce(ctx context.Context) error {
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	f, err := c.cli.Get(pctx, meter.EPCGetPropertyMap, meter.EPCCoefficient, meter.EPCCumulativeUnit)
	if err != nil {
		return err
	}
	c.oneShotMu.Lock()
	defer c.oneShotMu.Unlock()
	for _, p := range f.Props {
		switch p.EPC {
		case meter.EPCGetPropertyMap:
			m, err := meter.DecodeGetPropertyMap(p.EDT)
			if err != nil {
				c.log.Warn("bad get property map", "err", err)
			} else {
				c.epcMap = m
				c.log.Info("probed Get property map", "size", len(m))
			}
		case meter.EPCCoefficient:
			if v, err := meter.DecodeCoefficient(p.EDT); err == nil {
				c.coeff = v
				c.log.Info("coefficient", "value", v)
			}
		case meter.EPCCumulativeUnit:
			if len(p.EDT) == 1 {
				c.unitByte = p.EDT[0]
				c.log.Info("cumulative unit", "byte", fmt.Sprintf("%#x", p.EDT[0]))
			}
		}
	}
	c.haveOneShot = true
	return nil
}

// supports reports whether epc is in the meter's advertised Get map
// AND is not on the runtime-discovered unsupported list.
func (c *Collector) supports(epc byte) bool {
	c.unsupportedMu.RLock()
	if _, unsup := c.unsupported[epc]; unsup {
		c.unsupportedMu.RUnlock()
		return false
	}
	c.unsupportedMu.RUnlock()

	c.oneShotMu.RLock()
	defer c.oneShotMu.RUnlock()
	if !c.haveOneShot || c.epcMap == nil {
		return true
	}
	_, ok := c.epcMap[epc]
	return ok
}

// markUnsupported records an EPC as not implemented by this meter so
// future collect batches skip it. Idempotent; logs only the first time.
func (c *Collector) markUnsupported(epc byte, reason string) {
	c.unsupportedMu.Lock()
	defer c.unsupportedMu.Unlock()
	if c.unsupported == nil {
		c.unsupported = make(map[byte]struct{})
	}
	if _, seen := c.unsupported[epc]; seen {
		return
	}
	c.unsupported[epc] = struct{}{}
	c.log.Info("meter reports EPC unsupported — dropped from future polls",
		"epc", fmt.Sprintf("%#x", epc), "reason", reason)
}

func (c *Collector) collectInstant(ctx context.Context) {
	epcs := []byte{}
	for _, e := range []byte{meter.EPCInstantPower, meter.EPCInstantCurrent, meter.EPCInstantVoltage} {
		if c.supports(e) {
			epcs = append(epcs, e)
		}
	}
	if len(epcs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.GetTimeout)
	defer cancel()
	f, err := c.cli.Get(cctx, epcs...)
	if err != nil {
		c.log.Warn("instant Get failed", "err", err)
		if c.OnGetError != nil {
			c.OnGetError(0, err)
		}
		return
	}
	if c.OnGetSuccess != nil {
		c.OnGetSuccess()
	}
	now := time.Now()
	for _, p := range f.Props {
		if len(p.EDT) == 0 {
			c.markUnsupported(p.EPC, "PDC=0 in Get response")
			continue
		}
		c.emitInstant(p, now)
	}
}

func (c *Collector) collectCumulative(ctx context.Context) {
	epcs := []byte{}
	for _, e := range []byte{meter.EPCCumulativeForward, meter.EPCCumulativeReverse, meter.EPCFaultStatus} {
		if c.supports(e) {
			epcs = append(epcs, e)
		}
	}
	if len(epcs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.GetTimeout)
	defer cancel()
	f, err := c.cli.Get(cctx, epcs...)
	if err != nil {
		c.log.Warn("cumulative Get failed", "err", err)
		if c.OnGetError != nil {
			c.OnGetError(0, err)
		}
		return
	}
	if c.OnGetSuccess != nil {
		c.OnGetSuccess()
	}
	now := time.Now()
	c.oneShotMu.RLock()
	coeff, unit := c.coeff, c.unitByte
	c.oneShotMu.RUnlock()
	for _, p := range f.Props {
		if len(p.EDT) == 0 {
			c.markUnsupported(p.EPC, "PDC=0 in Get response")
			continue
		}
		c.emitCumulative(p, now, coeff, unit)
	}
}

func (c *Collector) collectScheduled(ctx context.Context) {
	epcs := []byte{}
	for _, e := range []byte{meter.EPCScheduledCumForward, meter.EPCScheduledCumReverse} {
		if c.supports(e) {
			epcs = append(epcs, e)
		}
	}
	if len(epcs) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.GetTimeout)
	defer cancel()
	f, err := c.cli.Get(cctx, epcs...)
	if err != nil {
		c.log.Warn("scheduled Get failed", "err", err)
		if c.OnGetError != nil {
			c.OnGetError(0, err)
		}
		return
	}
	if c.OnGetSuccess != nil {
		c.OnGetSuccess()
	}
	c.oneShotMu.RLock()
	coeff, unit := c.coeff, c.unitByte
	c.oneShotMu.RUnlock()
	for _, p := range f.Props {
		if len(p.EDT) == 0 {
			c.markUnsupported(p.EPC, "PDC=0 in Get response")
			continue
		}
		c.emitScheduled(p, coeff, unit)
	}
}

func (c *Collector) emitInstant(p echonet.Property, at time.Time) {
	switch p.EPC {
	case meter.EPCInstantPower:
		v, err := meter.DecodeInstantPowerW(p.EDT)
		if err != nil {
			c.log.Warn("decode E7", "err", err)
			return
		}
		c.write(sink.Measurement{Timestamp: at, Name: "smartmeter_power_instant_w", Value: float64(v), Unit: "W"})
	case meter.EPCInstantCurrent:
		i, err := meter.DecodeInstantCurrent(p.EDT)
		if err != nil {
			c.log.Warn("decode E8", "err", err)
			return
		}
		c.write(sink.Measurement{
			Timestamp: at, Name: "smartmeter_current_instant_a",
			Value: i.RPhaseA, Unit: "A", Tags: map[string]string{"phase": "r"},
		})
		if i.HasT {
			c.write(sink.Measurement{
				Timestamp: at, Name: "smartmeter_current_instant_a",
				Value: i.TPhaseA, Unit: "A", Tags: map[string]string{"phase": "t"},
			})
		}
	case meter.EPCInstantVoltage:
		v, err := meter.DecodeInstantVoltage(p.EDT)
		if err != nil {
			c.log.Warn("decode E9", "err", err)
			return
		}
		c.write(sink.Measurement{
			Timestamp: at, Name: "smartmeter_voltage_instant_v",
			Value: v.RPhaseV, Unit: "V", Tags: map[string]string{"phase": "r"},
		})
		c.write(sink.Measurement{
			Timestamp: at, Name: "smartmeter_voltage_instant_v",
			Value: v.TPhaseV, Unit: "V", Tags: map[string]string{"phase": "t"},
		})
	default:
		c.log.Debug("unexpected EPC in instant batch", "epc", fmt.Sprintf("%#x", p.EPC), "pdc", len(p.EDT))
	}
}

func (c *Collector) emitCumulative(p echonet.Property, at time.Time, coeff uint32, unit byte) {
	switch p.EPC {
	case meter.EPCCumulativeForward:
		raw, err := meter.DecodeCumulativeRaw(p.EDT)
		if err != nil {
			return
		}
		kWh, err := meter.CumulativeKWh(raw, coeff, unit)
		if err != nil {
			return
		}
		c.write(sink.Measurement{Timestamp: at, Name: "smartmeter_energy_forward_kwh", Value: kWh, Unit: "kWh"})
	case meter.EPCCumulativeReverse:
		raw, err := meter.DecodeCumulativeRaw(p.EDT)
		if err != nil {
			return
		}
		kWh, err := meter.CumulativeKWh(raw, coeff, unit)
		if err != nil {
			return
		}
		c.write(sink.Measurement{Timestamp: at, Name: "smartmeter_energy_reverse_kwh", Value: kWh, Unit: "kWh"})
	case meter.EPCFaultStatus:
		fault, err := meter.DecodeFaultStatus(p.EDT)
		if err != nil {
			return
		}
		v := 0.0
		if fault {
			v = 1
		}
		c.write(sink.Measurement{Timestamp: at, Name: "smartmeter_fault", Value: v})
	}
}

func (c *Collector) emitScheduled(p echonet.Property, coeff uint32, unit byte) {
	switch p.EPC {
	case meter.EPCScheduledCumForward:
		s, err := meter.DecodeScheduledCumulative(p.EDT)
		if err != nil {
			return
		}
		kWh, err := meter.CumulativeKWh(s.Value, coeff, unit)
		if err != nil {
			return
		}
		c.write(sink.Measurement{Timestamp: s.At, Name: "smartmeter_energy_forward_scheduled_kwh", Value: kWh, Unit: "kWh"})
	case meter.EPCScheduledCumReverse:
		s, err := meter.DecodeScheduledCumulative(p.EDT)
		if err != nil {
			return
		}
		kWh, err := meter.CumulativeKWh(s.Value, coeff, unit)
		if err != nil {
			return
		}
		c.write(sink.Measurement{Timestamp: s.At, Name: "smartmeter_energy_reverse_scheduled_kwh", Value: kWh, Unit: "kWh"})
	}
}

func (c *Collector) write(m sink.Measurement) {
	// A local timeout: sinks each get their own budget inside Multi.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.sink.Write(ctx, m); err != nil {
		c.log.Warn("sink write error", "err", err)
	}
}

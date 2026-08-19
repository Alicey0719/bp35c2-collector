package sink

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Aggregation names the reduction applied to samples within a window.
type Aggregation string

const (
	AggMean Aggregation = "mean"
	AggMax  Aggregation = "max"
	AggMin  Aggregation = "min"
	AggLast Aggregation = "last"
	AggSum  Aggregation = "sum"
)

// Valid reports whether a is a known aggregation.
func (a Aggregation) Valid() bool {
	switch a {
	case AggMean, AggMax, AggMin, AggLast, AggSum:
		return true
	}
	return false
}

// DownsampleConfig configures a Downsampler.
type DownsampleConfig struct {
	// Window is the bucket size, e.g. 1 minute. Windows are aligned to
	// the unix epoch so bucket boundaries match across process restarts.
	Window time.Duration
	// Aggregation applied to gauge-typed metrics (default: mean).
	Aggregation Aggregation
	// CounterNames names metrics that are monotonic counters (e.g.
	// cumulative kWh). These always use AggLast regardless of
	// Aggregation, because averaging a cumulative counter is
	// meaningless.
	CounterNames []string
	// IdleFlushInterval is how often the background flusher emits any
	// window whose newest sample is older than 1.5 × Window (so a series
	// that stops receiving samples still emits its last aggregate).
	// Default: Window.
	IdleFlushInterval time.Duration
}

// Downsampler wraps a Sink and time-bucket-aggregates measurements
// before forwarding. Samples are grouped by (metric + tag-set) into
// windows aligned to the unix epoch; when a sample for a later window
// arrives, the previous window's aggregate is emitted downstream. A
// background flusher also emits stale windows so an inactive series
// does not sit indefinitely in the buffer.
type Downsampler struct {
	inner        Sink
	window       time.Duration
	agg          Aggregation
	counters     map[string]struct{}
	idleFlush    time.Duration

	mu   sync.Mutex
	bufs map[string]*windowBuf

	stopFlusher context.CancelFunc
}

type windowBuf struct {
	seriesKey string
	name      string
	unit      string
	tags      map[string]string
	winStart  time.Time // Unix-epoch-aligned start of the window
	samples   []float64
	lastVal   float64
	lastTS    time.Time
	updated   time.Time // wall-clock time of most recent Add
}

// NewDownsampler wraps inner with time-bucket aggregation. It starts a
// background goroutine that flushes idle windows; call Close to stop.
func NewDownsampler(inner Sink, cfg DownsampleConfig) (*Downsampler, error) {
	if cfg.Window <= 0 {
		return nil, fmt.Errorf("downsample: Window must be positive")
	}
	if cfg.Aggregation == "" {
		cfg.Aggregation = AggMean
	}
	if !cfg.Aggregation.Valid() {
		return nil, fmt.Errorf("downsample: unknown aggregation %q", cfg.Aggregation)
	}
	if cfg.IdleFlushInterval <= 0 {
		cfg.IdleFlushInterval = cfg.Window
	}
	d := &Downsampler{
		inner:     inner,
		window:    cfg.Window,
		agg:       cfg.Aggregation,
		idleFlush: cfg.IdleFlushInterval,
		bufs:      make(map[string]*windowBuf),
		counters:  make(map[string]struct{}, len(cfg.CounterNames)),
	}
	for _, n := range cfg.CounterNames {
		d.counters[n] = struct{}{}
	}
	fctx, cancel := context.WithCancel(context.Background())
	d.stopFlusher = cancel
	go d.backgroundFlush(fctx)
	return d, nil
}

// Name reports "downsample(<inner>)".
func (d *Downsampler) Name() string { return "downsample(" + d.inner.Name() + ")" }

// Write buckets m into its window and forwards the previous window's
// aggregate if this sample crosses a boundary.
func (d *Downsampler) Write(ctx context.Context, m Measurement) error {
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}
	winStart := m.Timestamp.Truncate(d.window)
	key := seriesKey(m.Name, m.Tags)

	d.mu.Lock()
	buf, ok := d.bufs[key]
	if ok && !buf.winStart.Equal(winStart) {
		// Sample belongs to a later window — emit the previous one.
		emitted := d.finishLocked(buf)
		delete(d.bufs, key)
		d.mu.Unlock()
		if emitted != nil {
			if err := d.inner.Write(ctx, *emitted); err != nil {
				// Continue: we still want to record the new sample.
				// The caller / OnError already handles per-sink errors.
				_ = err
			}
		}
		d.mu.Lock()
		buf = nil
		ok = false
	}
	if !ok {
		buf = &windowBuf{
			seriesKey: key,
			name:      m.Name,
			unit:      m.Unit,
			tags:      m.Tags,
			winStart:  winStart,
		}
		d.bufs[key] = buf
	}
	buf.samples = append(buf.samples, m.Value)
	buf.lastVal = m.Value
	buf.lastTS = m.Timestamp
	buf.updated = time.Now()
	d.mu.Unlock()
	return nil
}

// Close flushes every pending window to the inner sink, stops the
// background flusher, and closes the inner sink.
func (d *Downsampler) Close(ctx context.Context) error {
	if d.stopFlusher != nil {
		d.stopFlusher()
	}
	d.mu.Lock()
	pending := make([]Measurement, 0, len(d.bufs))
	for _, buf := range d.bufs {
		if m := d.finishLocked(buf); m != nil {
			pending = append(pending, *m)
		}
	}
	d.bufs = map[string]*windowBuf{}
	d.mu.Unlock()
	for _, m := range pending {
		_ = d.inner.Write(ctx, m)
	}
	return d.inner.Close(ctx)
}

// finishLocked computes the aggregate for buf. Caller must hold d.mu.
// May return nil if the buffer contains no samples.
func (d *Downsampler) finishLocked(buf *windowBuf) *Measurement {
	if len(buf.samples) == 0 {
		return nil
	}
	agg := d.agg
	if _, isCounter := d.counters[buf.name]; isCounter {
		agg = AggLast
	}
	var value float64
	switch agg {
	case AggMean:
		var sum float64
		for _, v := range buf.samples {
			sum += v
		}
		value = sum / float64(len(buf.samples))
	case AggMax:
		value = buf.samples[0]
		for _, v := range buf.samples[1:] {
			if v > value {
				value = v
			}
		}
	case AggMin:
		value = buf.samples[0]
		for _, v := range buf.samples[1:] {
			if v < value {
				value = v
			}
		}
	case AggSum:
		for _, v := range buf.samples {
			value += v
		}
	case AggLast:
		value = buf.lastVal
	}
	// Emit at the window start so consecutive windows have monotone
	// timestamps spaced by exactly d.window.
	return &Measurement{
		Timestamp: buf.winStart,
		Name:      buf.name,
		Value:     value,
		Unit:      buf.unit,
		Tags:      buf.tags,
	}
}

// backgroundFlush periodically emits windows that have not received a
// new sample for more than 1.5 × Window — this handles the case where
// a series stops updating (metric no longer reported by the meter,
// long disconnection) so the last bucket doesn't sit forever.
func (d *Downsampler) backgroundFlush(ctx context.Context) {
	t := time.NewTicker(d.idleFlush)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cutoff := time.Now().Add(-d.window - d.window/2)
			var toEmit []Measurement
			d.mu.Lock()
			for key, buf := range d.bufs {
				if buf.updated.Before(cutoff) {
					if m := d.finishLocked(buf); m != nil {
						toEmit = append(toEmit, *m)
					}
					delete(d.bufs, key)
				}
			}
			d.mu.Unlock()
			for _, m := range toEmit {
				_ = d.inner.Write(ctx, m)
			}
		}
	}
}

// tagsFingerprint is exported as a small helper for tests; it produces
// the same stable string used internally to key on (name, tag-set).
func tagsFingerprint(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteByte('|')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(tags[k])
	}
	return b.String()
}

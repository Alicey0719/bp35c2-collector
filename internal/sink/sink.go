// Package sink defines the Measurement fan-out interface plus three
// concrete outputs: stdout (JSON Lines), InfluxDB v2, and Prometheus.
//
// Sinks are wired behind a Multi that dispatches each Measurement to
// every enabled sink in parallel with a per-sink write budget so a
// stuck backend never blocks the collector.
package sink

import (
	"context"
	"sync"
	"time"
)

// Measurement is one data point produced by the collector.
type Measurement struct {
	Timestamp time.Time
	Name      string            // e.g. "smartmeter_power_instant_w"
	Value     float64
	Unit      string            // e.g. "W", "A", "V", "kWh"
	Tags      map[string]string // e.g. {"phase":"r"}
}

// Sink accepts measurements. Implementations must be safe for use from
// multiple goroutines.
type Sink interface {
	Name() string
	Write(ctx context.Context, m Measurement) error
	Close(ctx context.Context) error
}

// Multi is a fan-out sink: each incoming measurement is written to
// every child sink concurrently, with a per-sink write timeout.
type Multi struct {
	sinks   []Sink
	timeout time.Duration

	// OnError is invoked (nil-safe) whenever a child sink returns an
	// error. Used by internal metrics to count failures per sink.
	OnError func(sinkName string, err error)
}

// NewMulti bundles children into a Multi sink.
func NewMulti(children []Sink, timeout time.Duration) *Multi {
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	return &Multi{sinks: children, timeout: timeout}
}

// Name reports "multi".
func (m *Multi) Name() string { return "multi" }

// Write dispatches m to all children concurrently and waits for them
// all to finish or the per-child timeout to elapse.
func (m *Multi) Write(ctx context.Context, meas Measurement) error {
	var wg sync.WaitGroup
	for _, s := range m.sinks {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, m.timeout)
			defer cancel()
			if err := s.Write(cctx, meas); err != nil && m.OnError != nil {
				m.OnError(s.Name(), err)
			}
		}()
	}
	wg.Wait()
	return nil
}

// Close closes each child sink; errors are aggregated by the callback.
func (m *Multi) Close(ctx context.Context) error {
	for _, s := range m.sinks {
		if err := s.Close(ctx); err != nil && m.OnError != nil {
			m.OnError(s.Name(), err)
		}
	}
	return nil
}

package sink

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recSink struct {
	mu   sync.Mutex
	rcvd []Measurement
}

func (r *recSink) Name() string { return "rec" }
func (r *recSink) Write(_ context.Context, m Measurement) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rcvd = append(r.rcvd, m)
	return nil
}
func (r *recSink) Close(_ context.Context) error { return nil }

func (r *recSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rcvd)
}

func (r *recSink) snapshot() []Measurement {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Measurement, len(r.rcvd))
	copy(out, r.rcvd)
	return out
}

// bucketStart returns an epoch-aligned time N minutes past epoch.
func bucketStart(minute int) time.Time {
	return time.Unix(int64(minute)*60, 0).UTC()
}

func TestDownsampler_MeanAcrossWindow(t *testing.T) {
	rec := &recSink{}
	ds, err := NewDownsampler(rec, DownsampleConfig{
		Window:      time.Minute,
		Aggregation: AggMean,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Six samples in window minute=100
	for i, v := range []float64{100, 200, 300, 400, 500, 600} {
		_ = ds.Write(context.Background(), Measurement{
			Timestamp: bucketStart(100).Add(time.Duration(i*10) * time.Second),
			Name:      "smartmeter_power_instant_w", Value: v, Unit: "W",
		})
	}
	if rec.count() != 0 {
		t.Fatalf("expected no emit yet (all in same window); got %d", rec.count())
	}
	// One sample in the next window triggers the previous window's flush.
	_ = ds.Write(context.Background(), Measurement{
		Timestamp: bucketStart(101),
		Name:      "smartmeter_power_instant_w", Value: 1000, Unit: "W",
	})
	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(got))
	}
	if got[0].Value != 350 { // mean of 100..600
		t.Fatalf("mean: got %v want 350", got[0].Value)
	}
	if !got[0].Timestamp.Equal(bucketStart(100)) {
		t.Fatalf("timestamp: got %v want %v", got[0].Timestamp, bucketStart(100))
	}
	if got[0].Unit != "W" {
		t.Fatalf("unit: %q", got[0].Unit)
	}
}

func TestDownsampler_MaxAndMin(t *testing.T) {
	for _, tc := range []struct {
		agg  Aggregation
		want float64
	}{
		{AggMax, 600},
		{AggMin, 100},
		{AggSum, 2100},
	} {
		t.Run(string(tc.agg), func(t *testing.T) {
			rec := &recSink{}
			ds, _ := NewDownsampler(rec, DownsampleConfig{Window: time.Minute, Aggregation: tc.agg})
			for i, v := range []float64{100, 200, 300, 400, 500, 600} {
				_ = ds.Write(context.Background(), Measurement{
					Timestamp: bucketStart(200).Add(time.Duration(i*10) * time.Second),
					Name:      "x", Value: v,
				})
			}
			_ = ds.Write(context.Background(), Measurement{
				Timestamp: bucketStart(201), Name: "x", Value: 999,
			})
			got := rec.snapshot()
			if len(got) != 1 || got[0].Value != tc.want {
				t.Fatalf("got %+v, want value=%v", got, tc.want)
			}
		})
	}
}

func TestDownsampler_CounterForcesLast(t *testing.T) {
	rec := &recSink{}
	ds, _ := NewDownsampler(rec, DownsampleConfig{
		Window:       time.Minute,
		Aggregation:  AggMean, // Would give wrong answer for a counter
		CounterNames: []string{"smartmeter_energy_forward_kwh"},
	})
	// Ever-growing counter values
	for i, v := range []float64{100, 100.1, 100.2, 100.3, 100.4, 100.5} {
		_ = ds.Write(context.Background(), Measurement{
			Timestamp: bucketStart(300).Add(time.Duration(i*10) * time.Second),
			Name:      "smartmeter_energy_forward_kwh", Value: v,
		})
	}
	_ = ds.Write(context.Background(), Measurement{
		Timestamp: bucketStart(301), Name: "smartmeter_energy_forward_kwh", Value: 101,
	})
	got := rec.snapshot()
	if len(got) != 1 || got[0].Value != 100.5 {
		t.Fatalf("counter should aggregate as last, got %+v", got)
	}
}

func TestDownsampler_SeparatesByTagSet(t *testing.T) {
	rec := &recSink{}
	ds, _ := NewDownsampler(rec, DownsampleConfig{Window: time.Minute, Aggregation: AggMean})
	for i, v := range []float64{10, 20, 30} {
		_ = ds.Write(context.Background(), Measurement{
			Timestamp: bucketStart(400).Add(time.Duration(i*10) * time.Second),
			Name:      "smartmeter_current_instant_a", Value: v,
			Tags: map[string]string{"phase": "r"},
		})
	}
	for i, v := range []float64{100, 200, 300} {
		_ = ds.Write(context.Background(), Measurement{
			Timestamp: bucketStart(400).Add(time.Duration(i*10) * time.Second),
			Name:      "smartmeter_current_instant_a", Value: v,
			Tags: map[string]string{"phase": "t"},
		})
	}
	// Force flush by advancing past the window.
	_ = ds.Close(context.Background())
	got := rec.snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 emits (one per phase), got %d: %+v", len(got), got)
	}
	byPhase := map[string]float64{}
	for _, m := range got {
		byPhase[m.Tags["phase"]] = m.Value
	}
	if byPhase["r"] != 20 || byPhase["t"] != 200 {
		t.Fatalf("bad per-phase means: %+v", byPhase)
	}
}

func TestDownsampler_CloseFlushesPending(t *testing.T) {
	rec := &recSink{}
	ds, _ := NewDownsampler(rec, DownsampleConfig{Window: time.Minute, Aggregation: AggMean})
	_ = ds.Write(context.Background(), Measurement{
		Timestamp: bucketStart(500), Name: "x", Value: 42,
	})
	if rec.count() != 0 {
		t.Fatalf("early emit: %d", rec.count())
	}
	_ = ds.Close(context.Background())
	if rec.count() != 1 || rec.snapshot()[0].Value != 42 {
		t.Fatalf("close did not flush: %+v", rec.snapshot())
	}
}

func TestDownsampler_RejectsBadAggregation(t *testing.T) {
	rec := &recSink{}
	if _, err := NewDownsampler(rec, DownsampleConfig{Window: time.Minute, Aggregation: "bogus"}); err == nil {
		t.Fatal("expected error")
	}
	if _, err := NewDownsampler(rec, DownsampleConfig{Window: 0, Aggregation: AggMean}); err == nil {
		t.Fatal("expected error for zero window")
	}
}

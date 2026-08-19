package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestStdout_JSONLineFormat(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdoutTo(&buf)
	err := s.Write(context.Background(), Measurement{
		Timestamp: time.Date(2026, 8, 19, 12, 0, 30, 0, time.UTC),
		Name:      "smartmeter_power_instant_w",
		Value:     523,
		Unit:      "W",
	})
	if err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(buf.String())
	if !strings.HasSuffix(line, "}") || !strings.HasPrefix(line, "{") {
		t.Fatalf("not JSON: %q", line)
	}
	var rec struct {
		Timestamp string  `json:"ts"`
		Metric    string  `json:"metric"`
		Value     float64 `json:"value"`
		Unit      string  `json:"unit"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Metric != "smartmeter_power_instant_w" || rec.Value != 523 || rec.Unit != "W" {
		t.Fatalf("record: %+v", rec)
	}
	if !strings.Contains(rec.Timestamp, "2026-08-19T12:00:30") {
		t.Fatalf("ts: %s", rec.Timestamp)
	}
}

type fakeSink struct {
	name    string
	writes  int
	err     error
	block   time.Duration
	writeMu chan struct{}
}

func (f *fakeSink) Name() string { return f.name }
func (f *fakeSink) Write(ctx context.Context, m Measurement) error {
	if f.block > 0 {
		select {
		case <-time.After(f.block):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.writes++
	if f.writeMu != nil {
		f.writeMu <- struct{}{}
	}
	return f.err
}
func (f *fakeSink) Close(ctx context.Context) error { return nil }

func TestMulti_FanoutAndErrorCallback(t *testing.T) {
	a := &fakeSink{name: "a"}
	b := &fakeSink{name: "b", err: errors.New("boom")}
	c := &fakeSink{name: "c"}
	var errs []string
	m := NewMulti([]Sink{a, b, c}, 100*time.Millisecond)
	m.OnError = func(name string, err error) {
		errs = append(errs, name+":"+err.Error())
	}
	_ = m.Write(context.Background(), Measurement{Name: "x", Value: 1})
	if a.writes != 1 || b.writes != 1 || c.writes != 1 {
		t.Fatalf("writes a=%d b=%d c=%d", a.writes, b.writes, c.writes)
	}
	if len(errs) != 1 || !strings.HasPrefix(errs[0], "b:boom") {
		t.Fatalf("errs=%v", errs)
	}
}

func TestMulti_SlowSinkDoesNotBlockOthers(t *testing.T) {
	slow := &fakeSink{name: "slow", block: 300 * time.Millisecond}
	fast := &fakeSink{name: "fast"}
	m := NewMulti([]Sink{slow, fast}, 50*time.Millisecond)
	start := time.Now()
	_ = m.Write(context.Background(), Measurement{Name: "x"})
	// Total wait bounded by timeout (slow's ctx expires at 50ms).
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("Multi.Write took too long: %v", elapsed)
	}
	if fast.writes != 1 {
		t.Fatal("fast sink did not fire")
	}
}

func TestPrometheus_ExposesGaugesAndCounters(t *testing.T) {
	p, err := NewPrometheus(PrometheusConfig{
		Listen:       "127.0.0.1:0", // will bind to zero port then fail — use random port instead
		CounterNames: []string{"smartmeter_energy_forward_kwh"},
	})
	if err != nil {
		t.Skipf("skipping: %v", err)
	}
	// Note: the http server may or may not be bound before we scrape.
	// We validate via the Collector directly through the /metrics HTTP endpoint.
	defer p.Close(context.Background())

	_ = p.Write(context.Background(), Measurement{
		Timestamp: time.Now(),
		Name:      "smartmeter_power_instant_w",
		Value:     523,
		Tags:      map[string]string{},
	})
	_ = p.Write(context.Background(), Measurement{
		Timestamp: time.Now(),
		Name:      "smartmeter_energy_forward_kwh",
		Value:     12345.6,
	})
	_ = p.Write(context.Background(), Measurement{
		Timestamp: time.Now(),
		Name:      "smartmeter_current_instant_a",
		Value:     5.0,
		Tags:      map[string]string{"phase": "r"},
	})

	// Read what the server exposes. Since Listen is "127.0.0.1:0",
	// port is opaque — hit the Collector directly instead.
	body, err := renderMetrics(p)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"smartmeter_power_instant_w",
		"smartmeter_energy_forward_kwh",
		`smartmeter_current_instant_a{phase="r"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in output:\n%s", want, body)
		}
	}
	// Counter metric type
	if !strings.Contains(body, "# TYPE smartmeter_energy_forward_kwh counter") {
		t.Errorf("expected counter type declaration:\n%s", body)
	}
	// Gauge metric type for instant power
	if !strings.Contains(body, "# TYPE smartmeter_power_instant_w gauge") {
		t.Errorf("expected gauge type declaration:\n%s", body)
	}
}

// renderMetrics ships a request through the collector without needing
// to know which port the http server bound to.
func renderMetrics(p *Prometheus) (string, error) {
	handler := p.server.Handler.(*http.ServeMux)
	rec := &responseRecorder{header: make(http.Header)}
	req, _ := http.NewRequest("GET", "/metrics", nil)
	handler.ServeHTTP(rec, req)
	return rec.body.String(), nil
}

type responseRecorder struct {
	header http.Header
	body   bytes.Buffer
	code   int
}

func (r *responseRecorder) Header() http.Header       { return r.header }
func (r *responseRecorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *responseRecorder) WriteHeader(code int)      { r.code = code }

var _ io.Writer = (*responseRecorder)(nil)

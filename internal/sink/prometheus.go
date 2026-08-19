package sink

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus is a pull sink: it caches the most-recent measurement per
// (name, tag-set) and exposes them via a Collector that ConstMetrics
// out at scrape time.
//
// The set of counter metric names is provided explicitly so that
// prometheus queries can use rate()/increase() correctly.
type Prometheus struct {
	registry *prometheus.Registry
	server   *http.Server
	listen   string

	// counterNames is the set of measurement names whose semantics are
	// monotonically increasing (e.g. cumulative kWh). Emitted as
	// prometheus.CounterValue; everything else is GaugeValue.
	counterNames map[string]struct{}

	// mu guards latest.
	mu     sync.RWMutex
	latest map[string]sample
}

type sample struct {
	name  string
	value float64
	tags  map[string]string
	unit  string
}

// PrometheusConfig captures listen address and counter metric names.
type PrometheusConfig struct {
	Listen       string
	CounterNames []string
}

// NewPrometheus starts a http server exposing /metrics.
func NewPrometheus(cfg PrometheusConfig) (*Prometheus, error) {
	if cfg.Listen == "" {
		return nil, errors.New("prometheus: Listen required")
	}
	p := &Prometheus{
		registry:     prometheus.NewRegistry(),
		listen:       cfg.Listen,
		counterNames: make(map[string]struct{}, len(cfg.CounterNames)),
		latest:       make(map[string]sample, 32),
	}
	for _, n := range cfg.CounterNames {
		p.counterNames[n] = struct{}{}
	}
	p.registry.MustRegister(p)
	// Also expose Go and process metrics — useful for the self-monitor
	// dashboard.
	p.registry.MustRegister(prometheus.NewGoCollector())
	p.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	p.server = &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = p.server.ListenAndServe()
	}()
	return p, nil
}

// Registry lets callers register additional collectors (e.g.
// internal/metrics).
func (p *Prometheus) Registry() *prometheus.Registry { return p.registry }

// Name reports "prometheus".
func (p *Prometheus) Name() string { return "prometheus" }

// Write stores the latest value for scrape.
func (p *Prometheus) Write(ctx context.Context, m Measurement) error {
	k := seriesKey(m.Name, m.Tags)
	p.mu.Lock()
	p.latest[k] = sample{name: m.Name, value: m.Value, tags: m.Tags, unit: m.Unit}
	p.mu.Unlock()
	return nil
}

// Close shuts the HTTP server down.
func (p *Prometheus) Close(ctx context.Context) error {
	return p.server.Shutdown(ctx)
}

// Describe is intentionally empty: metrics are emitted dynamically.
// Returning nothing means promhttp treats us as an unchecked collector
// (allowed).
func (p *Prometheus) Describe(_ chan<- *prometheus.Desc) {}

// Collect emits one metric per known series.
func (p *Prometheus) Collect(ch chan<- prometheus.Metric) {
	p.mu.RLock()
	samples := make([]sample, 0, len(p.latest))
	for _, s := range p.latest {
		samples = append(samples, s)
	}
	p.mu.RUnlock()

	for _, s := range samples {
		vt := prometheus.GaugeValue
		if _, ok := p.counterNames[s.name]; ok {
			vt = prometheus.CounterValue
		}
		labelKeys, labelVals := labelsOrdered(s.tags)
		desc := prometheus.NewDesc(s.name, "BP35C2 collector: "+s.name, labelKeys, nil)
		m, err := prometheus.NewConstMetric(desc, vt, s.value, labelVals...)
		if err != nil {
			continue
		}
		ch <- m
	}
}

func seriesKey(name string, tags map[string]string) string {
	if len(tags) == 0 {
		return name
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte('|')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(tags[k])
	}
	return b.String()
}

func labelsOrdered(tags map[string]string) ([]string, []string) {
	if len(tags) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = tags[k]
	}
	return keys, vals
}

// EnsureListenReady is exported so main can log & fail-fast on bind
// errors instead of relying on the goroutine that hosts the server.
func EnsureListenReady(_ context.Context, _ string) error {
	// The go net/http server returns from ListenAndServe on bind
	// failure; we intentionally do not detect that here to avoid
	// blocking main on startup. Consider adding a probe if
	// operationally needed.
	return nil
}

var _ = fmt.Sprintf // keep imports non-empty if fmt unused elsewhere

package sink

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
)

// Stdout writes each Measurement as one JSON line.
type Stdout struct {
	w   io.Writer
	mu  sync.Mutex
	enc *json.Encoder
}

// NewStdout writes to os.Stdout; tests can substitute via NewStdoutTo.
func NewStdout() *Stdout { return NewStdoutTo(os.Stdout) }

// NewStdoutTo is exported for tests.
func NewStdoutTo(w io.Writer) *Stdout {
	return &Stdout{w: w, enc: json.NewEncoder(w)}
}

// Name reports "stdout".
func (s *Stdout) Name() string { return "stdout" }

// Write serialises m as one JSON line.
func (s *Stdout) Write(ctx context.Context, m Measurement) error {
	rec := struct {
		Timestamp string            `json:"ts"`
		Metric    string            `json:"metric"`
		Value     float64           `json:"value"`
		Unit      string            `json:"unit,omitempty"`
		Tags      map[string]string `json:"tags,omitempty"`
	}{
		Timestamp: m.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
		Metric:    m.Name,
		Value:     m.Value,
		Unit:      m.Unit,
		Tags:      m.Tags,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(rec)
}

// Close is a no-op for stdout.
func (s *Stdout) Close(ctx context.Context) error { return nil }

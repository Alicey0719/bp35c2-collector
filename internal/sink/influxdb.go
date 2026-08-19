package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	influx "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
	influxwrite "github.com/influxdata/influxdb-client-go/v2/api/write"
)

// InfluxDB is a sink that writes to an InfluxDB v2 server using the
// bundled batch writer. The writer batches internally; per-Measurement
// Write calls are cheap.
type InfluxDB struct {
	client   influx.Client
	writer   influxapi.WriteAPI
	errChan  <-chan error
	measurement string
	// close hooks
	done chan struct{}
}

// InfluxDBConfig captures the connection parameters.
type InfluxDBConfig struct {
	URL           string
	Token         string
	Org           string
	Bucket        string
	Measurement   string // defaults to "smartmeter"
	BatchSize     uint
	FlushInterval time.Duration
}

// NewInfluxDB creates and starts a non-blocking InfluxDB writer.
func NewInfluxDB(cfg InfluxDBConfig, onWriteError func(error)) (*InfluxDB, error) {
	if cfg.URL == "" {
		return nil, errors.New("influxdb: URL required")
	}
	if cfg.Bucket == "" || cfg.Org == "" {
		return nil, errors.New("influxdb: Org and Bucket required")
	}
	if cfg.Measurement == "" {
		cfg.Measurement = "smartmeter"
	}
	opts := influx.DefaultOptions()
	if cfg.BatchSize > 0 {
		opts = opts.SetBatchSize(cfg.BatchSize)
	}
	if cfg.FlushInterval > 0 {
		opts = opts.SetFlushInterval(uint(cfg.FlushInterval / time.Millisecond))
	}
	// Client-side retry handling: retain up to 5000 points, retry
	// interval 5s, max retry interval 60s.
	opts = opts.SetRetryInterval(5000).SetMaxRetryInterval(60000).SetRetryBufferLimit(5000)

	c := influx.NewClientWithOptions(cfg.URL, cfg.Token, opts)
	w := c.WriteAPI(cfg.Org, cfg.Bucket)
	errs := w.Errors()

	i := &InfluxDB{
		client:      c,
		writer:      w,
		errChan:     errs,
		measurement: cfg.Measurement,
		done:        make(chan struct{}),
	}
	// Pump errors to callback until Close.
	if onWriteError != nil {
		go func() {
			for {
				select {
				case err, ok := <-errs:
					if !ok {
						return
					}
					onWriteError(err)
				case <-i.done:
					return
				}
			}
		}()
	}
	return i, nil
}

// Name reports "influxdb".
func (i *InfluxDB) Name() string { return "influxdb" }

// Write hands the point to the batching writer. It never blocks on
// network I/O; the writer flushes in a background goroutine.
func (i *InfluxDB) Write(ctx context.Context, m Measurement) error {
	if len(m.Name) == 0 {
		return fmt.Errorf("influxdb: measurement name empty")
	}
	fields := map[string]interface{}{
		m.Name: m.Value,
	}
	p := influxwrite.NewPoint(i.measurement, m.Tags, fields, m.Timestamp)
	i.writer.WritePoint(p)
	return nil
}

// Close flushes pending points and shuts the client down. Blocks until
// the flush completes or ctx expires.
func (i *InfluxDB) Close(ctx context.Context) error {
	i.writer.Flush()
	close(i.done)
	// influxdb-client-go's Close is synchronous; call it in a
	// goroutine so we can honour ctx.
	done := make(chan struct{})
	go func() {
		i.client.Close()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

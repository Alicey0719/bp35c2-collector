package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const validYAML = `
serial:
  device: /dev/ttyUSB0
  baud: 115200
broute:
  id: "${BROUTE_ID}"
  password: "${BROUTE_PASSWORD}"
  state_dir: /tmp/state
poll:
  instant_interval: 5s
sinks:
  stdout: { enabled: true }
  influxdb:
    enabled: true
    url: http://influx:8086
    token: "${INFLUX_TOKEN}"
    org: home
    bucket: sm
  prometheus:
    enabled: true
    listen: ":9101"
log:
  level: debug
`

func TestLoad_EnvExpansionAndDefaults(t *testing.T) {
	t.Setenv("BROUTE_ID", "0123456789ABCDEF0123456789ABCDEF") // 32
	t.Setenv("BROUTE_PASSWORD", "PASSWORD1234")               // 12
	t.Setenv("INFLUX_TOKEN", "secret-token")

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(validYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.BRoute.ID != "0123456789ABCDEF0123456789ABCDEF" {
		t.Fatalf("id: %q", c.BRoute.ID)
	}
	if c.BRoute.Password != "PASSWORD1234" {
		t.Fatalf("pass: %q", c.BRoute.Password)
	}
	if c.Sinks.InfluxDB.Token != "secret-token" {
		t.Fatalf("token: %q", c.Sinks.InfluxDB.Token)
	}
	// Interval override applied
	if c.Poll.InstantInterval != 5*time.Second {
		t.Fatalf("instant: %v", c.Poll.InstantInterval)
	}
	// Defaults filled for unmentioned interval
	if c.Poll.CumulativeInterval != 60*time.Second {
		t.Fatalf("cumulative: %v", c.Poll.CumulativeInterval)
	}
}

func TestValidate_MissingCredentials(t *testing.T) {
	c := Default()
	c.BRoute.ID = "short"
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
	c.BRoute.ID = "0123456789ABCDEF0123456789ABCDEF"
	c.BRoute.Password = "short"
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidate_InfluxDBRequiresURL(t *testing.T) {
	c := Default()
	c.BRoute.ID = "0123456789ABCDEF0123456789ABCDEF"
	c.BRoute.Password = "PASSWORD1234"
	c.Sinks.InfluxDB.Enabled = true
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
	c.Sinks.InfluxDB.URL = "http://x"
	c.Sinks.InfluxDB.Org = "o"
	c.Sinks.InfluxDB.Bucket = "b"
	if err := c.Validate(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

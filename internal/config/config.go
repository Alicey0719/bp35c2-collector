// Package config loads the YAML config and applies env-var overrides.
//
// The file uses ${ENV_VAR} references for secrets (Route-B credentials,
// InfluxDB token); referring to secrets in env instead of the file
// keeps them out of the on-disk config while still allowing systemd
// EnvironmentFile= to inject them.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	Serial     Serial      `yaml:"serial"`
	BRoute     BRoute      `yaml:"broute"`
	Poll       Poll        `yaml:"poll"`
	Sinks      Sinks       `yaml:"sinks"`
	Log        Log         `yaml:"log"`
}

type Serial struct {
	Device string `yaml:"device"`
	Baud   int    `yaml:"baud"`
}

type BRoute struct {
	ID              string        `yaml:"id"`
	Password        string        `yaml:"password"`
	StateDir        string        `yaml:"state_dir"`
	ScanTimeExp     int           `yaml:"scan_time_exp"`
	ChannelMask     uint32        `yaml:"channel_mask"`
	PANAAuthTimeout time.Duration `yaml:"pana_auth_timeout"`
	CommandTimeout  time.Duration `yaml:"command_timeout"`
	InitialBackoff  time.Duration `yaml:"initial_backoff"`
	MaxBackoff      time.Duration `yaml:"max_backoff"`
}

type Poll struct {
	InstantInterval    time.Duration `yaml:"instant_interval"`
	CumulativeInterval time.Duration `yaml:"cumulative_interval"`
	ScheduledInterval  time.Duration `yaml:"scheduled_interval"`
	EchonetTimeout     time.Duration `yaml:"echonet_timeout"`
}

type Sinks struct {
	Stdout     StdoutSink     `yaml:"stdout"`
	InfluxDB   InfluxDBSink   `yaml:"influxdb"`
	Prometheus PrometheusSink `yaml:"prometheus"`
}

type StdoutSink struct {
	Enabled bool `yaml:"enabled"`
}

type InfluxDBSink struct {
	Enabled       bool             `yaml:"enabled"`
	URL           string           `yaml:"url"`
	Token         string           `yaml:"token"`
	Org           string           `yaml:"org"`
	Bucket        string           `yaml:"bucket"`
	Measurement   string           `yaml:"measurement"`
	BatchSize     uint             `yaml:"batch_size"`
	FlushInterval time.Duration    `yaml:"flush_interval"`
	Downsample    DownsampleConfig `yaml:"downsample"`
}

// DownsampleConfig reduces the volume of points written to InfluxDB by
// aggregating each series into fixed-length time windows. Leave
// `window` at zero to disable.
type DownsampleConfig struct {
	Window      time.Duration `yaml:"window"`      // e.g. 1m — 0 disables
	Aggregation string        `yaml:"aggregation"` // mean|max|min|last|sum (default mean)
}

type PrometheusSink struct {
	Enabled bool   `yaml:"enabled"`
	Listen  string `yaml:"listen"`
}

type Log struct {
	Level string `yaml:"level"`
}

// Default returns a Config populated with sensible defaults.
func Default() Config {
	return Config{
		Serial: Serial{Device: "/dev/ttyUSB0", Baud: 115200},
		BRoute: BRoute{
			StateDir:        "/var/lib/bp35c2-collector",
			ScanTimeExp:     6,
			ChannelMask:     0x0003FFF0,
			PANAAuthTimeout: 720 * time.Second,
			CommandTimeout:  6 * time.Second,
			InitialBackoff:  5 * time.Second,
			MaxBackoff:      300 * time.Second,
		},
		Poll: Poll{
			InstantInterval:    10 * time.Second,
			CumulativeInterval: 60 * time.Second,
			ScheduledInterval:  30 * time.Minute,
			EchonetTimeout:     5 * time.Second,
		},
		Sinks: Sinks{
			Stdout:     StdoutSink{Enabled: true},
			Prometheus: PrometheusSink{Enabled: false, Listen: ":9101"},
		},
		Log: Log{Level: "info"},
	}
}

// Load reads path, applies env-var substitution ${VAR} → os.Getenv(VAR),
// unmarshals, merges over defaults, and validates.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	expanded := expandEnv(string(raw))
	c := Default()
	if err := yaml.Unmarshal([]byte(expanded), &c); err != nil {
		return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate returns an error if required fields are missing / invalid.
// Errors have short, actionable messages — surface them at startup.
func (c Config) Validate() error {
	if c.Serial.Device == "" {
		return errors.New("config: serial.device required")
	}
	if c.Serial.Baud <= 0 {
		return errors.New("config: serial.baud must be positive")
	}
	if len(c.BRoute.ID) != 32 {
		return fmt.Errorf("config: broute.id must be 32 chars, got %d — is BROUTE_ID set?", len(c.BRoute.ID))
	}
	if len(c.BRoute.Password) != 12 {
		return fmt.Errorf("config: broute.password must be 12 chars, got %d — is BROUTE_PASSWORD set?", len(c.BRoute.Password))
	}
	if c.BRoute.StateDir == "" {
		return errors.New("config: broute.state_dir required")
	}
	if c.Sinks.InfluxDB.Enabled {
		if c.Sinks.InfluxDB.URL == "" || c.Sinks.InfluxDB.Org == "" || c.Sinks.InfluxDB.Bucket == "" {
			return errors.New("config: influxdb enabled but url/org/bucket missing")
		}
		if c.Sinks.InfluxDB.Downsample.Window > 0 {
			agg := c.Sinks.InfluxDB.Downsample.Aggregation
			if agg == "" {
				agg = "mean"
			}
			switch agg {
			case "mean", "max", "min", "last", "sum":
			default:
				return fmt.Errorf("config: influxdb.downsample.aggregation %q invalid (mean|max|min|last|sum)", agg)
			}
		}
	}
	if c.Sinks.Prometheus.Enabled && c.Sinks.Prometheus.Listen == "" {
		return errors.New("config: prometheus enabled but listen address missing")
	}
	return nil
}

// envRE matches ${VAR} and ${VAR:-default}. The default part is any
// run of characters that does not contain a closing brace — inline
// paths, ports, and URLs work; embedded ${...} does not.
var envRE = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// expandEnv replaces ${VAR} and ${VAR:-default} tokens.
//   - ${VAR}            → os.Getenv(VAR), empty if unset
//   - ${VAR:-default}   → os.Getenv(VAR) if non-empty, else default
//
// The "empty falls back to default" semantic matches POSIX shell and
// is what callers usually want (env vars that exist but are blank
// almost always indicate a config mistake rather than intent).
func expandEnv(s string) string {
	return envRE.ReplaceAllStringFunc(s, func(m string) string {
		// Use SubmatchIndex so we can distinguish "group 2 matched empty"
		// from "group 2 did not match" — plain FindStringSubmatch returns
		// "" for both.
		idx := envRE.FindStringSubmatchIndex(m)
		name := m[idx[2]:idx[3]]
		v := os.Getenv(name)
		hasDefault := idx[4] != -1
		if v == "" && hasDefault {
			return m[idx[4]:idx[5]]
		}
		return v
	})
}

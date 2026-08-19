package broute

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ChannelStore persists the last successful scan result so we can
// skip active scan on restart. On first-ever start (or when the
// stored channel stops working) we fall back to a full scan.
type ChannelStore struct {
	Path string
}

// Load returns the persisted channel, or (0, os.ErrNotExist) if none.
func (c ChannelStore) Load() (byte, error) {
	b, err := os.ReadFile(c.Path)
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	n, err := strconv.ParseUint(s, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("broute: parse channel %q: %w", s, err)
	}
	if n == 0 {
		return 0, errors.New("broute: stored channel is zero")
	}
	return byte(n), nil
}

// Save writes ch atomically.
func (c ChannelStore) Save(ch byte) error {
	if err := os.MkdirAll(filepath.Dir(c.Path), 0o755); err != nil {
		return err
	}
	tmp := c.Path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(int(ch))), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.Path)
}

// Clear removes the persisted channel. Ignores not-exist errors.
func (c ChannelStore) Clear() error {
	if err := os.Remove(c.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

package broute

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestChannelStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := ChannelStore{Path: filepath.Join(dir, "channel")}

	if _, err := s.Load(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected NotExist, got %v", err)
	}
	if err := s.Save(0x0C); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != 0x0C {
		t.Fatalf("got %#x, want %#x", got, 0x0C)
	}
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := s.Load(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("after clear, want NotExist, got %v", err)
	}
}

func TestChannelStore_RejectsZero(t *testing.T) {
	dir := t.TempDir()
	s := ChannelStore{Path: filepath.Join(dir, "channel")}
	if err := os.WriteFile(s.Path, []byte("0"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("expected error on zero channel")
	}
}

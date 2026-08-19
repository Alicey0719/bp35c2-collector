package broute

import (
	"bytes"
	"net"
	"testing"
)

func TestMacToLinkLocalIPv6_ULBitFlipped(t *testing.T) {
	mac := [8]byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}
	got := MacToLinkLocalIPv6(mac)
	want := net.ParseIP("fe80::1034:5678:9abc:def0")
	if !bytes.Equal(got, want.To16()) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestMacToLinkLocalIPv6_ZeroMacIsFlipped(t *testing.T) {
	// mac 00:00:00:00:00:00:00:00 → fe80::0200:0000:0000:0000
	// first byte becomes 0x02 after XOR
	mac := [8]byte{}
	got := MacToLinkLocalIPv6(mac)
	want := net.ParseIP("fe80::200:0:0:0")
	if !bytes.Equal(got, want.To16()) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

package broute

import (
	"strings"
	"testing"
)

func TestMacToLinkLocalIPv6(t *testing.T) {
	cases := []struct {
		mac  string
		want string
	}{
		// Real EPANDESC-derived meter MAC from a live BP35C2 join:
		{"008087003041692A", "fe80::280:8700:3041:692a"},
		{"001D129012341234", "fe80::21d:1290:1234:1234"},
		// All-zero MAC → first byte becomes 0x02 after XOR
		{"0000000000000000", "fe80::200:0:0:0"},
	}
	for _, tc := range cases {
		ip, err := macToLinkLocalIPv6(tc.mac)
		if err != nil {
			t.Errorf("macToLinkLocalIPv6(%q): %v", tc.mac, err)
			continue
		}
		if ip.String() != tc.want {
			t.Errorf("macToLinkLocalIPv6(%q) = %s, want %s", tc.mac, ip, tc.want)
		}
	}
}

func TestMacToLinkLocalIPv6_Invalid(t *testing.T) {
	if _, err := macToLinkLocalIPv6("shortmac"); err == nil {
		t.Fatal("expected error for short mac")
	}
	if _, err := macToLinkLocalIPv6("Z0000000000000ZZ"); err == nil {
		t.Fatal("expected error for non-hex")
	}
	if _, err := macToLinkLocalIPv6("  001D129012341234  "); err != nil {
		if !strings.Contains(err.Error(), "must be 16 hex") {
			t.Fatalf("expected trim to succeed; got %v", err)
		}
	}
}

package broute

import (
	"fmt"
	"net"
)

// MacToLinkLocalIPv6 converts an 8-byte IEEE 802.15.4 EUI-64 MAC (as
// reported by the BP35C2 in the CmdBRouteStart response) to its
// link-local IPv6 address.
//
// The transformation is fe80::<mac>, with the U/L bit of the first
// MAC byte inverted (XOR 0x02). Forgetting the flip makes PANA
// authenticate but every UDP send silently fail — hence a dedicated
// test.
func MacToLinkLocalIPv6(mac [8]byte) net.IP {
	ip := make(net.IP, 16)
	ip[0] = 0xFE
	ip[1] = 0x80
	// bytes [2..8) remain zero (link-local prefix)
	copy(ip[8:], mac[:])
	ip[8] ^= 0x02
	return ip
}

// ParseMac parses the 8-byte MAC as returned by CmdBRouteStart.
func ParseMac(b []byte) ([8]byte, error) {
	var m [8]byte
	if len(b) < 8 {
		return m, fmt.Errorf("broute: mac too short (%d)", len(b))
	}
	copy(m[:], b[:8])
	return m, nil
}

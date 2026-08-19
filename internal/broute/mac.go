package broute

import (
	"encoding/hex"
	"fmt"
	"net"
	"strings"
)

// macToLinkLocalIPv6 converts an 8-byte IEEE 802.15.4 EUI-64 MAC (as
// returned in EPANDESC's Addr field) into its link-local IPv6.
//
// Layout: fe80:: + <mac>, with the U/L bit (bit 1 of the first byte)
// inverted (XOR 0x02). Some SKSTACK firmwares expose the same
// computation through SKLL64, but that command doesn't emit an OK
// terminator on all firmwares — doing the arithmetic locally is both
// simpler and free of that quirk.
func macToLinkLocalIPv6(addrHex string) (net.IP, error) {
	addrHex = strings.TrimSpace(addrHex)
	if len(addrHex) != 16 {
		return nil, fmt.Errorf("broute: mac %q must be 16 hex chars", addrHex)
	}
	mac, err := hex.DecodeString(addrHex)
	if err != nil {
		return nil, fmt.Errorf("broute: mac %q: %w", addrHex, err)
	}
	ip := make(net.IP, 16)
	ip[0] = 0xFE
	ip[1] = 0x80
	// bytes [2..8) remain zero
	copy(ip[8:], mac)
	ip[8] ^= 0x02
	return ip, nil
}

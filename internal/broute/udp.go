package broute

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
	"github.com/Alicey0719/bp35c2-collector/internal/echonet"
)

// sendUDP sends payload via SKSENDTO to the meter over ECHONET port.
//
// SKSENDTO format:
//
//	SKSENDTO <handle=1> <ipaddr> <port> <sec=1> <side=0> <datalen> <data>
//
// The <data> is exactly datalen raw bytes appended after the trailing
// space. We use CommandBinary to write header + payload + CRLF as one
// syscall. The module answers OK (and emits EVENT 21 with the send
// result; we don't currently gate on it).
func (m *Manager) sendUDP(ctx context.Context, dst net.IP, payload []byte) error {
	if len(payload) == 0 {
		return errors.New("broute: empty payload")
	}
	if len(payload) > 1232 {
		return fmt.Errorf("broute: payload %d bytes exceeds UDP send limit (1232)", len(payload))
	}
	ip := dst.To16()
	if ip == nil {
		return fmt.Errorf("broute: bad destination address %v", dst)
	}
	header := fmt.Sprintf("SKSENDTO 1 %s %04X 1 0 %04X ",
		formatIPv6Upper(ip), echonet.Port, len(payload))

	// Doc says data send takes up to ~7s with ND involved. Give
	// ourselves headroom.
	sendCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	_, err := m.drv.CommandBinary(sendCtx, header, payload)
	if err != nil {
		return fmt.Errorf("broute: SKSENDTO: %w", err)
	}
	return nil
}

// formatIPv6Upper renders an IPv6 as SKSTACK expects: 8 groups of 4
// upper-case hex, colon-separated, no compression (no "::"). Some
// firmwares accept ::-compressed forms too, but the fully-expanded
// upper-case form is universally safe.
func formatIPv6Upper(ip net.IP) string {
	ip = ip.To16()
	return fmt.Sprintf("%02X%02X:%02X%02X:%02X%02X:%02X%02X:%02X%02X:%02X%02X:%02X%02X:%02X%02X",
		ip[0], ip[1], ip[2], ip[3], ip[4], ip[5], ip[6], ip[7],
		ip[8], ip[9], ip[10], ip[11], ip[12], ip[13], ip[14], ip[15],
	)
}

// parseIncoming turns an ERXUDP event into an Incoming record.
//
// ERXUDP layout (ROPT=1 hex mode):
//
//	<sender> <dest> <sport> <dport> <senderlladdr> <secured> <side> <datalen> <hexdata>
func parseIncoming(ev bp35c2.Event) (Incoming, bool) {
	if len(ev.Params) < 9 {
		return Incoming{}, false
	}
	sender := ev.Params[0]
	sportStr := ev.Params[2]
	ip := net.ParseIP(sender)
	if ip == nil {
		return Incoming{}, false
	}
	sport, err := parseHexU16(sportStr)
	if err != nil {
		return Incoming{}, false
	}
	if ev.Data == nil {
		// ROPT=0 mode not supported here; caller should have set ROPT 01.
		return Incoming{}, false
	}
	return Incoming{
		SrcIP:   ip,
		SrcPort: sport,
		Payload: ev.Data,
		At:      time.Now(),
	}, true
}

// parseHexU16 parses "0E1A" style ASCII hex.
func parseHexU16(s string) (uint16, error) {
	var v uint16
	for i := 0; i < len(s); i++ {
		c := s[i]
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'A' && c <= 'F':
			d = c - 'A' + 10
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		default:
			return 0, fmt.Errorf("bad hex byte %q", c)
		}
		v = (v << 4) | uint16(d)
	}
	return v, nil
}

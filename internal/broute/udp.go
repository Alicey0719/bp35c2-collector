package broute

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
	"github.com/Alicey0719/bp35c2-collector/internal/echonet"
)

// sendUDP builds a CmdUDPSend frame and waits for the module's response.
//
// CmdUDPSend request layout:
//
//	[dstIPv6 16B][srcPort 2B][dstPort 2B][dataSize 2B][data ...]
//
// Response 0x2008 layout:
//
//	[respResult 1B][txResult 1B][txDataSummary 1..5B]
//
// txResult upper nibble = queueing result, lower nibble = actual UDP
// send result. Lower nibble 0x0 = success; anything else is a failure
// worth propagating so the caller can retry or reconnect.
func (m *Manager) sendUDP(ctx context.Context, dst net.IP, payload []byte) error {
	if len(payload) == 0 {
		return errors.New("broute: empty payload")
	}
	if len(payload) > 1232 {
		return fmt.Errorf("broute: payload %d bytes exceeds UDP send limit (1232)", len(payload))
	}

	dst16 := dst.To16()
	if dst16 == nil {
		return fmt.Errorf("broute: bad destination address %v", dst)
	}
	buf := make([]byte, 0, 16+2+2+2+len(payload))
	buf = append(buf, dst16...)
	buf = binary.BigEndian.AppendUint16(buf, echonet.Port)
	buf = binary.BigEndian.AppendUint16(buf, echonet.Port)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(payload)))
	buf = append(buf, payload...)

	// Doc says data send takes up to ~7s with ND involved. Give
	// ourselves headroom above CommandTimeout for large replies.
	sendCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		sendCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	resp, err := m.drv.Request(sendCtx, bp35c2.CmdUDPSend, buf)
	if err != nil {
		return fmt.Errorf("broute: CmdUDPSend: %w", err)
	}
	if len(resp.Data) < 2 {
		return fmt.Errorf("broute: 0x2008 response too short (%d bytes)", len(resp.Data))
	}
	if resp.Data[0] != 0x01 {
		return fmt.Errorf("broute: CmdUDPSend rejected (respResult=%#x)", resp.Data[0])
	}
	if lower := resp.Data[1] & 0x0F; lower != 0x00 {
		return fmt.Errorf("broute: UDP send failed (txResult lower nibble=%#x)", lower)
	}
	return nil
}

// parseUDPReceive parses the 0x6018 notification payload.
//
//	[srcIP 16B][srcPort 2B][dstPort 2B][srcPAN 2B][dstType 1B]
//	[encrypted 1B][rssi 1B][rxSize 2B][rxData rxSize B]
func parseUDPReceive(data []byte) (Incoming, error) {
	const fixed = 16 + 2 + 2 + 2 + 1 + 1 + 1 + 2 // 27
	if len(data) < fixed {
		return Incoming{}, fmt.Errorf("0x6018: header too short (%d)", len(data))
	}
	rxSize := int(binary.BigEndian.Uint16(data[25:27]))
	if len(data) < fixed+rxSize {
		return Incoming{}, fmt.Errorf("0x6018: rx data truncated (want %d, have %d)", rxSize, len(data)-fixed)
	}
	inc := Incoming{
		SrcIP:   append(net.IP(nil), data[0:16]...),
		SrcPort: binary.BigEndian.Uint16(data[16:18]),
		RSSI:    int8(data[24]),
		Payload: append([]byte(nil), data[fixed:fixed+rxSize]...),
		At:      time.Now(),
	}
	return inc, nil
}

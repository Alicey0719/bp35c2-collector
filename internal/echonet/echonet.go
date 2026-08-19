// Package echonet implements the subset of ECHONET Lite framing needed
// to talk to a low-voltage smart electric meter (EOJ 0x028801) over the
// B-route.
//
// Frame layout (big-endian):
//
//	[EHD1 1B=0x10][EHD2 1B=0x81][TID 2B]
//	[SEOJ 3B][DEOJ 3B][ESV 1B][OPC 1B]
//	{[EPC 1B][PDC 1B][EDT PDC B]} × OPC
package echonet

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Standard header bytes.
const (
	EHD1 = 0x10
	EHD2 = 0x81
)

// UDP port used by ECHONET Lite on both ends.
const Port = 3610

// ESV values used in this project.
type ESV uint8

const (
	ESVSetI    ESV = 0x60
	ESVSetC    ESV = 0x61
	ESVGet     ESV = 0x62
	ESVINFReq  ESV = 0x63
	ESVSetGet  ESV = 0x6E
	ESVSetRes  ESV = 0x71
	ESVGetRes  ESV = 0x72
	ESVINF     ESV = 0x73
	ESVINFC    ESV = 0x74
	ESVINFCRes ESV = 0x7A
	ESVSetGetRes ESV = 0x7E
	ESVSetISNA ESV = 0x50
	ESVSetCSNA ESV = 0x51
	ESVGetSNA  ESV = 0x52
	ESVINFSNA  ESV = 0x53
	ESVSetGetSNA ESV = 0x5E
)

// IsSNA reports whether e is a service-not-available (error) response.
func (e ESV) IsSNA() bool {
	return e >= 0x50 && e <= 0x5F
}

// EOJ is a 3-byte ECHONET Object identifier (class group + class + instance).
type EOJ [3]byte

// Well-known EOJs.
var (
	// Low-voltage smart electric meter: class group 0x02, class 0x88, instance 1.
	EOJSmartMeter = EOJ{0x02, 0x88, 0x01}
	// Controller (HEMS-side): class group 0x05, class 0xFF, instance 1.
	EOJController = EOJ{0x05, 0xFF, 0x01}
)

// Property is a single EPC/EDT pair inside a frame.
type Property struct {
	EPC byte
	EDT []byte // empty for Get requests
}

// Frame is a decoded ECHONET Lite frame.
type Frame struct {
	TID        uint16
	SEOJ, DEOJ EOJ
	ESV        ESV
	Props      []Property
}

// Errors returned by Decode.
var (
	ErrTooShort    = errors.New("echonet: frame too short")
	ErrBadEHD      = errors.New("echonet: unsupported EHD")
	ErrTruncated   = errors.New("echonet: property list truncated")
	ErrPropOverflow = errors.New("echonet: property extends beyond frame")
)

// Encode serialises f. Callers must set TID; SEOJ/DEOJ/ESV/Props must be
// set consistently (this function does no semantic validation).
func Encode(f Frame) []byte {
	// Compute size upfront.
	size := 2 + 2 + 3 + 3 + 1 + 1 // EHD + TID + SEOJ + DEOJ + ESV + OPC
	for _, p := range f.Props {
		size += 2 + len(p.EDT)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, EHD1, EHD2)
	buf = binary.BigEndian.AppendUint16(buf, f.TID)
	buf = append(buf, f.SEOJ[:]...)
	buf = append(buf, f.DEOJ[:]...)
	buf = append(buf, byte(f.ESV))
	buf = append(buf, byte(len(f.Props)))
	for _, p := range f.Props {
		buf = append(buf, p.EPC, byte(len(p.EDT)))
		buf = append(buf, p.EDT...)
	}
	return buf
}

// Decode parses a raw ECHONET Lite frame.
func Decode(b []byte) (Frame, error) {
	if len(b) < 12 { // EHD(2)+TID(2)+SEOJ(3)+DEOJ(3)+ESV(1)+OPC(1)
		return Frame{}, ErrTooShort
	}
	if b[0] != EHD1 || b[1] != EHD2 {
		return Frame{}, fmt.Errorf("%w: %02x %02x", ErrBadEHD, b[0], b[1])
	}
	f := Frame{
		TID: binary.BigEndian.Uint16(b[2:4]),
		ESV: ESV(b[10]),
	}
	copy(f.SEOJ[:], b[4:7])
	copy(f.DEOJ[:], b[7:10])
	opc := int(b[11])
	f.Props = make([]Property, 0, opc)
	off := 12
	for i := 0; i < opc; i++ {
		if off+2 > len(b) {
			return Frame{}, ErrTruncated
		}
		epc := b[off]
		pdc := int(b[off+1])
		off += 2
		if off+pdc > len(b) {
			return Frame{}, ErrPropOverflow
		}
		var edt []byte
		if pdc > 0 {
			edt = append([]byte(nil), b[off:off+pdc]...)
		}
		off += pdc
		f.Props = append(f.Props, Property{EPC: epc, EDT: edt})
	}
	return f, nil
}

// NewGetRequest builds a Get frame for the given DEOJ and EPC list.
// TID identifies the request; the caller should increment it per outgoing
// request so responses can be correlated.
func NewGetRequest(tid uint16, dest EOJ, epcs ...byte) Frame {
	props := make([]Property, len(epcs))
	for i, e := range epcs {
		props[i] = Property{EPC: e}
	}
	return Frame{
		TID:   tid,
		SEOJ:  EOJController,
		DEOJ:  dest,
		ESV:   ESVGet,
		Props: props,
	}
}

// NewSetCRequest builds a SetC frame that expects a response.
func NewSetCRequest(tid uint16, dest EOJ, epc byte, edt []byte) Frame {
	return Frame{
		TID:   tid,
		SEOJ:  EOJController,
		DEOJ:  dest,
		ESV:   ESVSetC,
		Props: []Property{{EPC: epc, EDT: append([]byte(nil), edt...)}},
	}
}

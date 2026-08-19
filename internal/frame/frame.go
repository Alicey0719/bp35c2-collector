// Package frame implements BP35C0-J11 / BP35C2 UART binary framing.
//
// Frame layout (big-endian throughout):
//
//	[Unique 4B][Command 2B][MsgLen 2B][HeaderCS 2B][DataCS 2B][Data 0..N B]
//
// Unique code differs by direction: host→module uses UniqueRequest,
// module→host uses UniqueResponse (both responses and notifications).
// MsgLen counts HeaderCS + DataCS + Data (i.e. total frame minus the
// first 8 bytes). HeaderCS = 16-bit sum (mod 2^16) of Unique+Command+MsgLen
// bytes. DataCS = 16-bit sum of data bytes; 0x0000 when Data is empty.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderSize = 12

	// UniqueRequest is the 4-byte preamble for host→module frames.
	UniqueRequestB0 = 0xD0
	UniqueRequestB1 = 0xEA
	UniqueRequestB2 = 0x83
	UniqueRequestB3 = 0xFC

	// UniqueResponse is the 4-byte preamble for module→host frames.
	UniqueResponseB0 = 0xD0
	UniqueResponseB1 = 0xF9
	UniqueResponseB2 = 0xEE
	UniqueResponseB3 = 0x5D

	MaxDataLen = 1361 - HeaderSize // per UART spec §2.6 (max receive 1361 B)
)

var (
	UniqueRequest  = [4]byte{UniqueRequestB0, UniqueRequestB1, UniqueRequestB2, UniqueRequestB3}
	UniqueResponse = [4]byte{UniqueResponseB0, UniqueResponseB1, UniqueResponseB2, UniqueResponseB3}
)

// Direction identifies which unique code a frame carries.
type Direction uint8

const (
	DirectionRequest  Direction = iota // host → module
	DirectionResponse                  // module → host (both replies and notifications)
)

// Frame is a decoded BP35C2 frame.
type Frame struct {
	Direction Direction
	Command   uint16
	Data      []byte
}

// Errors returned by Decode / Read.
var (
	ErrShortFrame     = errors.New("frame: short frame")
	ErrUnknownUnique  = errors.New("frame: unknown unique code")
	ErrHeaderChecksum = errors.New("frame: bad header checksum")
	ErrDataChecksum   = errors.New("frame: bad data checksum")
	ErrDataTooLarge   = errors.New("frame: data length exceeds maximum")
	ErrLengthMismatch = errors.New("frame: message length does not match data")
)

// sum16 returns the 16-bit unsigned sum of the byte slice, mod 2^16.
func sum16(b []byte) uint16 {
	var s uint32
	for _, x := range b {
		s += uint32(x)
	}
	return uint16(s)
}

// Encode serialises f into a byte slice ready for UART transmission.
func Encode(f Frame) ([]byte, error) {
	if len(f.Data) > MaxDataLen {
		return nil, ErrDataTooLarge
	}
	var unique [4]byte
	switch f.Direction {
	case DirectionRequest:
		unique = UniqueRequest
	case DirectionResponse:
		unique = UniqueResponse
	default:
		return nil, fmt.Errorf("frame: invalid direction %d", f.Direction)
	}

	msgLen := uint16(4 + len(f.Data)) // HeaderCS(2) + DataCS(2) + data
	buf := make([]byte, HeaderSize+len(f.Data))
	copy(buf[0:4], unique[:])
	binary.BigEndian.PutUint16(buf[4:6], f.Command)
	binary.BigEndian.PutUint16(buf[6:8], msgLen)

	headerCS := sum16(buf[0:8])
	binary.BigEndian.PutUint16(buf[8:10], headerCS)

	var dataCS uint16
	if len(f.Data) > 0 {
		dataCS = sum16(f.Data)
	}
	binary.BigEndian.PutUint16(buf[10:12], dataCS)
	copy(buf[HeaderSize:], f.Data)
	return buf, nil
}

// Decode parses a single fully-buffered frame. It does not resync on
// misaligned unique codes — use Reader for a stream.
func Decode(b []byte) (Frame, error) {
	if len(b) < HeaderSize {
		return Frame{}, ErrShortFrame
	}
	var dir Direction
	switch {
	case b[0] == UniqueRequestB0 && b[1] == UniqueRequestB1 && b[2] == UniqueRequestB2 && b[3] == UniqueRequestB3:
		dir = DirectionRequest
	case b[0] == UniqueResponseB0 && b[1] == UniqueResponseB1 && b[2] == UniqueResponseB2 && b[3] == UniqueResponseB3:
		dir = DirectionResponse
	default:
		return Frame{}, ErrUnknownUnique
	}
	cmd := binary.BigEndian.Uint16(b[4:6])
	msgLen := binary.BigEndian.Uint16(b[6:8])
	if msgLen < 4 {
		return Frame{}, ErrLengthMismatch
	}
	dataLen := int(msgLen) - 4
	if dataLen > MaxDataLen {
		return Frame{}, ErrDataTooLarge
	}
	if len(b) < HeaderSize+dataLen {
		return Frame{}, ErrShortFrame
	}
	if got, want := binary.BigEndian.Uint16(b[8:10]), sum16(b[0:8]); got != want {
		return Frame{}, ErrHeaderChecksum
	}
	data := b[HeaderSize : HeaderSize+dataLen]
	wantDataCS := uint16(0)
	if dataLen > 0 {
		wantDataCS = sum16(data)
	}
	if got := binary.BigEndian.Uint16(b[10:12]); got != wantDataCS {
		return Frame{}, ErrDataChecksum
	}
	out := Frame{Direction: dir, Command: cmd}
	if dataLen > 0 {
		out.Data = append([]byte(nil), data...)
	}
	return out, nil
}

// Reader reads well-formed frames from an underlying byte stream,
// resynchronising on the unique-code preamble as needed.
type Reader struct {
	r   io.Reader
	buf []byte
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, buf: make([]byte, 0, 256)}
}

// isUniqueStart reports whether b begins with a known unique code.
// It returns (matched, direction). If matched is false the direction is
// meaningless.
func isUniqueStart(b []byte) (bool, Direction) {
	if len(b) < 4 {
		return false, 0
	}
	if b[0] == UniqueRequestB0 && b[1] == UniqueRequestB1 && b[2] == UniqueRequestB2 && b[3] == UniqueRequestB3 {
		return true, DirectionRequest
	}
	if b[0] == UniqueResponseB0 && b[1] == UniqueResponseB1 && b[2] == UniqueResponseB2 && b[3] == UniqueResponseB3 {
		return true, DirectionResponse
	}
	return false, 0
}

// looksLikeUniquePrefix reports whether b is a strict prefix (len 1..3)
// of a known unique code. Used to decide whether keeping partial bytes
// is worthwhile during resync.
func looksLikeUniquePrefix(b []byte) bool {
	if len(b) == 0 || len(b) > 3 {
		return false
	}
	for _, u := range [][4]byte{UniqueRequest, UniqueResponse} {
		match := true
		for i, x := range b {
			if u[i] != x {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// Read blocks until the next well-formed frame is available and returns
// it. Bytes that fail unique-code alignment are discarded silently.
// A checksum failure is returned so the caller can log/metric it, and
// Reader resynchronises past that frame's unique code.
func (r *Reader) Read() (Frame, error) {
	tmp := make([]byte, 256)
	for {
		// Skip bytes that cannot start a valid frame. When the buffer has
		// fewer than 4 bytes, keep any suffix that could still be the
		// beginning of a unique code (we might just need more data).
		for len(r.buf) > 0 {
			if len(r.buf) >= 4 {
				if ok, _ := isUniqueStart(r.buf); ok {
					break
				}
			} else if looksLikeUniquePrefix(r.buf) {
				break
			}
			r.buf = r.buf[1:]
		}

		if len(r.buf) < HeaderSize {
			n, err := r.r.Read(tmp)
			if n > 0 {
				r.buf = append(r.buf, tmp[:n]...)
			}
			if err != nil {
				return Frame{}, err
			}
			continue
		}

		msgLen := binary.BigEndian.Uint16(r.buf[6:8])
		if msgLen < 4 || int(msgLen)-4 > MaxDataLen {
			// Bogus length — skip past this unique code and resync.
			r.buf = r.buf[4:]
			continue
		}
		total := HeaderSize + int(msgLen) - 4
		for len(r.buf) < total {
			n, err := r.r.Read(tmp)
			if n > 0 {
				r.buf = append(r.buf, tmp[:n]...)
			}
			if err != nil {
				return Frame{}, err
			}
		}
		f, err := Decode(r.buf[:total])
		// Advance past this frame regardless: on checksum errors we should
		// not re-parse the same bytes forever.
		r.buf = r.buf[total:]
		if err != nil {
			return Frame{}, err
		}
		return f, nil
	}
}

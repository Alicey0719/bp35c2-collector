package frame

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

// initialSettingsHex is the AN F3 example: mode=Dual(0x05), HAN sleep=0,
// ch=0x04, tx=0. It also validates the header/data checksums quoted in
// the documentation.
const initialSettingsHex = "d0ea83fc005f000803a00009050004 00"

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	// strip whitespace
	var b []byte
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' {
			continue
		}
		b = append(b, byte(r))
	}
	out, err := hex.DecodeString(string(b))
	if err != nil {
		t.Fatalf("hex: %v", err)
	}
	return out
}

func TestEncodeGolden_InitialSettings(t *testing.T) {
	want := mustHex(t, initialSettingsHex)
	got, err := Encode(Frame{
		Direction: DirectionRequest,
		Command:   0x005F,
		Data:      []byte{0x05, 0x00, 0x04, 0x00},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded mismatch\n got=%x\nwant=%x", got, want)
	}
}

func TestEncodeGolden_ResetNoData(t *testing.T) {
	// Hardware reset 0x00D9, no payload.
	// header CS = D0+EA+83+FC+00+D9+00+04 = 0x0416
	// data CS = 0x0000
	want := mustHex(t, "d0ea83fc00d9000404160000")
	got, err := Encode(Frame{Direction: DirectionRequest, Command: 0x00D9})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded mismatch\n got=%x\nwant=%x", got, want)
	}
}

func TestDecodeGolden_InitialSettings(t *testing.T) {
	in := mustHex(t, initialSettingsHex)
	f, err := Decode(in)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if f.Direction != DirectionRequest {
		t.Fatalf("dir: got %d want request", f.Direction)
	}
	if f.Command != 0x005F {
		t.Fatalf("cmd: got %#x want 0x005F", f.Command)
	}
	if !bytes.Equal(f.Data, []byte{0x05, 0x00, 0x04, 0x00}) {
		t.Fatalf("data: got %x", f.Data)
	}
}

func TestRoundTrip(t *testing.T) {
	cases := []Frame{
		{Direction: DirectionRequest, Command: 0x0056},
		{Direction: DirectionResponse, Command: 0x6028, Data: []byte{0x01, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11}},
		{Direction: DirectionRequest, Command: 0x0008, Data: bytes.Repeat([]byte{0xAB}, 200)},
	}
	for _, c := range cases {
		enc, err := Encode(c)
		if err != nil {
			t.Fatalf("Encode(%v): %v", c, err)
		}
		got, err := Decode(enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.Direction != c.Direction || got.Command != c.Command || !bytes.Equal(got.Data, c.Data) {
			t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, c)
		}
	}
}

func TestDecodeErrors(t *testing.T) {
	good := mustHex(t, initialSettingsHex)

	// Corrupt header checksum.
	corruptHdr := append([]byte(nil), good...)
	corruptHdr[8] ^= 0xFF
	if _, err := Decode(corruptHdr); !errors.Is(err, ErrHeaderChecksum) {
		t.Fatalf("want ErrHeaderChecksum, got %v", err)
	}

	// Corrupt data checksum.
	corruptData := append([]byte(nil), good...)
	corruptData[10] ^= 0xFF
	if _, err := Decode(corruptData); !errors.Is(err, ErrDataChecksum) {
		t.Fatalf("want ErrDataChecksum, got %v", err)
	}

	// Unknown unique code.
	unknown := append([]byte(nil), good...)
	unknown[0] = 0x00
	if _, err := Decode(unknown); !errors.Is(err, ErrUnknownUnique) {
		t.Fatalf("want ErrUnknownUnique, got %v", err)
	}

	// Truncated.
	if _, err := Decode(good[:6]); !errors.Is(err, ErrShortFrame) {
		t.Fatalf("want ErrShortFrame, got %v", err)
	}
}

func TestReader_HandlesGarbageAndSplitReads(t *testing.T) {
	f1, _ := Encode(Frame{Direction: DirectionResponse, Command: 0x6019})
	f2, _ := Encode(Frame{Direction: DirectionResponse, Command: 0x2053, Data: []byte{0x01, 0x02, 0x03}})

	stream := bytes.NewBuffer(nil)
	stream.Write([]byte{0x00, 0xFF, 0xAA}) // pre-garbage
	stream.Write(f1[:5])                   // split frame 1
	stream.Write(f1[5:])
	stream.Write([]byte{0xD0}) // false start (single byte of a unique code)
	stream.Write(f2)

	r := NewReader(&splitReader{buf: stream.Bytes(), chunk: 3})
	got1, err := r.Read()
	if err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	if got1.Command != 0x6019 {
		t.Fatalf("frame 1 cmd: %#x", got1.Command)
	}
	got2, err := r.Read()
	if err != nil {
		t.Fatalf("Read 2: %v", err)
	}
	if got2.Command != 0x2053 || !bytes.Equal(got2.Data, []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("frame 2: %+v", got2)
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF, got %v", err)
	}
}

// splitReader returns at most chunk bytes per Read to exercise the
// framer's partial-read handling.
type splitReader struct {
	buf   []byte
	chunk int
	off   int
}

func (s *splitReader) Read(p []byte) (int, error) {
	if s.off >= len(s.buf) {
		return 0, io.EOF
	}
	n := s.chunk
	if n > len(p) {
		n = len(p)
	}
	if n > len(s.buf)-s.off {
		n = len(s.buf) - s.off
	}
	copy(p, s.buf[s.off:s.off+n])
	s.off += n
	return n, nil
}

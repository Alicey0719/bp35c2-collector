package broute

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestParseUDPReceive_HappyPath(t *testing.T) {
	// 27-byte header + 3-byte payload
	buf := make([]byte, 0, 30)
	buf = append(buf, bytes.Repeat([]byte{0xFE}, 16)...) // srcIP
	buf = binary.BigEndian.AppendUint16(buf, 3610)      // srcPort
	buf = binary.BigEndian.AppendUint16(buf, 3610)      // dstPort
	buf = binary.BigEndian.AppendUint16(buf, 0x1234)   // srcPAN
	buf = append(buf, 0x00)                             // unicast
	buf = append(buf, 0x01)                             // encrypted
	buf = append(buf, 0xC0)                             // RSSI (int8: -64)
	buf = binary.BigEndian.AppendUint16(buf, 3)        // rxSize
	buf = append(buf, 0xAA, 0xBB, 0xCC)                 // payload

	inc, err := parseUDPReceive(buf)
	if err != nil {
		t.Fatalf("parseUDPReceive: %v", err)
	}
	if inc.SrcPort != 3610 {
		t.Fatalf("srcPort: %d", inc.SrcPort)
	}
	if inc.RSSI != -64 {
		t.Fatalf("rssi: %d", inc.RSSI)
	}
	if !bytes.Equal(inc.Payload, []byte{0xAA, 0xBB, 0xCC}) {
		t.Fatalf("payload: %x", inc.Payload)
	}
}

func TestParseUDPReceive_TruncatedHeader(t *testing.T) {
	if _, err := parseUDPReceive(make([]byte, 10)); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseUDPReceive_RxSizeMismatch(t *testing.T) {
	buf := make([]byte, 27) // header only, rxSize=0 read from zero bytes
	// Say rxSize=5 but we don't have those bytes.
	binary.BigEndian.PutUint16(buf[25:27], 5)
	if _, err := parseUDPReceive(buf); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseActiveScanResult(t *testing.T) {
	ch, found, err := parseActiveScanResult([]byte{0x00, 0x0B, 0x09})
	if err != nil {
		t.Fatal(err)
	}
	if !found || ch != 0x0B {
		t.Fatalf("got ch=%#x found=%v", ch, found)
	}
	_, found, err = parseActiveScanResult([]byte{0x01, 0x0C})
	if err != nil || found {
		t.Fatalf("got found=%v err=%v", found, err)
	}
	if _, _, err := parseActiveScanResult([]byte{0x00}); err == nil {
		t.Fatal("want error on truncated payload")
	}
}

package echonet

import (
	"bytes"
	"testing"
)

func TestEncodeGetRequest_SingleEPC(t *testing.T) {
	// Get instant power (EPC 0xE7) from smart meter, TID=1
	got := Encode(NewGetRequest(0x0001, EOJSmartMeter, 0xE7))
	want := []byte{
		0x10, 0x81, // EHD
		0x00, 0x01, // TID
		0x05, 0xFF, 0x01, // SEOJ controller
		0x02, 0x88, 0x01, // DEOJ smart meter
		0x62,       // ESV Get
		0x01,       // OPC 1
		0xE7, 0x00, // EPC=E7 PDC=0
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes mismatch\n got=%x\nwant=%x", got, want)
	}
}

func TestEncodeGetRequest_MultipleEPCs(t *testing.T) {
	got := Encode(NewGetRequest(0x1234, EOJSmartMeter, 0xE7, 0xE8, 0xE9))
	want := []byte{
		0x10, 0x81,
		0x12, 0x34,
		0x05, 0xFF, 0x01,
		0x02, 0x88, 0x01,
		0x62,
		0x03,
		0xE7, 0x00,
		0xE8, 0x00,
		0xE9, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bytes mismatch\n got=%x\nwant=%x", got, want)
	}
}

func TestDecodeGetResponse_InstantPower(t *testing.T) {
	// Response: EHD 1081 | TID 0001 | SEOJ 028801 | DEOJ 05FF01 | ESV 72 | OPC 01
	// EPC E7 PDC 04 EDT 00 00 02 0B (523W)
	raw := []byte{
		0x10, 0x81,
		0x00, 0x01,
		0x02, 0x88, 0x01,
		0x05, 0xFF, 0x01,
		0x72, 0x01,
		0xE7, 0x04, 0x00, 0x00, 0x02, 0x0B,
	}
	f, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if f.TID != 0x0001 || f.ESV != ESVGetRes || len(f.Props) != 1 {
		t.Fatalf("unexpected: %+v", f)
	}
	p := f.Props[0]
	if p.EPC != 0xE7 || !bytes.Equal(p.EDT, []byte{0x00, 0x00, 0x02, 0x0B}) {
		t.Fatalf("prop mismatch: %+v", p)
	}
}

func TestDecodeGetResponse_MultiProperty_ScheduledPower(t *testing.T) {
	// AN F20-style: two properties EA/EB with PDC=11 each
	edt := []byte{0x07, 0xE2, 0x0A, 0x02, 0x0E, 0x1E, 0x00, 0x00, 0x03, 0x73, 0xAF}
	raw := []byte{
		0x10, 0x81, 0x00, 0x05,
		0x02, 0x88, 0x01,
		0x05, 0xFF, 0x01,
		0x72, 0x02,
		0xEA, 0x0B,
	}
	raw = append(raw, edt...)
	raw = append(raw, 0xEB, 0x0B)
	raw = append(raw, edt...)
	f, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(f.Props) != 2 || f.Props[0].EPC != 0xEA || f.Props[1].EPC != 0xEB {
		t.Fatalf("props: %+v", f.Props)
	}
	if !bytes.Equal(f.Props[0].EDT, edt) || !bytes.Equal(f.Props[1].EDT, edt) {
		t.Fatalf("edt mismatch")
	}
}

func TestDecodeSNA(t *testing.T) {
	// Get_SNA: EPC 0xE9 not implemented → PDC=0, ESV 0x52
	raw := []byte{
		0x10, 0x81, 0x00, 0x07,
		0x02, 0x88, 0x01,
		0x05, 0xFF, 0x01,
		byte(ESVGetSNA), 0x01,
		0xE9, 0x00,
	}
	f, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !f.ESV.IsSNA() {
		t.Fatalf("expected SNA, got %#x", f.ESV)
	}
}

func TestDecodeErrors(t *testing.T) {
	// too short
	if _, err := Decode([]byte{0x10, 0x81}); err == nil {
		t.Fatal("want error")
	}
	// bad EHD
	bad := []byte{0x11, 0x81, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if _, err := Decode(bad); err == nil {
		t.Fatal("want error")
	}
	// truncated property
	trunc := []byte{
		0x10, 0x81, 0, 1,
		0x02, 0x88, 0x01,
		0x05, 0xFF, 0x01,
		0x72, 0x01,
		0xE7, 0x04, 0x00, // says PDC=4 but only 1 byte follows
	}
	if _, err := Decode(trunc); err == nil {
		t.Fatal("want overflow error")
	}
}

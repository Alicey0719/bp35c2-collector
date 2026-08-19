package meter

import (
	"testing"
	"time"
)

func TestDecodeInstantPowerW(t *testing.T) {
	// 523 W → 0x0000020B
	v, err := DecodeInstantPowerW([]byte{0x00, 0x00, 0x02, 0x0B})
	if err != nil || v != 523 {
		t.Fatalf("got %d, %v", v, err)
	}
	// -1000 W (reverse flow) → 0xFFFFFC18
	v, err = DecodeInstantPowerW([]byte{0xFF, 0xFF, 0xFC, 0x18})
	if err != nil || v != -1000 {
		t.Fatalf("got %d, %v", v, err)
	}
}

func TestDecodeInstantCurrent_Both(t *testing.T) {
	// R = 50 (5.0 A), T = 60 (6.0 A)
	c, err := DecodeInstantCurrent([]byte{0x00, 0x32, 0x00, 0x3C})
	if err != nil {
		t.Fatal(err)
	}
	if c.RPhaseA != 5.0 || c.TPhaseA != 6.0 || !c.HasT {
		t.Fatalf("%+v", c)
	}
}

func TestDecodeInstantCurrent_SinglePhase2Wire(t *testing.T) {
	// T = sentinel 0x7FFE → HasT false
	c, err := DecodeInstantCurrent([]byte{0x00, 0x32, 0x7F, 0xFE})
	if err != nil {
		t.Fatal(err)
	}
	if c.HasT || c.RPhaseA != 5.0 {
		t.Fatalf("%+v", c)
	}
}

func TestDecodeInstantCurrent_Negative(t *testing.T) {
	// R = -10 (0xFFF6), T = 20 (0x0014)
	c, err := DecodeInstantCurrent([]byte{0xFF, 0xF6, 0x00, 0x14})
	if err != nil {
		t.Fatal(err)
	}
	if c.RPhaseA != -1.0 || c.TPhaseA != 2.0 {
		t.Fatalf("%+v", c)
	}
}

func TestDecodeInstantVoltage(t *testing.T) {
	// R = 1050 (105.0 V), T = 1043 (104.3 V)
	v, err := DecodeInstantVoltage([]byte{0x04, 0x1A, 0x04, 0x13})
	if err != nil {
		t.Fatal(err)
	}
	if v.RPhaseV != 105.0 || v.TPhaseV != 104.3 {
		t.Fatalf("%+v", v)
	}
}

func TestDecodeScheduledCumulative(t *testing.T) {
	// AN F20: 07 E2 0A 02 0E 1E 00 00 00 03 73 AF → 2018-10-02 14:30:00, 226735
	edt := []byte{0x07, 0xE2, 0x0A, 0x02, 0x0E, 0x1E, 0x00, 0x00, 0x03, 0x73, 0xAF}
	s, err := DecodeScheduledCumulative(edt)
	if err != nil {
		t.Fatal(err)
	}
	wantAt := time.Date(2018, 10, 2, 14, 30, 0, 0, time.Local)
	if !s.At.Equal(wantAt) {
		t.Fatalf("time: got %v want %v", s.At, wantAt)
	}
	if s.Value != 0x000373AF { // 226223
		t.Fatalf("value: %d", s.Value)
	}
}

func TestCumulativeKWh(t *testing.T) {
	// coefficient 1, unit 0x01 (0.1 kWh) → 100 * 0.1 = 10 kWh
	kWh, err := CumulativeKWh(100, 1, 0x01)
	if err != nil {
		t.Fatal(err)
	}
	if kWh != 10.0 {
		t.Fatalf("%f", kWh)
	}
	// zero coefficient defaults to 1
	kWh, err = CumulativeKWh(10, 0, 0x00)
	if err != nil || kWh != 10.0 {
		t.Fatalf("%f %v", kWh, err)
	}
	// bad unit
	if _, err := CumulativeKWh(10, 1, 0xFF); err == nil {
		t.Fatal("want error on bad unit")
	}
}

func TestDecodeFaultStatus(t *testing.T) {
	yes, err := DecodeFaultStatus([]byte{0x41})
	if err != nil || !yes {
		t.Fatalf("got %v %v", yes, err)
	}
	no, err := DecodeFaultStatus([]byte{0x42})
	if err != nil || no {
		t.Fatalf("got %v %v", no, err)
	}
	if _, err := DecodeFaultStatus([]byte{0x00}); err == nil {
		t.Fatal("want error")
	}
}

func TestDecodeGetPropertyMap_ShortForm(t *testing.T) {
	edt := []byte{0x03, 0xE7, 0xE8, 0xE9}
	m, err := DecodeGetPropertyMap(edt)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 {
		t.Fatalf("size %d", len(m))
	}
	for _, e := range []byte{0xE7, 0xE8, 0xE9} {
		if _, ok := m[e]; !ok {
			t.Fatalf("missing %#x", e)
		}
	}
}

func TestDecodeGetPropertyMap_LongForm(t *testing.T) {
	// Set EPC 0xE7 and 0xE8:
	//   0xE7 = 1110 0111 → nibble high=0xE (bit 6), low=0x7 → byte index 7, bit 6
	//   0xE8 = 1110 1000 → nibble high=0xE (bit 6), low=0x8 → byte index 8, bit 6
	edt := make([]byte, 17)
	edt[0] = 16 // long-form marker (count > 15)
	edt[1+7] |= 1 << 6
	edt[1+8] |= 1 << 6
	m, err := DecodeGetPropertyMap(edt)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m[0xE7]; !ok {
		t.Fatalf("missing 0xE7: %v", m)
	}
	if _, ok := m[0xE8]; !ok {
		t.Fatalf("missing 0xE8: %v", m)
	}
}

// Package meter provides a high-level view of a low-voltage smart
// electric meter (ECHONET Lite EOJ 0x028801) reached over the B-route.
//
// Responsibilities:
//   - Send Get requests over an established broute.Session and
//     correlate responses by ECHONET Lite TID.
//   - Cache one-shot properties (D3 coefficient, E1 unit, D7 digits)
//     that the module returns identically every time.
//   - Decode raw EDT bytes into typed measurements, applying the unit
//     and coefficient conversions.
package meter

import (
	"encoding/binary"
	"fmt"
	"time"
)

// EPC constants (low-voltage smart electric meter class).
const (
	EPCVersion              byte = 0x82
	EPCFaultStatus          byte = 0x88
	EPCManufacturerCode     byte = 0x8A
	EPCCoefficient          byte = 0xD3
	EPCEffectiveDigits      byte = 0xD7
	EPCCumulativeForward    byte = 0xE0
	EPCCumulativeUnit       byte = 0xE1
	EPCCumulativeReverse    byte = 0xE3
	EPCInstantPower         byte = 0xE7
	EPCInstantCurrent       byte = 0xE8
	EPCInstantVoltage       byte = 0xE9
	EPCScheduledCumForward  byte = 0xEA
	EPCScheduledCumReverse  byte = 0xEB
	EPCGetPropertyMap       byte = 0x9F
)

// UnitKWh maps the 0xE1 byte to a kWh multiplier.
//
// From the ECHONET Lite appendix: 0x00=1, 0x01=0.1, ..., 0x04=0.0001,
// 0x0A..0x0D = 10, 100, 1000, 10000 respectively.
func UnitKWh(u byte) (float64, error) {
	switch u {
	case 0x00:
		return 1, nil
	case 0x01:
		return 0.1, nil
	case 0x02:
		return 0.01, nil
	case 0x03:
		return 0.001, nil
	case 0x04:
		return 0.0001, nil
	case 0x0A:
		return 10, nil
	case 0x0B:
		return 100, nil
	case 0x0C:
		return 1000, nil
	case 0x0D:
		return 10000, nil
	}
	return 0, fmt.Errorf("meter: unknown cumulative unit %#x", u)
}

// DecodeInstantPowerW decodes EPC 0xE7 (signed int32, watts).
func DecodeInstantPowerW(edt []byte) (int32, error) {
	if len(edt) != 4 {
		return 0, fmt.Errorf("EPC 0xE7: want 4 bytes, got %d", len(edt))
	}
	return int32(binary.BigEndian.Uint32(edt)), nil
}

// InstantCurrent is a decoded EPC 0xE8 (R & T phase in amps).
// TPhase is math.NaN-equivalent (returned as HasT=false) when the raw
// value is the sentinel 0x7FFE meaning "single-phase 2-wire meter".
type InstantCurrent struct {
	RPhaseA float64
	TPhaseA float64
	HasT    bool
}

// DecodeInstantCurrent decodes EPC 0xE8: R and T phase int16 in 0.1 A.
func DecodeInstantCurrent(edt []byte) (InstantCurrent, error) {
	if len(edt) != 4 {
		return InstantCurrent{}, fmt.Errorf("EPC 0xE8: want 4 bytes, got %d", len(edt))
	}
	r := int16(binary.BigEndian.Uint16(edt[0:2]))
	tRaw := int16(binary.BigEndian.Uint16(edt[2:4]))
	out := InstantCurrent{RPhaseA: float64(r) / 10.0, HasT: true}
	if uint16(tRaw) == 0x7FFE {
		out.HasT = false
	} else {
		out.TPhaseA = float64(tRaw) / 10.0
	}
	return out, nil
}

// InstantVoltage is decoded EPC 0xE9 (R & T phase in volts).
// Meters that do not implement it should return Get_SNA at the ECHONET
// Lite layer; callers should handle absence rather than call this.
type InstantVoltage struct {
	RPhaseV float64
	TPhaseV float64
}

// DecodeInstantVoltage decodes EPC 0xE9: R + T uint16 in 0.1 V.
func DecodeInstantVoltage(edt []byte) (InstantVoltage, error) {
	if len(edt) != 4 {
		return InstantVoltage{}, fmt.Errorf("EPC 0xE9: want 4 bytes, got %d", len(edt))
	}
	return InstantVoltage{
		RPhaseV: float64(binary.BigEndian.Uint16(edt[0:2])) / 10.0,
		TPhaseV: float64(binary.BigEndian.Uint16(edt[2:4])) / 10.0,
	}, nil
}

// DecodeCumulativeRaw returns the raw uint32 counter from EPC 0xE0/0xE3.
func DecodeCumulativeRaw(edt []byte) (uint32, error) {
	if len(edt) != 4 {
		return 0, fmt.Errorf("cumulative EPC: want 4 bytes, got %d", len(edt))
	}
	return binary.BigEndian.Uint32(edt), nil
}

// ScheduledCumulative is a decoded EPC 0xEA/0xEB.
type ScheduledCumulative struct {
	At    time.Time
	Value uint32 // raw counter — apply coefficient/unit to get kWh
}

// DecodeScheduledCumulative decodes EPC 0xEA/0xEB:
// [YYYY 2B][MM 1B][DD 1B][hh 1B][mm 1B][ss 1B][value uint32]
func DecodeScheduledCumulative(edt []byte) (ScheduledCumulative, error) {
	if len(edt) != 11 {
		return ScheduledCumulative{}, fmt.Errorf("EPC 0xEA/0xEB: want 11 bytes, got %d", len(edt))
	}
	year := int(binary.BigEndian.Uint16(edt[0:2]))
	mon := int(edt[2])
	day := int(edt[3])
	hh := int(edt[4])
	mm := int(edt[5])
	ss := int(edt[6])
	val := binary.BigEndian.Uint32(edt[7:11])
	// Timestamp is in the meter's local wall clock. We don't know the
	// meter's timezone reliably — use the process-local zone, which
	// matches expectation for a home installation.
	at := time.Date(year, time.Month(mon), day, hh, mm, ss, 0, time.Local)
	return ScheduledCumulative{At: at, Value: val}, nil
}

// CumulativeKWh applies the coefficient (D3) and unit (E1) multipliers
// to a raw counter value.
func CumulativeKWh(raw uint32, coefficient uint32, unitByte byte) (float64, error) {
	unit, err := UnitKWh(unitByte)
	if err != nil {
		return 0, err
	}
	if coefficient == 0 {
		coefficient = 1
	}
	return float64(raw) * float64(coefficient) * unit, nil
}

// DecodeCoefficient reads EPC 0xD3.
func DecodeCoefficient(edt []byte) (uint32, error) {
	if len(edt) != 4 {
		return 0, fmt.Errorf("EPC 0xD3: want 4 bytes, got %d", len(edt))
	}
	return binary.BigEndian.Uint32(edt), nil
}

// DecodeFaultStatus reads EPC 0x88: 0x41 = fault present, 0x42 = none.
func DecodeFaultStatus(edt []byte) (bool, error) {
	if len(edt) != 1 {
		return false, fmt.Errorf("EPC 0x88: want 1 byte, got %d", len(edt))
	}
	switch edt[0] {
	case 0x41:
		return true, nil
	case 0x42:
		return false, nil
	}
	return false, fmt.Errorf("EPC 0x88: unknown value %#x", edt[0])
}

// DecodeGetPropertyMap parses the two forms of EPC 0x9F:
//   - short form (<=15 EPCs): [count][epc1][epc2]...
//   - long form (>15 EPCs):    [count][16 B bitmap]
// The 16-byte bitmap encodes EPCs as follows: the byte index n (0..15)
// stands for bit index in the low nibble of each EPC group; each of the
// 8 bits selects the upper nibble.
// Returns the set of EPCs advertised.
func DecodeGetPropertyMap(edt []byte) (map[byte]struct{}, error) {
	if len(edt) < 1 {
		return nil, fmt.Errorf("EPC 0x9F: empty payload")
	}
	n := int(edt[0])
	out := make(map[byte]struct{}, n)
	if n <= 15 {
		if len(edt) < 1+n {
			return nil, fmt.Errorf("EPC 0x9F: short form truncated")
		}
		for _, epc := range edt[1 : 1+n] {
			out[epc] = struct{}{}
		}
		return out, nil
	}
	if len(edt) < 1+16 {
		return nil, fmt.Errorf("EPC 0x9F: long form truncated")
	}
	// Long form: byte i (i=0..15) holds an 8-bit mask.
	// Bit b in byte i sets EPC = ((b+8) << 4) | i.
	// See ECHONET Lite Rel J appendix.
	for i, b := range edt[1:17] {
		for bit := 0; bit < 8; bit++ {
			if b&(1<<bit) == 0 {
				continue
			}
			epc := byte(((bit + 8) << 4) | i)
			out[epc] = struct{}{}
		}
	}
	return out, nil
}

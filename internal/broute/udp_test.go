package broute

import (
	"net"
	"testing"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
)

func TestFormatIPv6Upper(t *testing.T) {
	ip := net.ParseIP("fe80::1234:5678:9abc:def0")
	got := formatIPv6Upper(ip)
	want := "FE80:0000:0000:0000:1234:5678:9ABC:DEF0"
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestParseIncoming_ROPTHexPath(t *testing.T) {
	ev := bp35c2.Event{
		Kind: "ERXUDP",
		Params: []string{
			"FE80:0000:0000:0000:0011:2233:4455:6677", // sender
			"FE80:0000:0000:0000:AAAA:BBBB:CCCC:DDDD", // dest
			"0E1A",           // sport
			"0E1A",           // dport
			"001122334455",   // senderlladdr
			"1",              // secured
			"0",              // side
			"0004",           // datalen
			"DEADBEEF",       // hex payload
		},
		Data: []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}
	inc, ok := parseIncoming(ev)
	if !ok {
		t.Fatal("parseIncoming returned !ok")
	}
	if inc.SrcPort != 3610 {
		t.Fatalf("sport: %d", inc.SrcPort)
	}
	if inc.SrcIP.String() != "fe80::11:2233:4455:6677" {
		t.Fatalf("src ip: %s", inc.SrcIP)
	}
	if string(inc.Payload) != string([]byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Fatalf("payload: %x", inc.Payload)
	}
}

func TestParseHexU16(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want uint16
	}{
		{"0E1A", 3610},
		{"ffff", 0xFFFF},
		{"0000", 0},
	} {
		got, err := parseHexU16(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseHexU16(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	if _, err := parseHexU16("XX"); err == nil {
		t.Error("expected error for non-hex")
	}
}

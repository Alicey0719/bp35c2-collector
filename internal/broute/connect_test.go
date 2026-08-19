package broute

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
)

type pipeConn struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p pipeConn) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p pipeConn) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p pipeConn) Close() error {
	_ = p.r.Close()
	return p.w.Close()
}

func newPair() (host, module pipeConn) {
	hostR, moduleW := io.Pipe()
	moduleR, hostW := io.Pipe()
	return pipeConn{r: hostR, w: hostW}, pipeConn{r: moduleR, w: moduleW}
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mockModule speaks the SKSTACK-IP subset needed for a full connect.
// It is driven by scripting from the test: each incoming line is
// matched by prefix, and a fixed reply script is emitted.
type mockModule struct {
	side pipeConn
	log  *slog.Logger
	// Post-command scripted replies to inject after ACK. Sent as raw
	// strings so tests can include CRLF and multi-line blobs.
	afterOK map[string]string
}

func (mm *mockModule) run(t *testing.T) {
	buf := make([]byte, 4096)
	var acc []byte
	handleLine := func(line string) {
		// Echo, then optional canned reply, then OK.
		reply := "OK\r\n"
		if extra, ok := mm.afterOK[line]; ok {
			reply = extra + "OK\r\n"
		}
		// Some commands like SKSCAN emit their result asynchronously via
		// EVENT — the reply script for those handles it.
		switch {
		case strings.HasPrefix(line, "SKSCAN "):
			// OK immediately, then scripted async block (EPANDESC + EVENT 22).
			mm.side.Write([]byte(line + "\r\n" + reply))
			return
		case strings.HasPrefix(line, "SKJOIN "):
			mm.side.Write([]byte(line + "\r\n" + reply))
			return
		}
		mm.side.Write([]byte(line + "\r\n" + reply))
	}
	for {
		n, err := mm.side.Read(buf)
		if err != nil {
			return
		}
		acc = append(acc, buf[:n]...)
		for {
			idx := strings.Index(string(acc), "\r\n")
			if idx < 0 {
				break
			}
			line := string(acc[:idx])
			acc = acc[idx+2:]
			handleLine(line)
		}
	}
}

func TestConnectOnce_FullSequence(t *testing.T) {
	host, module := newPair()
	drv := bp35c2.New(host, silentLogger(), 32)
	defer drv.Close()

	// EPANDESC block + EVENT 22 to emit right after SKSCAN OK.
	scanReply := "" +
		"EPANDESC\r\n" +
		"  Channel:21\r\n" +
		"  Channel Page:09\r\n" +
		"  Pan ID:8888\r\n" +
		"  Addr:001D129012341234\r\n" +
		"  LQI:E1\r\n" +
		"  PairID:12345678\r\n" +
		"EVENT 22 FE80:0000:0000:0000:021D:1291:0002:0129\r\n"

	mm := &mockModule{
		side: module,
		log:  silentLogger(),
		afterOK: map[string]string{
			"SKLL64 001D129012341234": "FE80:0000:0000:0000:021D:1290:1234:1234\r\n",
			"SKSCAN 2 FFFFFFFF 6 0":   scanReply,
			"SKJOIN FE80:0000:0000:0000:021D:1290:1234:1234": "EVENT 25 FE80::1\r\n", // ideal case
		},
	}
	go mm.run(t)

	mgr := NewManager(drv, Config{
		BRouteID:       "0123456789ABCDEF0123456789ABCDEF",
		BRoutePassword: "PASSWORD1234",
		ScanDuration:   6,
		ChannelMask:    0xFFFFFFFF,
		CommandTimeout: 2 * time.Second,
		JoinTimeout:    2 * time.Second,
	}, silentLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go mgr.dispatchEvents(ctx)

	sess, err := mgr.connectOnce(ctx)
	if err != nil {
		t.Fatalf("connectOnce: %v", err)
	}
	if sess.Channel != 0x21 {
		t.Fatalf("channel: %#x", sess.Channel)
	}
	if sess.PANID != 0x8888 {
		t.Fatalf("panid: %#x", sess.PANID)
	}
	if sess.MeterMAC != "001D129012341234" {
		t.Fatalf("mac: %q", sess.MeterMAC)
	}
	if sess.MeterIP.String() != "fe80::21d:1290:1234:1234" {
		t.Fatalf("ip: %s", sess.MeterIP)
	}
}

func TestConnectOnce_JoinFailureFromEVENT24(t *testing.T) {
	host, module := newPair()
	drv := bp35c2.New(host, silentLogger(), 32)
	defer drv.Close()

	scanReply := "" +
		"EPANDESC\r\n" +
		"  Channel:21\r\n" +
		"  Channel Page:09\r\n" +
		"  Pan ID:8888\r\n" +
		"  Addr:001D129012341234\r\n" +
		"  LQI:E1\r\n" +
		"  PairID:12345678\r\n" +
		"EVENT 22 FE80::1\r\n"

	mm := &mockModule{
		side: module,
		log:  silentLogger(),
		afterOK: map[string]string{
			"SKLL64 001D129012341234":          "FE80:0000:0000:0000:021D:1290:1234:1234\r\n",
			"SKSCAN 2 FFFFFFFF 6 0":            scanReply,
			"SKJOIN FE80:0000:0000:0000:021D:1290:1234:1234":  "EVENT 24 FE80::1\r\n",
		},
	}
	go mm.run(t)

	var authFails int32
	var mu sync.Mutex
	mgr := NewManager(drv, Config{
		BRouteID:       "0123456789ABCDEF0123456789ABCDEF",
		BRoutePassword: "PASSWORD1234",
		ScanDuration:   6,
		ChannelMask:    0xFFFFFFFF,
		CommandTimeout: 2 * time.Second,
		JoinTimeout:    2 * time.Second,
	}, silentLogger())
	mgr.OnAuthFailure = func() {
		mu.Lock()
		authFails++
		mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go mgr.dispatchEvents(ctx)

	_, err := mgr.connectOnce(ctx)
	if err == nil {
		t.Fatal("expected auth failure")
	}
	// AllowOnAuthFailure to fire async
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	got := authFails
	mu.Unlock()
	if got == 0 {
		t.Fatal("OnAuthFailure not fired")
	}
}

func TestParseEPANDESC(t *testing.T) {
	ev := bp35c2.Event{
		Kind: "EPANDESC",
		Fields: map[string]string{
			"Channel":      "21",
			"Channel Page": "09",
			"Pan ID":       "8888",
			"Addr":         "001D129012341234",
			"LQI":          "E1",
			"PairID":       "12345678",
		},
	}
	d, err := parseEPANDESC(ev)
	if err != nil {
		t.Fatalf("parseEPANDESC: %v", err)
	}
	if d.Channel != 0x21 || d.PANID != 0x8888 || d.LQI != 0xE1 {
		t.Fatalf("bad parse: %+v", d)
	}
}

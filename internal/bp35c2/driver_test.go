package bp35c2

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/frame"
)

// pipeConn glues two io.Pipe halves into an in-process bidirectional
// stream that satisfies ReadWriteCloser.
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

// newPair returns (host-side, module-side) connections. Writes on one
// appear on the other's Read.
func newPair() (host, module pipeConn) {
	hostR, moduleW := io.Pipe()
	moduleR, hostW := io.Pipe()
	return pipeConn{r: hostR, w: hostW}, pipeConn{r: moduleR, w: moduleW}
}

// mustEncode is a test helper that panics on encoding error.
func mustEncode(cmd uint16, data []byte, dir frame.Direction) []byte {
	b, err := frame.Encode(frame.Frame{Direction: dir, Command: cmd, Data: data})
	if err != nil {
		panic(err)
	}
	return b
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRequest_HappyPath(t *testing.T) {
	hostSide, moduleSide := newPair()
	d := New(hostSide, silentLogger(), 4)
	defer d.Close()

	// Module: read one request frame, then send the matching response.
	go func() {
		r := frame.NewReader(moduleSide)
		f, err := r.Read()
		if err != nil {
			t.Errorf("module read: %v", err)
			return
		}
		if f.Command != CmdBRouteStart {
			t.Errorf("module got cmd %#x", f.Command)
		}
		resp := mustEncode(RespBRouteStart, []byte{0x01, 0x02, 0x03}, frame.DirectionResponse)
		if _, err := moduleSide.Write(resp); err != nil {
			t.Errorf("module write: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := d.Request(ctx, CmdBRouteStart, nil)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if r.Command != RespBRouteStart || string(r.Data) != string([]byte{0x01, 0x02, 0x03}) {
		t.Fatalf("bad response: %+v", r)
	}
}

func TestRequest_TimeoutOnNoResponse(t *testing.T) {
	hostSide, moduleSide := newPair()
	d := New(hostSide, silentLogger(), 4)
	defer d.Close()

	// Drain writes on the module side but never reply.
	go io.Copy(io.Discard, moduleSide)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := d.Request(ctx, CmdBRouteStart, nil)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
}

func TestNotifications_DeliveredToChannel(t *testing.T) {
	hostSide, moduleSide := newPair()
	d := New(hostSide, silentLogger(), 4)
	defer d.Close()

	go func() {
		// Send boot notification, then PANA result notification.
		_, _ = moduleSide.Write(mustEncode(NotifyBoot, nil, frame.DirectionResponse))
		_, _ = moduleSide.Write(mustEncode(NotifyPANAResult, []byte{0x01, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11}, frame.DirectionResponse))
	}()

	select {
	case n := <-d.Notifications():
		if n.Command != NotifyBoot {
			t.Fatalf("got %#x", n.Command)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for boot notification")
	}
	select {
	case n := <-d.Notifications():
		if n.Command != NotifyPANAResult || n.Data[0] != 0x01 {
			t.Fatalf("got %+v", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pana notification")
	}
}

func TestRequest_IgnoresInterleavedNotification(t *testing.T) {
	hostSide, moduleSide := newPair()
	d := New(hostSide, silentLogger(), 4)
	defer d.Close()

	go func() {
		r := frame.NewReader(moduleSide)
		if _, err := r.Read(); err != nil {
			t.Errorf("module read: %v", err)
		}
		// Emit a notification first, then the actual response.
		_, _ = moduleSide.Write(mustEncode(NotifyLinkStateChg, []byte{0x03}, frame.DirectionResponse))
		time.Sleep(20 * time.Millisecond)
		_, _ = moduleSide.Write(mustEncode(RespUDPPortOpen, []byte{0x01}, frame.DirectionResponse))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := d.Request(ctx, CmdUDPPortOpen, []byte{0x0E, 0x1A})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if r.Command != RespUDPPortOpen || r.Data[0] != 0x01 {
		t.Fatalf("bad response: %+v", r)
	}
	// Notification should still be visible on the channel.
	select {
	case n := <-d.Notifications():
		if n.Command != NotifyLinkStateChg {
			t.Fatalf("got %#x", n.Command)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("notification lost")
	}
}

func TestClose_UnblocksRequest(t *testing.T) {
	hostSide, moduleSide := newPair()
	d := New(hostSide, silentLogger(), 4)
	go io.Copy(io.Discard, moduleSide)

	done := make(chan error, 1)
	go func() {
		_, err := d.Request(context.Background(), CmdBRouteStart, nil)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = d.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("want ErrClosed, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Request did not unblock on Close")
	}
}

func TestSendRaw_WritesAndReturns(t *testing.T) {
	hostSide, moduleSide := newPair()
	d := New(hostSide, silentLogger(), 4)
	defer d.Close()

	rc := make(chan uint16, 1)
	go func() {
		f, err := frame.NewReader(moduleSide).Read()
		if err != nil {
			t.Errorf("module read: %v", err)
			return
		}
		rc <- f.Command
	}()
	if err := d.SendRaw(CmdHardReset, nil); err != nil {
		t.Fatalf("SendRaw: %v", err)
	}
	select {
	case cmd := <-rc:
		if cmd != CmdHardReset {
			t.Fatalf("got %#x", cmd)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("frame not received")
	}
}

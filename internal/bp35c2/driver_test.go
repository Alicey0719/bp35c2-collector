package bp35c2

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
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

// hostReadLine reads from moduleSide (host-tx) as CRLF lines.
type lineReader struct{ buf []byte }

func (lr *lineReader) read(r io.Reader, tmp []byte) (string, error) {
	for {
		if idx := indexCRLF(lr.buf); idx >= 0 {
			line := string(lr.buf[:idx])
			lr.buf = lr.buf[idx+2:]
			return line, nil
		}
		n, err := r.Read(tmp)
		if n > 0 {
			lr.buf = append(lr.buf, tmp[:n]...)
		}
		if err != nil {
			return "", err
		}
	}
}

func indexCRLF(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' {
			return i
		}
	}
	return -1
}

func TestCommand_HappyPath(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()

	go func() {
		// Module: read incoming, then reply with echo + EINFO + OK.
		lr := &lineReader{}
		tmp := make([]byte, 128)
		line, err := lr.read(module, tmp)
		if err != nil {
			t.Errorf("module read: %v", err)
			return
		}
		if line != "SKINFO" {
			t.Errorf("module got: %q", line)
		}
		module.Write([]byte("SKINFO\r\nEINFO FE80::1 001122... 21 FFFF 0\r\nOK\r\n"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := d.Command(ctx, "SKINFO")
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if len(resp) != 1 || !strings.HasPrefix(resp[0], "EINFO ") {
		t.Fatalf("resp: %+v", resp)
	}
}

func TestCommand_FailReturnsFailError(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()

	go func() {
		lr := &lineReader{}
		tmp := make([]byte, 128)
		_, _ = lr.read(module, tmp)
		module.Write([]byte("SKVER\r\nFAIL ER04\r\n"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := d.Command(ctx, "SKVER")
	var fe *FailError
	if !errors.As(err, &fe) || fe.Code != "ER04" {
		t.Fatalf("want FailError ER04, got %v", err)
	}
}

func TestCommand_TimeoutWhenSilent(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()
	go io.Copy(io.Discard, module)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := d.Command(ctx, "SKINFO")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
}

func TestEvents_DeliveredAsync(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()

	go func() {
		// Emit two async events without any command being in flight.
		module.Write([]byte("EVENT 20 FE80::1 0\r\n"))
		module.Write([]byte("ERXUDP FE80::1 FE80::2 0E1A 0E1A 001122334455 1 0 0004 DEADBEEF\r\n"))
	}()

	select {
	case ev := <-d.Events():
		if ev.Kind != "EVENT" || len(ev.Params) < 1 || ev.Params[0] != "20" {
			t.Fatalf("first event: %+v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("EVENT not received")
	}
	select {
	case ev := <-d.Events():
		if ev.Kind != "ERXUDP" {
			t.Fatalf("second event kind: %q", ev.Kind)
		}
		if len(ev.Data) != 4 || ev.Data[0] != 0xDE || ev.Data[1] != 0xAD {
			t.Fatalf("ERXUDP data: %x", ev.Data)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("ERXUDP not received")
	}
}

func TestEvents_EPANDESCMultiLine(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()

	go func() {
		module.Write([]byte("EPANDESC\r\n" +
			"  Channel:21\r\n" +
			"  Channel Page:09\r\n" +
			"  Pan ID:8888\r\n" +
			"  Addr:001D129012341234\r\n" +
			"  LQI:E1\r\n" +
			"  PairID:12345678\r\n"))
	}()

	select {
	case ev := <-d.Events():
		if ev.Kind != "EPANDESC" {
			t.Fatalf("kind: %q", ev.Kind)
		}
		if ev.Fields["Channel"] != "21" || ev.Fields["Pan ID"] != "8888" ||
			ev.Fields["Addr"] != "001D129012341234" {
			t.Fatalf("fields: %+v", ev.Fields)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EPANDESC not received")
	}
}

func TestCommand_InterleavedEventGoesToChannel(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()

	go func() {
		lr := &lineReader{}
		tmp := make([]byte, 128)
		_, _ = lr.read(module, tmp)
		// Emit an unrelated event before the OK.
		module.Write([]byte("SKINFO\r\nEVENT 29 FE80::1 0\r\nEINFO FE80::1 X 21 FFFF 0\r\nOK\r\n"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := d.Command(ctx, "SKINFO")
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if len(resp) != 1 || !strings.HasPrefix(resp[0], "EINFO ") {
		t.Fatalf("resp: %+v", resp)
	}
	select {
	case ev := <-d.Events():
		if ev.Kind != "EVENT" || ev.Params[0] != "29" {
			t.Fatalf("event: %+v", ev)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("interleaved event lost")
	}
}

func TestCommand_CredentialsRedactedInLogs(t *testing.T) {
	// Not user-facing behaviour, but a safety net: we don't emit the
	// SKSETPWD/SKSETRBID contents in structured logs.
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()
	go func() {
		lr := &lineReader{}
		tmp := make([]byte, 128)
		l, err := lr.read(module, tmp)
		if err != nil {
			return
		}
		// Actual on-wire bytes still carry the real credential.
		if !strings.HasPrefix(l, "SKSETPWD C ") {
			t.Errorf("wire: %q", l)
		}
		module.Write([]byte(l + "\r\nOK\r\n"))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := d.Command(ctx, "SKSETPWD C secret1234"); err != nil {
		t.Fatalf("Command: %v", err)
	}
}

func TestClose_UnblocksPendingCommand(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	go io.Copy(io.Discard, module)

	done := make(chan error, 1)
	go func() {
		_, err := d.Command(context.Background(), "SKINFO")
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
		t.Fatal("Command did not unblock")
	}
}

func TestCommandBinary_WritesHeaderPlusPayload(t *testing.T) {
	host, module := newPair()
	d := New(host, silentLogger(), 8)
	defer d.Close()

	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 128)
		n, _ := module.Read(buf)
		got <- buf[:n]
		module.Write([]byte("OK\r\n"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := d.CommandBinary(ctx, "SKSENDTO 1 FE80::1 0E1A 1 0 0004 ", []byte{0xDE, 0xAD, 0xBE, 0xEF}); err != nil {
		t.Fatalf("CommandBinary: %v", err)
	}
	select {
	case b := <-got:
		want := "SKSENDTO 1 FE80::1 0E1A 1 0 0004 \xDE\xAD\xBE\xEF\r\n"
		if string(b) != want {
			t.Fatalf("wire mismatch\n got=%q\nwant=%q", b, want)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("module never saw the write")
	}
}

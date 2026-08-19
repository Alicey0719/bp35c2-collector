package broute

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
	"github.com/Alicey0719/bp35c2-collector/internal/frame"
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

func encResp(cmd uint16, data []byte) []byte {
	b, err := frame.Encode(frame.Frame{Direction: frame.DirectionResponse, Command: cmd, Data: data})
	if err != nil {
		panic(err)
	}
	return b
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestConnectOnce_CachedChannel exercises the connect path where a
// cached channel is present, so no active scan is performed.
func TestConnectOnce_CachedChannel(t *testing.T) {
	dir := t.TempDir()
	chPath := filepath.Join(dir, "channel")
	// Seed the cache with channel 0x0C.
	if err := (ChannelStore{Path: chPath}).Save(0x0C); err != nil {
		t.Fatal(err)
	}

	hostSide, moduleSide := newPair()
	drv := bp35c2.New(hostSide, silentLogger(), 8)
	defer drv.Close()

	// Scripted module: replies to CmdBRouteAuthSet → CmdInitialSettings →
	// CmdBRouteStart → CmdUDPPortOpen → CmdBRoutePANAStart, then sends
	// the 0x6028 PANA success notification.
	moduleDone := make(chan error, 1)
	go func() {
		defer close(moduleDone)
		r := frame.NewReader(moduleSide)
		expect := []uint16{
			bp35c2.CmdBRouteAuthSet,
			bp35c2.CmdInitialSettings,
			bp35c2.CmdBRouteStart,
			bp35c2.CmdUDPPortOpen,
			bp35c2.CmdBRoutePANAStart,
		}
		for _, wantCmd := range expect {
			f, err := r.Read()
			if err != nil {
				moduleDone <- err
				return
			}
			if f.Command != wantCmd {
				t.Errorf("module: got cmd %#x, want %#x", f.Command, wantCmd)
				return
			}
			var respData []byte
			switch wantCmd {
			case bp35c2.CmdBRouteStart:
				// [respResult=0x01][channel=0x0C][panID=0xABCD][mac 8B][rssi=0xC0]
				respData = append(respData, 0x01, 0x0C)
				respData = binary.BigEndian.AppendUint16(respData, 0xABCD)
				respData = append(respData, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88)
				respData = append(respData, 0xC0)
			default:
				respData = []byte{0x01}
			}
			if _, err := moduleSide.Write(encResp(wantCmd|0x2000, respData)); err != nil {
				moduleDone <- err
				return
			}
			// After acknowledging PANA start, deliver the async result.
			if wantCmd == bp35c2.CmdBRoutePANAStart {
				time.Sleep(10 * time.Millisecond)
				payload := append([]byte{0x01}, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}...)
				if _, err := moduleSide.Write(encResp(bp35c2.NotifyPANAResult, payload)); err != nil {
					moduleDone <- err
					return
				}
			}
		}
	}()

	mgr := NewManager(drv, Config{
		BRouteID:         "0123456789ABCDEF0123456789ABCDEF",
		BRoutePassword:   "PASSWORD1234",
		ChannelStorePath: chPath,
		CommandTimeout:   1 * time.Second,
		PANAAuthTimeout:  1 * time.Second,
	}, silentLogger())

	// dispatchNotifications must be running for PANA result to reach doPANA.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go mgr.dispatchNotifications(ctx)

	sess, joinErr, err := mgr.connectOnce(ctx)
	if err != nil {
		t.Fatalf("connectOnce: %v (joinErr=%v)", err, joinErr)
	}
	if sess.Channel != 0x0C {
		t.Fatalf("channel: %#x", sess.Channel)
	}
	if sess.PANID != 0xABCD {
		t.Fatalf("pan: %#x", sess.PANID)
	}
	wantMAC := [8]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	if sess.MeterMAC != wantMAC {
		t.Fatalf("mac: %x", sess.MeterMAC)
	}
	// Link-local IP: fe80::1322:3344:5566:7788 (first byte 11 XOR 02 = 13)
	if sess.MeterIP.String() != "fe80::1322:3344:5566:7788" {
		t.Fatalf("ip: %s", sess.MeterIP)
	}
	if err := <-moduleDone; err != nil {
		t.Fatalf("module: %v", err)
	}
}

// TestConnectOnce_PANAAuthFailure verifies that a 0x6028 result != 0x01
// propagates as an error from connectOnce.
func TestConnectOnce_PANAAuthFailure(t *testing.T) {
	dir := t.TempDir()
	chPath := filepath.Join(dir, "channel")
	_ = (ChannelStore{Path: chPath}).Save(0x0C)

	hostSide, moduleSide := newPair()
	drv := bp35c2.New(hostSide, silentLogger(), 8)
	defer drv.Close()

	go func() {
		r := frame.NewReader(moduleSide)
		expect := []uint16{
			bp35c2.CmdBRouteAuthSet,
			bp35c2.CmdInitialSettings,
			bp35c2.CmdBRouteStart,
			bp35c2.CmdUDPPortOpen,
			bp35c2.CmdBRoutePANAStart,
		}
		for _, wantCmd := range expect {
			f, err := r.Read()
			if err != nil {
				return
			}
			if f.Command != wantCmd {
				t.Errorf("module: got cmd %#x, want %#x", f.Command, wantCmd)
				return
			}
			var respData []byte
			switch wantCmd {
			case bp35c2.CmdBRouteStart:
				respData = append(respData, 0x01, 0x0C)
				respData = binary.BigEndian.AppendUint16(respData, 0xABCD)
				respData = append(respData, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88)
				respData = append(respData, 0xC0)
			default:
				respData = []byte{0x01}
			}
			_, _ = moduleSide.Write(encResp(wantCmd|0x2000, respData))
			if wantCmd == bp35c2.CmdBRoutePANAStart {
				time.Sleep(10 * time.Millisecond)
				// result 0x02 = auth failure
				payload := append([]byte{0x02}, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
				_, _ = moduleSide.Write(encResp(bp35c2.NotifyPANAResult, payload))
			}
		}
	}()

	mgr := NewManager(drv, Config{
		BRouteID:         "0123456789ABCDEF0123456789ABCDEF",
		BRoutePassword:   "PASSWORD1234",
		ChannelStorePath: chPath,
		CommandTimeout:   1 * time.Second,
		PANAAuthTimeout:  1 * time.Second,
	}, silentLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go mgr.dispatchNotifications(ctx)

	_, _, err := mgr.connectOnce(ctx)
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

// Package bp35c2 owns the UART link to a BP35C2 (BP35C0-J11) module.
//
// The module is single-task: only one command may be in flight at a
// time; sending a second before the first is answered produces error
// 0x3D. The Driver enforces this with a request mutex.
//
// One goroutine reads frames; responses are routed to the pending
// Request call and notifications go on the Notifications channel.
// Writes happen on the caller's goroutine while holding the request
// mutex.
package bp35c2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/frame"
)

// Response is a successful reply frame from the module. Command is the
// response command code (typically request | 0x2000). Data is the
// payload after the frame header.
type Response struct {
	Command uint16
	Data    []byte
}

// Notification is an async frame from the module (upper nibble != 0x2).
type Notification struct {
	Command uint16
	Data    []byte
}

// Errors returned by Driver.
var (
	ErrClosed       = errors.New("bp35c2: driver closed")
	ErrBusy         = errors.New("bp35c2: another request in flight")
	ErrTimeout      = errors.New("bp35c2: response timeout")
	ErrUnexpectedResponse = errors.New("bp35c2: unexpected response command")
)

// ReadWriteCloser is the subset of io interfaces we need. Real
// implementations wrap serial.Port; tests use io.Pipe.
type ReadWriteCloser interface {
	io.Reader
	io.Writer
	io.Closer
}

// Driver owns a single BP35C2 serial link.
type Driver struct {
	rw     ReadWriteCloser
	log    *slog.Logger
	reader *frame.Reader

	// notifCh is the fan-out channel for async notifications.
	notifCh chan Notification

	// reqMu serialises Request(): the module can only handle one at a time.
	reqMu sync.Mutex

	// pending holds the state of the currently in-flight request. Access
	// is guarded by pendMu; the reader goroutine writes into resp when a
	// matching frame arrives.
	pendMu   sync.Mutex
	pending  *pendingRequest

	// closed is closed by Close(); readers use it to bail out.
	closeOnce sync.Once
	closed    chan struct{}

	// readErr is set (once) when the reader goroutine exits.
	readErrMu sync.Mutex
	readErr   error

	// Optional: a hook called synchronously for every notification.
	// Used mainly for tests / metrics.
	OnNotification func(Notification)
}

type pendingRequest struct {
	wantCmd uint16 // expected response cmd (request | 0x2000)
	resp    chan Response
	// If true, accept any 0x2xxx response (used for probing).
	// Not currently used but leaves room for it.
	acceptAny bool
}

// New constructs a Driver over rw and starts the reader goroutine.
// notifBuf is the size of the notifications channel.
func New(rw ReadWriteCloser, log *slog.Logger, notifBuf int) *Driver {
	if notifBuf <= 0 {
		notifBuf = 32
	}
	if log == nil {
		log = slog.Default()
	}
	d := &Driver{
		rw:      rw,
		log:     log,
		reader:  frame.NewReader(rw),
		notifCh: make(chan Notification, notifBuf),
		closed:  make(chan struct{}),
	}
	go d.readLoop()
	return d
}

// Close shuts down the driver. Safe to call multiple times.
func (d *Driver) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.closed)
		err = d.rw.Close()
		// Wake any in-flight request.
		d.pendMu.Lock()
		if d.pending != nil {
			close(d.pending.resp)
			d.pending = nil
		}
		d.pendMu.Unlock()
	})
	return err
}

// Notifications returns the receive-only channel of async notifications.
// If the buffer fills up, the driver drops the oldest notification and
// logs a warning — this is preferable to blocking the reader goroutine.
func (d *Driver) Notifications() <-chan Notification { return d.notifCh }

// Request sends cmd with the given data and blocks until a matching
// response arrives, ctx expires, or the driver is closed. The caller
// must not send another request concurrently — Request holds a mutex
// to serialise, so callers that share a Driver naturally queue.
//
// Note: the reset command 0x00D9 has no response; do not call Request
// for it, use SendRaw instead and wait on Notifications for 0x6019.
func (d *Driver) Request(ctx context.Context, cmd uint16, data []byte) (Response, error) {
	d.reqMu.Lock()
	defer d.reqMu.Unlock()

	select {
	case <-d.closed:
		return Response{}, ErrClosed
	default:
	}

	pr := &pendingRequest{
		wantCmd: cmd | 0x2000,
		resp:    make(chan Response, 1),
	}
	d.pendMu.Lock()
	if d.pending != nil {
		d.pendMu.Unlock()
		return Response{}, ErrBusy // should not happen given reqMu, but defensive
	}
	d.pending = pr
	d.pendMu.Unlock()

	// Always clear on exit.
	defer func() {
		d.pendMu.Lock()
		if d.pending == pr {
			d.pending = nil
		}
		d.pendMu.Unlock()
	}()

	if err := d.writeFrame(cmd, data); err != nil {
		return Response{}, err
	}

	select {
	case r, ok := <-pr.resp:
		if !ok {
			return Response{}, ErrClosed
		}
		return r, nil
	case <-ctx.Done():
		return Response{}, fmt.Errorf("%w: %v", ErrTimeout, ctx.Err())
	case <-d.closed:
		return Response{}, ErrClosed
	}
}

// SendRaw writes a request frame without waiting for a response.
// Only use for commands documented as "no response", currently
// hardware reset (0x00D9).
func (d *Driver) SendRaw(cmd uint16, data []byte) error {
	d.reqMu.Lock()
	defer d.reqMu.Unlock()
	return d.writeFrame(cmd, data)
}

// WaitNotification blocks until a notification with cmd arrives, ctx
// expires, or the driver is closed. Other notifications are re-queued
// onto the caller-facing channel so consumers still see them.
//
// This is a convenience: for the common case of "wait for boot notice",
// callers can subscribe to Notifications() directly.
func (d *Driver) WaitNotification(ctx context.Context, cmd uint16) (Notification, error) {
	for {
		select {
		case n, ok := <-d.notifCh:
			if !ok {
				return Notification{}, ErrClosed
			}
			if n.Command == cmd {
				return n, nil
			}
			// Not the one we wanted — put it back for other consumers.
			// This is best-effort: if the channel is full, drop it.
			select {
			case d.notifCh <- n:
			default:
				d.log.Warn("bp35c2: dropped notification during WaitNotification", "cmd", n.Command)
			}
			// Small delay to avoid hot-looping if we keep seeing the same
			// notification requeued.
			time.Sleep(5 * time.Millisecond)
		case <-ctx.Done():
			return Notification{}, fmt.Errorf("%w: %v", ErrTimeout, ctx.Err())
		case <-d.closed:
			return Notification{}, ErrClosed
		}
	}
}

func (d *Driver) writeFrame(cmd uint16, data []byte) error {
	b, err := frame.Encode(frame.Frame{
		Direction: frame.DirectionRequest,
		Command:   cmd,
		Data:      data,
	})
	if err != nil {
		return err
	}
	d.log.Debug("bp35c2 tx",
		"cmd", fmt.Sprintf("%#04x", cmd),
		"len", len(data),
		"hex", fmt.Sprintf("%X", b))
	_, err = d.rw.Write(b)
	return err
}

// ReadError returns the last error from the reader goroutine, or nil.
func (d *Driver) ReadError() error {
	d.readErrMu.Lock()
	defer d.readErrMu.Unlock()
	return d.readErr
}

func (d *Driver) readLoop() {
	defer func() {
		// Ensure any in-flight request wakes up.
		d.pendMu.Lock()
		if d.pending != nil {
			close(d.pending.resp)
			d.pending = nil
		}
		d.pendMu.Unlock()
		close(d.notifCh)
	}()
	for {
		f, err := d.reader.Read()
		if err != nil {
			select {
			case <-d.closed:
				return
			default:
			}
			d.readErrMu.Lock()
			d.readErr = err
			d.readErrMu.Unlock()
			d.log.Error("bp35c2 read failure — closing driver", "err", err)
			// Close the driver so consumers see it.
			d.closeOnce.Do(func() {
				close(d.closed)
				_ = d.rw.Close()
			})
			return
		}
		if f.Direction != frame.DirectionResponse {
			d.log.Warn("bp35c2: dropped host-direction frame", "cmd", fmt.Sprintf("%#04x", f.Command))
			continue
		}
		d.log.Debug("bp35c2 rx",
			"cmd", fmt.Sprintf("%#04x", f.Command),
			"len", len(f.Data),
			"hex", fmt.Sprintf("%X", f.Data))
		if isResponse(f.Command) {
			d.pendMu.Lock()
			pr := d.pending
			d.pendMu.Unlock()
			if pr != nil && (pr.acceptAny || f.Command == pr.wantCmd) {
				select {
				case pr.resp <- Response{Command: f.Command, Data: f.Data}:
				default:
					// Should never happen: buffered 1, cleared once used.
				}
				continue
			}
			// Unmatched response — log; do not drop into notifications channel.
			d.log.Warn("bp35c2: unmatched response frame", "cmd", fmt.Sprintf("%#04x", f.Command))
			continue
		}
		// Notification.
		n := Notification{Command: f.Command, Data: f.Data}
		if d.OnNotification != nil {
			d.OnNotification(n)
		}
		select {
		case d.notifCh <- n:
		default:
			d.log.Warn("bp35c2: notification channel full — dropping oldest")
			// Drain one to make room, then push.
			select {
			case <-d.notifCh:
			default:
			}
			select {
			case d.notifCh <- n:
			default:
			}
		}
	}
}

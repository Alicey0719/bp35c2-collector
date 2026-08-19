// Package bp35c2 drives a ROHM BP35C2 USB Wi-SUN dongle running the
// SKSTACK-IP firmware (ASCII, line-oriented).
//
// The module echoes every command back, then emits zero or more
// response lines, then finishes with either "OK" or "FAIL ERnn".
// Asynchronous events (EVENT, EPANDESC, ERXUDP) may arrive at any
// time; this driver routes them to Events() and only the command
// path sees echo + response + terminator.
//
// SKSENDTO is a hybrid: the ASCII header ends with a space, then
// exactly datalen raw bytes follow. CommandBinary handles that.
//
// Every ERXUDP arrives as a single ASCII line ending with the UDP
// payload — either raw bytes (ROPT=0, the module default) or hex
// (ROPT=1). We call "ROPT 01" at startup so parsing is line-only.
package bp35c2

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
)

// Event is one asynchronous message from the module.
type Event struct {
	Kind   string            // "EVENT", "EPANDESC", "ERXUDP", ...
	Params []string          // whitespace-tokenised trailing parameters
	Fields map[string]string // for EPANDESC: parsed Key:Value block
	Data   []byte            // for ERXUDP: hex-decoded UDP payload
	Raw    string            // original line (or joined block)
}

// Errors
var (
	ErrClosed  = errors.New("bp35c2: driver closed")
	ErrBusy    = errors.New("bp35c2: another command in flight")
	ErrTimeout = errors.New("bp35c2: command timeout")
)

// FailError is returned by Command when the module answered "FAIL ERnn".
type FailError struct {
	Code string // e.g. "ER04"
}

func (e *FailError) Error() string { return "bp35c2: module returned FAIL " + e.Code }

// ReadWriteCloser is the subset of io interfaces we need. Real
// implementations wrap serial.Port; tests use io.Pipe.
type ReadWriteCloser interface {
	io.Reader
	io.Writer
	io.Closer
}

// Driver owns a single BP35C2 serial link.
type Driver struct {
	rw  ReadWriteCloser
	log *slog.Logger

	// events fans out async messages.
	events chan Event

	// cmdMu serialises Command calls so the reader can attribute
	// non-event lines to the right pending request.
	cmdMu sync.Mutex

	// pending is set while a command is awaiting OK/FAIL. The reader
	// pushes non-event lines here and signals completion.
	pendMu  sync.Mutex
	pending *pendingCmd

	closeOnce sync.Once
	closed    chan struct{}

	readErrMu sync.Mutex
	readErr   error

	// epMu / epanBuf accumulate the multi-line EPANDESC block.
	epMu    sync.Mutex
	epanBuf []string
}

type pendingCmd struct {
	cmd  string      // the request (without CRLF) — used to skip echo
	resp []string    // accumulated response lines
	done chan result // signalled with terminal outcome
}

type result struct {
	ok    bool
	fail  string
	lines []string
	err   error
}

// New constructs a Driver over rw and starts the reader goroutine.
func New(rw ReadWriteCloser, log *slog.Logger, eventBuf int) *Driver {
	if eventBuf <= 0 {
		eventBuf = 32
	}
	if log == nil {
		log = slog.Default()
	}
	d := &Driver{
		rw:     rw,
		log:    log,
		events: make(chan Event, eventBuf),
		closed: make(chan struct{}),
	}
	go d.readLoop()
	return d
}

// Close shuts the driver down. Safe to call multiple times.
func (d *Driver) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.closed)
		err = d.rw.Close()
		d.pendMu.Lock()
		if d.pending != nil {
			select {
			case d.pending.done <- result{err: ErrClosed}:
			default:
			}
			d.pending = nil
		}
		d.pendMu.Unlock()
	})
	return err
}

// Events exposes the async event channel.
func (d *Driver) Events() <-chan Event { return d.events }

// ReadError returns the last reader-goroutine error, or nil.
func (d *Driver) ReadError() error {
	d.readErrMu.Lock()
	defer d.readErrMu.Unlock()
	return d.readErr
}

// Command writes line + "\r\n", waits for OK/FAIL, and returns any
// non-event response lines that arrived in between.
//
// If the module answers "FAIL ERnn", the returned err is *FailError.
// ctx cancellation returns ErrTimeout wrapping ctx.Err().
func (d *Driver) Command(ctx context.Context, line string) (resp []string, err error) {
	return d.command(ctx, line, nil)
}

// CommandBinary is the SKSENDTO variant: writes header (must not
// contain CRLF), then payload bytes, then CRLF, all in one write.
// Waits for OK/FAIL the same way Command does.
func (d *Driver) CommandBinary(ctx context.Context, header string, payload []byte) (resp []string, err error) {
	return d.command(ctx, header, payload)
}

func (d *Driver) command(ctx context.Context, line string, payload []byte) ([]string, error) {
	d.cmdMu.Lock()
	defer d.cmdMu.Unlock()

	select {
	case <-d.closed:
		return nil, ErrClosed
	default:
	}

	pc := &pendingCmd{cmd: line, done: make(chan result, 1)}
	d.pendMu.Lock()
	if d.pending != nil {
		d.pendMu.Unlock()
		return nil, ErrBusy
	}
	d.pending = pc
	d.pendMu.Unlock()
	defer func() {
		d.pendMu.Lock()
		if d.pending == pc {
			d.pending = nil
		}
		d.pendMu.Unlock()
	}()

	if err := d.writeCommand(line, payload); err != nil {
		return nil, err
	}

	select {
	case r := <-pc.done:
		if r.err != nil {
			return r.lines, r.err
		}
		if !r.ok {
			return r.lines, &FailError{Code: r.fail}
		}
		return r.lines, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrTimeout, ctx.Err())
	case <-d.closed:
		return nil, ErrClosed
	}
}

func (d *Driver) writeCommand(line string, payload []byte) error {
	// For safety, redact credentials in debug logs.
	logLine := line
	switch {
	case strings.HasPrefix(line, "SKSETPWD"):
		logLine = "SKSETPWD ***"
	case strings.HasPrefix(line, "SKSETRBID"):
		logLine = "SKSETRBID ***"
	}
	if len(payload) > 0 {
		d.log.Debug("bp35c2 tx", "line", logLine, "payload_len", len(payload))
	} else {
		d.log.Debug("bp35c2 tx", "line", logLine)
	}
	buf := make([]byte, 0, len(line)+len(payload)+2)
	buf = append(buf, line...)
	buf = append(buf, payload...)
	buf = append(buf, '\r', '\n')
	_, err := d.rw.Write(buf)
	return err
}

func (d *Driver) readLoop() {
	defer func() {
		d.pendMu.Lock()
		if d.pending != nil {
			select {
			case d.pending.done <- result{err: ErrClosed}:
			default:
			}
			d.pending = nil
		}
		d.pendMu.Unlock()
		close(d.events)
	}()

	scanner := bufio.NewScanner(d.rw)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	scanner.Split(scanLinesCRLF)

	for scanner.Scan() {
		raw := scanner.Text()
		trimmed := strings.TrimRight(raw, "\r")
		if trimmed == "" {
			continue
		}
		d.log.Debug("bp35c2 rx", "line", trimmed)
		d.handleLine(trimmed)
	}
	if err := scanner.Err(); err != nil {
		select {
		case <-d.closed:
			return
		default:
		}
		d.readErrMu.Lock()
		d.readErr = err
		d.readErrMu.Unlock()
		d.log.Error("bp35c2 read failure — closing driver", "err", err)
		d.closeOnce.Do(func() {
			close(d.closed)
			_ = d.rw.Close()
		})
	}
}

// scanLinesCRLF splits on \n but keeps behaviour identical for lone
// \n or \r\n (bufio's default already handles that). Kept as a named
// function so tests can swap in easily.
func scanLinesCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	for i, b := range data {
		if b == '\n' {
			return i + 1, data[:i], nil
		}
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// handleLine routes one line to either the pending command or the
// event channel.
func (d *Driver) handleLine(line string) {
	// EPANDESC is multi-line: header + 6 indented "Key:Value" rows.
	// We accumulate here.
	d.epMu.Lock()
	if d.epanBuf != nil {
		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			d.epanBuf = append(d.epanBuf, line)
			// EPANDESC has 6 fields; emit when we've got all 6 rows.
			if len(d.epanBuf) >= 7 {
				ev := buildEPANDESC(d.epanBuf)
				d.epanBuf = nil
				d.epMu.Unlock()
				d.pushEvent(ev)
				return
			}
			d.epMu.Unlock()
			return
		}
		// Non-indented line ends the EPANDESC block early — emit whatever
		// we have and fall through to process the new line.
		ev := buildEPANDESC(d.epanBuf)
		d.epanBuf = nil
		d.epMu.Unlock()
		d.pushEvent(ev)
	} else {
		d.epMu.Unlock()
	}

	switch {
	case line == "OK":
		d.completePending(true, "", nil)
	case strings.HasPrefix(line, "FAIL "):
		code := strings.TrimPrefix(line, "FAIL ")
		d.completePending(false, strings.TrimSpace(code), nil)
	case line == "EPANDESC":
		d.epMu.Lock()
		d.epanBuf = []string{line}
		d.epMu.Unlock()
	case strings.HasPrefix(line, "EVENT "):
		d.pushEvent(parseEvent("EVENT", line))
	case strings.HasPrefix(line, "ERXUDP "):
		d.pushEvent(parseERXUDP(line))
	case strings.HasPrefix(line, "ERXTCP "), strings.HasPrefix(line, "ETIMER"):
		d.pushEvent(parseEvent(strings.SplitN(line, " ", 2)[0], line))
	default:
		// Command echo: skip
		d.pendMu.Lock()
		pc := d.pending
		d.pendMu.Unlock()
		if pc != nil && line == pc.cmd {
			return
		}
		// Response line (EINFO, EVER, EADDR, EPONG, etc.)
		if pc != nil {
			d.pendMu.Lock()
			if d.pending == pc {
				d.pending.resp = append(d.pending.resp, line)
			}
			d.pendMu.Unlock()
			return
		}
		d.log.Debug("bp35c2: unattributed line", "line", line)
	}
}

func (d *Driver) completePending(ok bool, fail string, err error) {
	d.pendMu.Lock()
	pc := d.pending
	if pc != nil {
		d.pending = nil
	}
	d.pendMu.Unlock()
	if pc == nil {
		return
	}
	select {
	case pc.done <- result{ok: ok, fail: fail, lines: pc.resp, err: err}:
	default:
	}
}

func (d *Driver) pushEvent(ev Event) {
	select {
	case d.events <- ev:
	default:
		d.log.Warn("bp35c2 event channel full — dropping oldest")
		select {
		case <-d.events:
		default:
		}
		select {
		case d.events <- ev:
		default:
		}
	}
}

// parseEvent splits "KIND arg1 arg2 ..." into an Event.
func parseEvent(kind, line string) Event {
	rest := strings.TrimSpace(strings.TrimPrefix(line, kind))
	fields := strings.Fields(rest)
	return Event{Kind: kind, Params: fields, Raw: line}
}

// parseERXUDP parses the receive line in ROPT=1 (hex) mode.
//
//	ERXUDP <sender> <dest> <sport> <dport> <senderlladdr> <secured> <side> <datalen> <data>
//
// In ROPT=0 the trailing data is raw binary and this parser mangles
// it — we set ROPT 01 at startup so this branch is safe.
func parseERXUDP(line string) Event {
	fields := strings.Fields(strings.TrimPrefix(line, "ERXUDP "))
	ev := Event{Kind: "ERXUDP", Params: fields, Raw: line}
	if len(fields) < 9 {
		return ev
	}
	if data, err := hex.DecodeString(fields[8]); err == nil {
		ev.Data = data
	}
	return ev
}

// buildEPANDESC turns the collected lines into an Event with Fields
// populated. Expected layout:
//
//	EPANDESC
//	  Channel: 33
//	  Channel Page: 09
//	  Pan ID: 8888
//	  Addr: 001D129012341234
//	  LQI: E1
//	  PairID: 12345678
func buildEPANDESC(lines []string) Event {
	ev := Event{Kind: "EPANDESC", Fields: make(map[string]string, 6), Raw: strings.Join(lines, "\n")}
	for _, l := range lines[1:] {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		colon := strings.IndexByte(l, ':')
		if colon <= 0 {
			continue
		}
		key := strings.TrimSpace(l[:colon])
		val := strings.TrimSpace(l[colon+1:])
		ev.Fields[key] = val
	}
	return ev
}

// waitForEvent-style helpers are up on the manager, not the driver.
// Consumers subscribe to Events() and filter themselves.

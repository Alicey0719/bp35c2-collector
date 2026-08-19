// Package broute drives the SKSTACK-IP BP35C2 through the full
// B-route connection lifecycle and hands ECHONET Lite payloads to
// higher layers.
//
// The Manager runs a supervisor goroutine that:
//
//  1. Resets the module and runs the join sequence (SKSETPWD +
//     SKSETRBID, SKSCAN, SKSREG, SKLL64, SKJOIN).
//  2. Consumes async events from the driver. ERXUDP flows out on
//     Incoming(); EVENT 24/27/28/29 tear down and drive the
//     reconnection loop.
//  3. On disconnect / auth-fail, backs off exponentially and retries.
//
// Higher layers only interact with Session, obtained via
// WaitConnected().
package broute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
)

// Config parameters — populated from the top-level config file.
type Config struct {
	// BRouteID is the 32-char Route-B ID.
	BRouteID string
	// BRoutePassword is the 12-char Route-B password.
	BRoutePassword string
	// ScanDuration is the SKSCAN duration value (0..14). Real time per
	// channel = 9.6 ms * 2^N. Default 6 (~0.6s/ch).
	ScanDuration byte
	// ChannelMask: bitmap of channels to scan. Default 0xFFFFFFFF
	// (scan all).
	ChannelMask uint32
	// JoinTimeout is how long to wait for EVENT 25/24 after SKJOIN.
	// Spec allows several minutes on first pairing; give ourselves
	// headroom. Default 3 min.
	JoinTimeout time.Duration
	// CommandTimeout applies to synchronous commands (SKSETPWD,
	// SKSREG, etc.). Default 6s.
	CommandTimeout time.Duration
	// InitialBackoff / MaxBackoff bracket the reconnection back-off.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// Incoming carries one received ECHONET Lite payload.
type Incoming struct {
	SrcIP   net.IP
	SrcPort uint16
	Payload []byte
	At      time.Time
}

// Session represents a live B-route connection.
type Session struct {
	MeterIP  net.IP
	PANID    uint16
	Channel  byte
	MeterMAC string // hex, 16 chars

	mgr *Manager
}

// SendUDP sends payload to the meter over UDP port 3610.
func (s *Session) SendUDP(ctx context.Context, payload []byte) error {
	return s.mgr.sendUDP(ctx, s.MeterIP, payload)
}

// Manager owns the driver and drives the connection lifecycle.
type Manager struct {
	cfg Config
	drv *bp35c2.Driver
	log *slog.Logger

	state atomic.Int32 // holds a State

	incoming chan Incoming

	sessMu  sync.RWMutex
	session *Session

	condMu      sync.Mutex
	connectedCh chan struct{}

	// joinResultCh is non-nil while the supervisor is blocking on the
	// EVENT 24/25 outcome after SKJOIN. dispatchEvents forwards those
	// events here when set.
	joinMu       sync.Mutex
	joinResultCh chan bp35c2.Event

	// scanResultCh is non-nil while an active scan is running.
	// dispatchEvents forwards EPANDESC and EVENT 22 here when set.
	scanMu       sync.Mutex
	scanResultCh chan bp35c2.Event

	// disc receives the reason the current connection died.
	discMu sync.Mutex
	disc   chan error

	OnReconnect   func()
	OnAuthFailure func()
	OnStateChange func(State)
}

// NewManager constructs a Manager. The caller retains ownership of drv.
func NewManager(drv *bp35c2.Driver, cfg Config, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	if cfg.ScanDuration == 0 {
		cfg.ScanDuration = 6
	}
	if cfg.ChannelMask == 0 {
		cfg.ChannelMask = 0xFFFFFFFF
	}
	if cfg.JoinTimeout == 0 {
		cfg.JoinTimeout = 3 * time.Minute
	}
	if cfg.CommandTimeout == 0 {
		cfg.CommandTimeout = 6 * time.Second
	}
	if cfg.InitialBackoff == 0 {
		cfg.InitialBackoff = 5 * time.Second
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 300 * time.Second
	}
	m := &Manager{
		cfg:         cfg,
		drv:         drv,
		log:         log.With("component", "broute"),
		incoming:    make(chan Incoming, 32),
		connectedCh: make(chan struct{}),
	}
	m.state.Store(int32(StateDisconnected))
	return m
}

// State returns the current connection state.
func (m *Manager) State() State { return State(m.state.Load()) }

// Incoming exposes the receive-only channel of ECHONET Lite payloads.
func (m *Manager) Incoming() <-chan Incoming { return m.incoming }

// WaitConnected blocks until a Session is available or ctx expires.
func (m *Manager) WaitConnected(ctx context.Context) (*Session, error) {
	for {
		m.sessMu.RLock()
		s := m.session
		m.sessMu.RUnlock()
		if s != nil {
			return s, nil
		}
		m.condMu.Lock()
		ch := m.connectedCh
		m.condMu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Run supervises the connection until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) error {
	go m.dispatchEvents(ctx)

	backoff := m.cfg.InitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.setState(StateInitializing)
		sess, err := m.connectOnce(ctx)
		if err != nil {
			m.log.Error("broute connect failed", "err", err, "backoff", backoff.String())
			if m.OnReconnect != nil {
				m.OnReconnect()
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, m.cfg.MaxBackoff)
			continue
		}
		backoff = m.cfg.InitialBackoff
		m.installSession(sess)
		m.setState(StateConnected)
		m.log.Info("broute connected",
			"meter_ip", sess.MeterIP.String(),
			"channel", sess.Channel,
			"pan_id", fmt.Sprintf("%#04x", sess.PANID))

		reason := m.waitForDisconnect(ctx)
		m.clearSession()
		m.setState(StateReconnecting)
		if m.OnReconnect != nil {
			m.OnReconnect()
		}
		if errors.Is(reason, context.Canceled) || errors.Is(reason, context.DeadlineExceeded) {
			return reason
		}
		m.log.Warn("broute link lost", "err", reason)
	}
}

func (m *Manager) installSession(s *Session) {
	m.sessMu.Lock()
	m.session = s
	m.sessMu.Unlock()
	m.condMu.Lock()
	close(m.connectedCh)
	m.condMu.Unlock()
}

func (m *Manager) clearSession() {
	m.sessMu.Lock()
	m.session = nil
	m.sessMu.Unlock()
	m.condMu.Lock()
	m.connectedCh = make(chan struct{})
	m.condMu.Unlock()
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		next = max
	}
	return next
}

func (m *Manager) setState(s State) {
	prev := State(m.state.Swap(int32(s)))
	if prev != s && m.OnStateChange != nil {
		m.OnStateChange(s)
	}
}

func (m *Manager) waitForDisconnect(ctx context.Context) error {
	m.discMu.Lock()
	m.disc = make(chan error, 1)
	ch := m.disc
	m.discMu.Unlock()
	defer func() {
		m.discMu.Lock()
		m.disc = nil
		m.discMu.Unlock()
	}()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) notifyDisconnect(err error) {
	m.discMu.Lock()
	ch := m.disc
	m.discMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

// dispatchEvents is the sole consumer of drv.Events().
func (m *Manager) dispatchEvents(ctx context.Context) {
	for {
		select {
		case ev, ok := <-m.drv.Events():
			if !ok {
				m.notifyDisconnect(errors.New("driver events channel closed"))
				return
			}
			m.handleEvent(ev)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) handleEvent(ev bp35c2.Event) {
	switch ev.Kind {
	case "ERXUDP":
		inc, ok := parseIncoming(ev)
		if !ok {
			return
		}
		select {
		case m.incoming <- inc:
		default:
			m.log.Warn("incoming channel full — dropping ECHONET Lite frame")
		}
	case "EPANDESC":
		m.scanNotify(ev)
	case "EVENT":
		if len(ev.Params) < 1 {
			return
		}
		code := ev.Params[0]
		switch code {
		case "21":
			// UDP send complete — the last param carries the send result
			// (00=success, others=failure). We currently rely on
			// SKSENDTO's OK/FAIL for the sync path, so this event is
			// mostly informational.
		case "22":
			m.scanNotify(ev)
		case "24":
			m.joinNotify(ev)
			if m.OnAuthFailure != nil {
				m.OnAuthFailure()
			}
			m.notifyDisconnect(errors.New("PANA connection failed (EVENT 24)"))
		case "25":
			m.joinNotify(ev)
		case "26":
			m.log.Info("PANA session lifetime — module will re-authenticate")
		case "27", "28", "29":
			m.notifyDisconnect(fmt.Errorf("PANA session terminated (EVENT %s)", code))
		case "32":
			m.log.Warn("ARIB transmit rate limit reached (EVENT 32)")
		case "33":
			m.log.Info("ARIB transmit rate limit lifted (EVENT 33)")
		default:
			m.log.Debug("unhandled EVENT", "code", code)
		}
	}
}

// scanNotify forwards EPANDESC / EVENT 22 events to an in-progress
// activeScan. Blocks briefly if the channel is momentarily full
// (buffered) rather than dropping — scan results are precious.
func (m *Manager) scanNotify(ev bp35c2.Event) {
	m.scanMu.Lock()
	ch := m.scanResultCh
	m.scanMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- ev:
	default:
		// Full — allow a short blocking wait so we don't lose EPANDESCs
		// during a burst.
		select {
		case ch <- ev:
		case <-time.After(50 * time.Millisecond):
			m.log.Warn("dropped scan event under back-pressure", "kind", ev.Kind)
		}
	}
}

// joinNotify forwards EVENT 24/25 to the connectOnce loop if it's
// currently waiting for the join outcome. Otherwise the event is
// swallowed (already have a session or the join path timed out).
func (m *Manager) joinNotify(ev bp35c2.Event) {
	m.joinMu.Lock()
	ch := m.joinResultCh
	m.joinMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- ev:
	default:
	}
}

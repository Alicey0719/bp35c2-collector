// Package broute drives the BP35C2 through the full B-route
// connection lifecycle and hands ECHONET Lite payloads to higher
// layers.
//
// The Manager runs a supervisor goroutine that:
//
//  1. Powers up the module and performs the init/PANA sequence
//     (skipping active scan if we already know a good channel).
//  2. Consumes async notifications from the driver: the ECHONET Lite
//     data receive path (0x6018) is republished on Incoming(); link
//     state changes (0x601A) and PANA re-auth failures (0x6028) drive
//     the reconnection loop.
//  3. On disconnect / auth-fail, tears down and reconnects with
//     exponential backoff. After three consecutive MAC-join failures
//     the cached channel is invalidated so the next attempt re-scans
//     (handles a meter that moved bands).
//
// Higher layers only interact with Session, obtained via
// WaitConnected(); the Session guarantees SendUDP is only attempted
// while the underlying link is up.
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
	// ChannelStorePath persists the last successful channel.
	ChannelStorePath string
	// ScanTimeExp: the S value passed to CmdActiveScan (0x01..0x0E).
	// Real time per channel = 9.64ms * 2^S. Default 6 (~0.6s/ch).
	ScanTimeExp byte
	// ChannelMask: bitmap of channels to scan. Default 0x0003FFF0
	// (channels 4..17).
	ChannelMask uint32
	// PANAAuthTimeout is how long we wait for the auth result
	// notification after CmdBRoutePANAStart. Spec allows up to 706s
	// on first pairing; give ourselves headroom.
	PANAAuthTimeout time.Duration
	// CommandTimeout applies to synchronous responses (initial
	// settings, port open, etc). Default 6s per doc + 1s.
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
	RSSI    int8
	At      time.Time
}

// Session represents a live B-route connection.
type Session struct {
	MeterIP  net.IP
	PANID    uint16
	Channel  byte
	MeterMAC [8]byte
	RSSI     int8

	mgr *Manager
}

// SendUDP sends payload to the meter over UDP port 3610. It blocks on
// the module's CmdUDPSend response.
func (s *Session) SendUDP(ctx context.Context, payload []byte) error {
	return s.mgr.sendUDP(ctx, s.MeterIP, payload)
}

// Manager owns the driver and drives the connection lifecycle.
type Manager struct {
	cfg   Config
	drv   *bp35c2.Driver
	log   *slog.Logger
	store ChannelStore

	state atomic.Int32 // holds a State

	incoming chan Incoming

	sessMu  sync.RWMutex
	session *Session

	condMu      sync.Mutex
	connectedCh chan struct{}

	// panaResultCh is non-nil while the supervisor is blocking on
	// the PANA authentication result. dispatchNotifications sends
	// 0x6028 frames here when set; otherwise it treats them as an
	// unsolicited auto-re-auth failure notification.
	panaMu       sync.Mutex
	panaResultCh chan bp35c2.Notification

	// disc receives the reason the current connection died.
	discMu sync.Mutex
	disc   chan error

	OnReconnect   func()
	OnAuthFailure func()
	OnStateChange func(State)
}

// NewManager constructs a Manager. The caller retains ownership of drv
// and is responsible for closing it after Manager.Run returns.
func NewManager(drv *bp35c2.Driver, cfg Config, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	if cfg.ScanTimeExp == 0 {
		cfg.ScanTimeExp = 6
	}
	if cfg.ChannelMask == 0 {
		cfg.ChannelMask = 0x0003FFF0
	}
	if cfg.PANAAuthTimeout == 0 {
		cfg.PANAAuthTimeout = 720 * time.Second
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
		store:       ChannelStore{Path: cfg.ChannelStorePath},
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

// Run supervises the connection until ctx is cancelled. Returns when
// ctx is done.
func (m *Manager) Run(ctx context.Context) error {
	go m.dispatchNotifications(ctx)

	backoff := m.cfg.InitialBackoff
	consecJoinFailures := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.setState(StateInitializing)
		sess, joinErr, err := m.connectOnce(ctx)
		if err != nil {
			m.log.Error("broute connect failed", "err", err, "backoff", backoff.String())
			if joinErr {
				consecJoinFailures++
				if consecJoinFailures >= 3 {
					m.log.Warn("clearing cached channel after repeated join failures")
					_ = m.store.Clear()
					consecJoinFailures = 0
				}
			}
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
		consecJoinFailures = 0
		backoff = m.cfg.InitialBackoff
		m.installSession(sess)
		m.setState(StateConnected)
		m.log.Info("broute connected",
			"meter_ip", sess.MeterIP.String(),
			"channel", sess.Channel,
			"pan_id", fmt.Sprintf("%#04x", sess.PANID),
			"rssi_dbm", sess.RSSI)

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

// dispatchNotifications is the sole consumer of drv.Notifications().
func (m *Manager) dispatchNotifications(ctx context.Context) {
	for {
		select {
		case n, ok := <-m.drv.Notifications():
			if !ok {
				m.notifyDisconnect(errors.New("driver notifications channel closed"))
				return
			}
			m.handleNotification(n)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) handleNotification(n bp35c2.Notification) {
	switch n.Command {
	case bp35c2.NotifyUDPReceive:
		inc, err := parseUDPReceive(n.Data)
		if err != nil {
			m.log.Warn("bad 0x6018 payload", "err", err)
			return
		}
		select {
		case m.incoming <- inc:
		default:
			m.log.Warn("incoming channel full — dropping ECHONET Lite frame")
		}
	case bp35c2.NotifyPANAResult:
		m.panaMu.Lock()
		ch := m.panaResultCh
		m.panaMu.Unlock()
		if ch != nil {
			select {
			case ch <- n:
			default:
			}
			return
		}
		if len(n.Data) < 1 || n.Data[0] != 0x01 {
			m.log.Warn("PANA auto re-auth failed", "result", fmt.Sprintf("%#x", firstByte(n.Data)))
			if m.OnAuthFailure != nil {
				m.OnAuthFailure()
			}
			m.notifyDisconnect(errors.New("PANA re-authentication failed"))
		}
	case bp35c2.NotifyLinkStateChg:
		if len(n.Data) < 1 {
			return
		}
		switch n.Data[0] {
		case 0x03:
			m.notifyDisconnect(errors.New("MAC link lost"))
		case 0x04:
			m.notifyDisconnect(errors.New("PANA link lost"))
		default:
			m.log.Debug("link state change", "state", fmt.Sprintf("%#x", n.Data[0]))
		}
	case bp35c2.NotifyBoot:
		m.log.Info("module boot notification received")
	default:
		m.log.Debug("unhandled notification", "cmd", fmt.Sprintf("%#04x", n.Command))
	}
}

func firstByte(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

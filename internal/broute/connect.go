package broute

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
)

// connectOnce performs the full SKSTACK-IP join sequence.
//
// Sequence (per ROHM SKSTACK-IP application note):
//
//	SKRESET                              // clean state
//	ROPT 01 / WOPT 01                    // ERXUDP hex mode (best-effort)
//	SKSETPWD C <pass>                    // Route-B password
//	SKSETRBID <id>                       // Route-B ID
//	SKSCAN 2 <mask> <dur> 0              // scan; collect EPANDESC, EVENT 22
//	SKSREG S2 <ch>                       // set channel
//	SKSREG S3 <panid>                    // set PAN ID
//	SKLL64 <mac>                         // MAC → IPv6 (handles U/L flip)
//	SKJOIN <ipv6>                        // PANA; wait EVENT 25 or 24
func (m *Manager) connectOnce(ctx context.Context) (*Session, error) {
	m.setState(StateInitializing)
	// SKRESET has no OK — it just replies with OK. Some firmwares also
	// emit a boot notice first. Give it a slightly larger timeout.
	if err := m.simple(ctx, "SKRESET", 5*time.Second); err != nil {
		return nil, fmt.Errorf("SKRESET: %w", err)
	}
	// Switch to hex-encoded ERXUDP data so the reader stays line-based.
	// Some firmwares don't support these — best-effort only.
	if err := m.simple(ctx, "WOPT 01", m.cfg.CommandTimeout); err != nil {
		m.log.Debug("WOPT 01 not supported (continuing with default)", "err", err)
	}
	if err := m.simple(ctx, "ROPT 01", m.cfg.CommandTimeout); err != nil {
		m.log.Debug("ROPT 01 not supported (continuing with default)", "err", err)
	}
	if err := m.simple(ctx, "SKSETPWD C "+m.cfg.BRoutePassword, m.cfg.CommandTimeout); err != nil {
		return nil, fmt.Errorf("SKSETPWD: %w", err)
	}
	if err := m.simple(ctx, "SKSETRBID "+m.cfg.BRouteID, m.cfg.CommandTimeout); err != nil {
		return nil, fmt.Errorf("SKSETRBID: %w", err)
	}

	m.setState(StateScanning)
	desc, err := m.activeScan(ctx)
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	m.log.Info("meter found",
		"channel", desc.Channel,
		"pan_id", fmt.Sprintf("%#04x", desc.PANID),
		"addr", desc.Addr,
		"lqi", desc.LQI)

	if err := m.simple(ctx, fmt.Sprintf("SKSREG S2 %02X", desc.Channel), m.cfg.CommandTimeout); err != nil {
		return nil, fmt.Errorf("SKSREG S2: %w", err)
	}
	if err := m.simple(ctx, fmt.Sprintf("SKSREG S3 %04X", desc.PANID), m.cfg.CommandTimeout); err != nil {
		return nil, fmt.Errorf("SKSREG S3: %w", err)
	}

	ipv6, err := m.macToIPv6(ctx, desc.Addr)
	if err != nil {
		return nil, fmt.Errorf("SKLL64: %w", err)
	}

	m.setState(StateJoining)
	if err := m.doJoin(ctx, ipv6); err != nil {
		return nil, err
	}

	return &Session{
		MeterIP:  ipv6,
		PANID:    desc.PANID,
		Channel:  desc.Channel,
		MeterMAC: desc.Addr,
		mgr:      m,
	}, nil
}

// simple runs a command that we expect to return OK with no meaningful
// response body.
func (m *Manager) simple(ctx context.Context, line string, timeout time.Duration) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := m.drv.Command(cctx, line)
	return err
}

// EPANDescriptor is one scan result parsed out of an EPANDESC event.
type EPANDescriptor struct {
	Channel byte
	PANID   uint16
	Addr    string // 16 hex chars
	LQI     byte
	PairID  string
}

// activeScan runs SKSCAN and collects results until EVENT 22.
//
// dispatchEvents routes EPANDESC and EVENT 22 to scanResultCh while
// we're subscribed, so we don't fight the dispatcher over events.
func (m *Manager) activeScan(ctx context.Context) (EPANDescriptor, error) {
	scanCh := make(chan bp35c2.Event, 16)
	m.scanMu.Lock()
	m.scanResultCh = scanCh
	m.scanMu.Unlock()
	defer func() {
		m.scanMu.Lock()
		m.scanResultCh = nil
		m.scanMu.Unlock()
	}()

	line := fmt.Sprintf("SKSCAN 2 %08X %d 0", m.cfg.ChannelMask, m.cfg.ScanDuration)
	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	if _, err := m.drv.Command(cctx, line); err != nil {
		cancel()
		return EPANDescriptor{}, fmt.Errorf("SKSCAN: %w", err)
	}
	cancel()

	// Overall wait: 14 channels × 9.6ms × 2^N + generous slack.
	scanBudget := time.Duration(14) * (10 * time.Millisecond) * (1 << m.cfg.ScanDuration)
	if scanBudget < 5*time.Second {
		scanBudget = 5 * time.Second
	}
	scanBudget += 5 * time.Second

	wctx, wcancel := context.WithTimeout(ctx, scanBudget)
	defer wcancel()
	var found *EPANDescriptor
	for {
		select {
		case ev := <-scanCh:
			switch ev.Kind {
			case "EPANDESC":
				desc, err := parseEPANDESC(ev)
				if err != nil {
					m.log.Warn("bad EPANDESC", "err", err)
					continue
				}
				if found == nil || desc.LQI > found.LQI {
					copy := desc
					found = &copy
				}
			case "EVENT":
				if len(ev.Params) >= 1 && ev.Params[0] == "22" {
					if found != nil {
						return *found, nil
					}
					return EPANDescriptor{}, errors.New("scan complete but no meter responded")
				}
			}
		case <-wctx.Done():
			if found != nil {
				return *found, nil
			}
			return EPANDescriptor{}, errors.New("scan timed out with no beacon")
		}
	}
}

func parseEPANDESC(ev bp35c2.Event) (EPANDescriptor, error) {
	if ev.Fields == nil {
		return EPANDescriptor{}, errors.New("EPANDESC has no fields")
	}
	get := func(k string) string { return strings.TrimSpace(ev.Fields[k]) }

	chS := get("Channel")
	ch64, err := strconv.ParseUint(chS, 16, 8)
	if err != nil {
		return EPANDescriptor{}, fmt.Errorf("Channel %q: %w", chS, err)
	}
	panS := get("Pan ID")
	pan64, err := strconv.ParseUint(panS, 16, 16)
	if err != nil {
		return EPANDescriptor{}, fmt.Errorf("Pan ID %q: %w", panS, err)
	}
	addr := get("Addr")
	if len(addr) != 16 {
		return EPANDescriptor{}, fmt.Errorf("Addr %q not 16 hex chars", addr)
	}
	if _, err := hex.DecodeString(addr); err != nil {
		return EPANDescriptor{}, fmt.Errorf("Addr %q: %w", addr, err)
	}
	lqi := byte(0)
	if s := get("LQI"); s != "" {
		if v, err := strconv.ParseUint(s, 16, 8); err == nil {
			lqi = byte(v)
		}
	}
	return EPANDescriptor{
		Channel: byte(ch64),
		PANID:   uint16(pan64),
		Addr:    addr,
		LQI:     lqi,
		PairID:  get("PairID"),
	}, nil
}

// macToIPv6 asks the module to convert the neighbour's MAC to its
// link-local IPv6 (with the U/L bit flip handled by firmware).
//
// SKLL64 response is one line containing the IPv6 in canonical form.
func (m *Manager) macToIPv6(ctx context.Context, addr string) (net.IP, error) {
	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	defer cancel()
	resp, err := m.drv.Command(cctx, "SKLL64 "+addr)
	if err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		return nil, errors.New("SKLL64: empty response")
	}
	ip := net.ParseIP(strings.TrimSpace(resp[0]))
	if ip == nil {
		return nil, fmt.Errorf("SKLL64: bad IP %q", resp[0])
	}
	return ip, nil
}

// doJoin sends SKJOIN and waits for the async EVENT 25 (success) or
// EVENT 24 (failure). SKJOIN itself returns OK immediately; the join
// outcome is separate.
func (m *Manager) doJoin(ctx context.Context, ip net.IP) error {
	joinCh := make(chan bp35c2.Event, 1)
	m.joinMu.Lock()
	m.joinResultCh = joinCh
	m.joinMu.Unlock()
	defer func() {
		m.joinMu.Lock()
		m.joinResultCh = nil
		m.joinMu.Unlock()
	}()

	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	if _, err := m.drv.Command(cctx, "SKJOIN "+ip.String()); err != nil {
		cancel()
		return fmt.Errorf("SKJOIN: %w", err)
	}
	cancel()

	wctx, wcancel := context.WithTimeout(ctx, m.cfg.JoinTimeout)
	defer wcancel()
	select {
	case ev := <-joinCh:
		if len(ev.Params) >= 1 && ev.Params[0] == "25" {
			return nil
		}
		return fmt.Errorf("PANA authentication failed (EVENT %s)", ev.Params[0])
	case <-wctx.Done():
		return fmt.Errorf("PANA join timed out after %s", m.cfg.JoinTimeout)
	}
}

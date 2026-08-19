package broute

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/bp35c2"
)

// connectOnce performs the full connect sequence once. On success it
// returns a live Session. joinErr is true when the failure looked like
// a MAC-level join problem (bad channel, meter unreachable) — the
// supervisor uses it to invalidate the cached channel after repeated
// failures.
func (m *Manager) connectOnce(ctx context.Context) (*Session, bool, error) {
	// 1. Ensure module is in known state. Hardware reset is
	//    intentionally NOT sent every attempt — it clobbers any
	//    other in-progress state and forces a full re-init. On the
	//    very first attempt after boot, most modules deliver the
	//    boot notification (0x6019) unsolicited; we don't block on
	//    it (it may have already been consumed by another
	//    reconnect cycle). Instead we rely on responses to actual
	//    commands to prove the module is alive.

	channel, err := m.pickInitialChannel(ctx)
	if err != nil {
		return nil, false, err
	}

	// 2. Push Route-B credentials (always: module is stateless).
	if err := m.setBRouteAuth(ctx); err != nil {
		return nil, false, err
	}

	// 3. Re-run initial settings with the confirmed channel.
	if err := m.setInitialSettings(ctx, channel); err != nil {
		return nil, false, err
	}

	// 4. Start B-route → meter MAC / PAN ID.
	m.setState(StateJoining)
	sess, err := m.startBRoute(ctx, channel)
	if err != nil {
		return nil, true, err // treat as join failure
	}

	// 5. Open UDP port 3610.
	if err := m.openUDPPort(ctx); err != nil {
		return nil, false, err
	}

	// 6. Kick off PANA and wait for the result notification.
	if err := m.doPANA(ctx); err != nil {
		return nil, false, err
	}

	// 7. Persist the channel we actually got connected on.
	if err := m.store.Save(channel); err != nil {
		m.log.Warn("failed to persist channel", "err", err)
	}
	sess.mgr = m
	return sess, false, nil
}

func (m *Manager) pickInitialChannel(ctx context.Context) (byte, error) {
	if ch, err := m.store.Load(); err == nil {
		m.log.Info("using cached channel from previous session", "channel", ch)
		return ch, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		m.log.Warn("failed to load cached channel — falling back to scan", "err", err)
	}
	m.setState(StateScanning)

	// Initial settings before scanning need Dual mode set.
	if err := m.setInitialSettings(ctx, 0x04); err != nil {
		return 0, fmt.Errorf("pre-scan initial settings: %w", err)
	}
	if err := m.setBRouteAuth(ctx); err != nil {
		return 0, fmt.Errorf("pre-scan auth setup: %w", err)
	}
	ch, err := m.activeScan(ctx)
	if err != nil {
		return 0, err
	}
	m.log.Info("active scan chose channel", "channel", ch)
	return ch, nil
}

func (m *Manager) setInitialSettings(ctx context.Context, channel byte) error {
	// CmdInitialSettings request: [mode 1B][sleep 1B][ch 1B][tx 1B]
	// mode 0x05 = Dual (required for B-route)
	data := []byte{0x05, 0x00, channel, 0x00}
	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	defer cancel()
	_, err := m.drv.Request(cctx, bp35c2.CmdInitialSettings, data)
	return err
}

func (m *Manager) setBRouteAuth(ctx context.Context) error {
	// CmdBRouteAuthSet: [id 32B ASCII HEX][pass 12B ASCII]
	if len(m.cfg.BRouteID) != 32 {
		return fmt.Errorf("BRouteID must be 32 chars (got %d)", len(m.cfg.BRouteID))
	}
	if len(m.cfg.BRoutePassword) != 12 {
		return fmt.Errorf("BRoutePassword must be 12 chars (got %d)", len(m.cfg.BRoutePassword))
	}
	data := make([]byte, 0, 44)
	data = append(data, []byte(m.cfg.BRouteID)...)
	data = append(data, []byte(m.cfg.BRoutePassword)...)
	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	defer cancel()
	_, err := m.drv.Request(cctx, bp35c2.CmdBRouteAuthSet, data)
	return err
}

// activeScan sends an active-scan request and gathers per-channel
// results delivered as 0x4051 notifications. Returns the first channel
// that reported "found" (byte 0x00 in the notification payload).
func (m *Manager) activeScan(ctx context.Context) (byte, error) {
	// CmdActiveScan: [scanTime 1B][chMask 4B][idSet 1B=0x01][pairingID 8B]
	pairing := []byte(m.cfg.BRouteID[len(m.cfg.BRouteID)-8:])
	data := make([]byte, 0, 1+4+1+8)
	data = append(data, m.cfg.ScanTimeExp)
	data = binary.BigEndian.AppendUint32(data, m.cfg.ChannelMask)
	data = append(data, 0x01)
	data = append(data, pairing...)

	// Response 0x2051 is the request acknowledgement; per-channel
	// results come as 0x4051. Scan time upper bound = 14 channels *
	// 9.64ms * 2^S. Add margin.
	scanDur := time.Duration(14) * time.Duration(9.64e6*(1<<m.cfg.ScanTimeExp))
	if scanDur < 5*time.Second {
		scanDur = 5 * time.Second
	}
	scanDur += 3 * time.Second

	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	defer cancel()
	if _, err := m.drv.Request(cctx, bp35c2.CmdActiveScan, data); err != nil {
		return 0, fmt.Errorf("CmdActiveScan: %w", err)
	}

	deadline := time.Now().Add(scanDur)
	for time.Now().Before(deadline) {
		nctx, ncancel := context.WithDeadline(ctx, deadline)
		n, err := m.drv.WaitNotification(nctx, bp35c2.NotifyActiveScanCh)
		ncancel()
		if err != nil {
			return 0, fmt.Errorf("waiting for scan result: %w", err)
		}
		ch, found, err := parseActiveScanResult(n.Data)
		if err != nil {
			m.log.Warn("bad active-scan notification", "err", err)
			continue
		}
		if found {
			return ch, nil
		}
	}
	return 0, errors.New("no meter responded to active scan")
}

// parseActiveScanResult extracts the channel byte and match flag from
// a 0x4051 notification. Layout (per UART spec §4):
//
//	[scanResult 1B][channel 1B][channelPage 1B]...
//
// scanResult: 0x00 = response received on this channel, 0x01 = no
// response.
func parseActiveScanResult(data []byte) (byte, bool, error) {
	if len(data) < 2 {
		return 0, false, fmt.Errorf("payload too short (%d)", len(data))
	}
	return data[1], data[0] == 0x00, nil
}

func (m *Manager) startBRoute(ctx context.Context, channel byte) (*Session, error) {
	// CmdBRouteStart takes no request payload.
	// Response 0x2053: [respResult 1B][channel 1B][panID 2B][macAddr 8B][rssi 1B]
	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	defer cancel()
	resp, err := m.drv.Request(cctx, bp35c2.CmdBRouteStart, nil)
	if err != nil {
		return nil, fmt.Errorf("CmdBRouteStart: %w", err)
	}
	if len(resp.Data) < 1 {
		return nil, fmt.Errorf("bad 0x2053 payload (len %d)", len(resp.Data))
	}
	if resp.Data[0] != 0x01 {
		return nil, fmt.Errorf("CmdBRouteStart returned respResult=%#x", resp.Data[0])
	}
	if len(resp.Data) < 13 {
		return nil, fmt.Errorf("bad 0x2053 payload (len %d)", len(resp.Data))
	}
	ch := resp.Data[1]
	pan := binary.BigEndian.Uint16(resp.Data[2:4])
	mac, err := ParseMac(resp.Data[4:12])
	if err != nil {
		return nil, err
	}
	rssi := int8(resp.Data[12])
	_ = channel // spec says 0x2053 always echoes ch; trust the response
	return &Session{
		MeterIP:  MacToLinkLocalIPv6(mac),
		PANID:    pan,
		Channel:  ch,
		MeterMAC: mac,
		RSSI:     rssi,
	}, nil
}

func (m *Manager) openUDPPort(ctx context.Context) error {
	data := []byte{0x0E, 0x1A} // 3610
	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	defer cancel()
	_, err := m.drv.Request(cctx, bp35c2.CmdUDPPortOpen, data)
	return err
}

func (m *Manager) doPANA(ctx context.Context) error {
	// Install PANA result channel so dispatchNotifications knows to
	// forward the async result here instead of treating it as an
	// auto-re-auth event.
	panaCh := make(chan bp35c2.Notification, 1)
	m.panaMu.Lock()
	m.panaResultCh = panaCh
	m.panaMu.Unlock()
	defer func() {
		m.panaMu.Lock()
		m.panaResultCh = nil
		m.panaMu.Unlock()
	}()

	cctx, cancel := context.WithTimeout(ctx, m.cfg.CommandTimeout)
	if _, err := m.drv.Request(cctx, bp35c2.CmdBRoutePANAStart, nil); err != nil {
		cancel()
		return fmt.Errorf("CmdBRoutePANAStart: %w", err)
	}
	cancel()

	waitCtx, waitCancel := context.WithTimeout(ctx, m.cfg.PANAAuthTimeout)
	defer waitCancel()
	select {
	case n := <-panaCh:
		if len(n.Data) < 1 {
			return fmt.Errorf("bad PANA result notification (len %d)", len(n.Data))
		}
		if n.Data[0] != 0x01 {
			return fmt.Errorf("PANA authentication failed (result=%#x)", n.Data[0])
		}
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("PANA authentication timed out after %s", m.cfg.PANAAuthTimeout)
	}
}

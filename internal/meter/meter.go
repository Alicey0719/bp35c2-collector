package meter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Alicey0719/bp35c2-collector/internal/broute"
	"github.com/Alicey0719/bp35c2-collector/internal/echonet"
)

// Client is a high-level view of one smart meter reachable via mgr.
// It owns TID allocation and correlates ECHONET Lite responses to
// pending Get calls.
type Client struct {
	mgr   *broute.Manager
	log   *slog.Logger

	// tid holds the next TID to allocate.
	tid atomic.Uint32

	// pending maps outstanding TIDs to a response channel.
	pendMu  sync.Mutex
	pending map[uint16]chan *echonet.Frame

	// GetTimeout defaults to 5s.
	GetTimeout time.Duration
}

// NewClient constructs a Client. The caller must invoke Run to start
// the incoming-frame dispatcher.
func NewClient(mgr *broute.Manager, log *slog.Logger) *Client {
	if log == nil {
		log = slog.Default()
	}
	c := &Client{
		mgr:        mgr,
		log:        log.With("component", "meter"),
		pending:    make(map[uint16]chan *echonet.Frame),
		GetTimeout: 5 * time.Second,
	}
	c.tid.Store(1)
	return c
}

// Run consumes incoming ECHONET Lite payloads until ctx expires. It
// must be invoked as a goroutine before any Get call.
func (c *Client) Run(ctx context.Context) error {
	incoming := c.mgr.Incoming()
	for {
		select {
		case in, ok := <-incoming:
			if !ok {
				return nil
			}
			f, err := echonet.Decode(in.Payload)
			if err != nil {
				c.log.Warn("failed to decode ECHONET Lite frame", "err", err)
				continue
			}
			c.pendMu.Lock()
			ch, has := c.pending[f.TID]
			if has {
				delete(c.pending, f.TID)
			}
			c.pendMu.Unlock()
			if !has {
				c.log.Debug("received frame with no pending request", "tid", f.TID, "esv", fmt.Sprintf("%#x", f.ESV))
				continue
			}
			ch <- &f
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Get sends a Get request for the given EPCs to the smart meter and
// waits for the response. On SNA (Get_SNA) it returns the frame anyway
// so the caller can distinguish per-EPC failures.
func (c *Client) Get(ctx context.Context, epcs ...byte) (*echonet.Frame, error) {
	if len(epcs) == 0 {
		return nil, errors.New("meter: no EPCs requested")
	}
	sess, err := c.mgr.WaitConnected(ctx)
	if err != nil {
		return nil, fmt.Errorf("meter: not connected: %w", err)
	}

	tid := uint16(c.tid.Add(1))
	if tid == 0 { // never use 0 — reserved for "no correlation"
		tid = uint16(c.tid.Add(1))
	}
	req := echonet.NewGetRequest(tid, echonet.EOJSmartMeter, epcs...)
	payload := echonet.Encode(req)

	respCh := make(chan *echonet.Frame, 1)
	c.pendMu.Lock()
	c.pending[tid] = respCh
	c.pendMu.Unlock()
	defer func() {
		c.pendMu.Lock()
		delete(c.pending, tid)
		c.pendMu.Unlock()
	}()

	sendCtx, cancel := context.WithTimeout(ctx, c.GetTimeout)
	defer cancel()
	if err := sess.SendUDP(sendCtx, payload); err != nil {
		return nil, fmt.Errorf("meter: send: %w", err)
	}

	waitCtx, waitCancel := context.WithTimeout(ctx, c.GetTimeout)
	defer waitCancel()
	select {
	case f := <-respCh:
		return f, nil
	case <-waitCtx.Done():
		return nil, fmt.Errorf("meter: response timeout for TID %d: %w", tid, waitCtx.Err())
	}
}

// SetC sends a SetC request (requires response). Returns the response
// frame so caller can verify Set_Res vs SetC_SNA.
func (c *Client) SetC(ctx context.Context, epc byte, edt []byte) (*echonet.Frame, error) {
	sess, err := c.mgr.WaitConnected(ctx)
	if err != nil {
		return nil, fmt.Errorf("meter: not connected: %w", err)
	}
	tid := uint16(c.tid.Add(1))
	if tid == 0 {
		tid = uint16(c.tid.Add(1))
	}
	req := echonet.NewSetCRequest(tid, echonet.EOJSmartMeter, epc, edt)
	payload := echonet.Encode(req)

	respCh := make(chan *echonet.Frame, 1)
	c.pendMu.Lock()
	c.pending[tid] = respCh
	c.pendMu.Unlock()
	defer func() {
		c.pendMu.Lock()
		delete(c.pending, tid)
		c.pendMu.Unlock()
	}()

	sendCtx, cancel := context.WithTimeout(ctx, c.GetTimeout)
	defer cancel()
	if err := sess.SendUDP(sendCtx, payload); err != nil {
		return nil, err
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, c.GetTimeout)
	defer waitCancel()
	select {
	case f := <-respCh:
		return f, nil
	case <-waitCtx.Done():
		return nil, waitCtx.Err()
	}
}

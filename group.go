package srt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
	"github.com/datarhei/gosrt/rand"
)

// ErrGroupClosed is returned when a write to a group is attempted after the
// group has been closed or all its links have failed.
var ErrGroupClosed = errors.New("srt: group closed")

// GroupType is the type of a bonding group.
type GroupType = packet.GroupType

const (
	// GroupTypeBroadcast sends all data over all links of the group.
	GroupTypeBroadcast = packet.GroupTypeBroadcast

	// GroupTypeBackup keeps all links but one idle and switches to an
	// idle link when the active link becomes unstable.
	GroupTypeBackup = packet.GroupTypeBackup
)

// GroupLink describes a link of a bonding group as returned by Links().
type GroupLink struct {
	// RemoteAddr is the address of the peer of the link.
	RemoteAddr string

	// Weight is the weight of the link as given to Connect. Links with a
	// higher weight are preferred in backup mode.
	Weight uint16

	// State is one of "idle", "running", "unstable", or "broken". In
	// broadcast mode all links are "running".
	State string
}

// GroupLinkState is the state of a link of a backup group.
type GroupLinkState int

const (
	// GroupLinkIdle means that the link is connected but not sending data.
	GroupLinkIdle GroupLinkState = iota

	// GroupLinkRunning means that the link is actively sending data.
	GroupLinkRunning

	// GroupLinkUnstable means that the link has not responded for the
	// stability timeout and is sending data while a replacement is found.
	GroupLinkUnstable

	// GroupLinkBroken means that the link has been closed.
	GroupLinkBroken
)

func (s GroupLinkState) String() string {
	switch s {
	case GroupLinkIdle:
		return "idle"
	case GroupLinkRunning:
		return "running"
	case GroupLinkUnstable:
		return "unstable"
	case GroupLinkBroken:
		return "broken"
	}

	return "unknown"
}

// groupLink is a single link of a bonding group.
type groupLink struct {
	conn   *srtConn
	weight uint16

	// backup state
	state        GroupLinkState
	lastResponse time.Time
	stableSince  time.Time
	acked        circular.Number // highest sequence number acknowledged by the peer

	closed bool
}

// Group is a SRT bonding group. It implements the Conn interface. Data is
// read from and written to the group as if it were a single connection. The
// group distributes the data over its links according to the group type and
// de-duplicates the data received over multiple links.
type Group struct {
	id     uint32 // group ID, with the SRTGROUP_MASK bit set
	gt     GroupType
	peerId uint32 // group ID of the peer, set when the first link is connected

	config Config

	isn   circular.Number // shared initial sequence number of all links
	start time.Time       // shared time base of all links

	// sending
	nextSequenceNumber circular.Number

	// receiving
	recvQueue    chan packet.Packet
	readQueue    chan packet.Packet
	recvExpected circular.Number // next sequence number to deliver
	parked       map[uint32]packet.Packet

	// send buffer, only used by backup groups
	buffer []packet.Packet

	links []*groupLink

	shutdownOnce sync.Once
	closed       bool

	lock sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	logger Logger

	readBuffer bytes.Buffer
}

// NewGroup creates a new bonding group of the given type with the given
// configuration. The configuration is used for all links of the group. The
// links are added with Connect. The group implements the Conn interface and
// can be used like any other connection.
func NewGroup(gt GroupType, config Config) (*Group, error) {
	if gt != GroupTypeBroadcast && gt != GroupTypeBackup {
		return nil, fmt.Errorf("invalid group type: %d", gt)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	if config.Logger == nil {
		config.Logger = NewLogger(nil)
	}

	id, err := rand.Uint32()
	if err != nil {
		return nil, fmt.Errorf("could not generate group id")
	}
	id = id&^packet.SRTGROUP_MASK | packet.SRTGROUP_MASK

	isn, err := rand.Uint32()
	if err != nil {
		return nil, fmt.Errorf("could not generate initial sequence number")
	}

	g := &Group{
		id:                 id,
		gt:                 gt,
		config:             config,
		isn:                circular.New(isn&packet.MAX_SEQUENCENUMBER, packet.MAX_SEQUENCENUMBER),
		start:              time.Now(),
		nextSequenceNumber: circular.New(isn&packet.MAX_SEQUENCENUMBER, packet.MAX_SEQUENCENUMBER),
		recvExpected:       circular.New(isn&packet.MAX_SEQUENCENUMBER, packet.MAX_SEQUENCENUMBER),
		recvQueue:          make(chan packet.Packet, 2048),
		readQueue:          make(chan packet.Packet, 1024),
		parked:             make(map[uint32]packet.Packet),
		logger:             config.Logger,
	}

	g.ctx, g.cancel = context.WithCancel(context.Background())

	go g.recvState()

	if gt == GroupTypeBackup {
		go g.backupState()
	}

	return g, nil
}

// Connect connects a new link to the address using the SRT protocol with the
// group configuration and adds it to the group. The weight is used in backup
// mode to prefer links with a higher weight. Connect can be called multiple
// times to add more links.
func (g *Group) Connect(network, address string, weight uint16) error {
	conn, err := dialGroup(network, address, g.config, g, weight)
	if err != nil {
		return err
	}

	_ = conn

	return nil
}

// Links returns the links of the group.
func (g *Group) Links() []GroupLink {
	g.lock.RLock()
	defer g.lock.RUnlock()

	links := make([]GroupLink, 0, len(g.links))

	for _, l := range g.links {
		links = append(links, GroupLink{
			RemoteAddr: l.conn.RemoteAddr().String(),
			Weight:     l.weight,
			State:      l.state.String(),
		})
	}

	return links
}

// deliver is the deliverTo hook of the links. It is called without holding
// any lock and must not block.
func (g *Group) deliver(p packet.Packet) {
	select {
	case g.recvQueue <- p:
	default:
		g.log("group:recv:error", func() string { return "receive queue is full, dropping packet" })
		p.Decommission()
	}
}

// linkByConn returns the link with the given connection.
func (g *Group) linkByConn(c *srtConn) *groupLink {
	for _, l := range g.links {
		if l.conn == c {
			return l
		}
	}

	return nil
}

// linkResponded is the onResponse hook of the links. It is called on every
// packet reception and is used to track the stability of the link.
func (g *Group) linkResponded(c *srtConn) {
	g.lock.Lock()
	defer g.lock.Unlock()

	l := g.linkByConn(c)
	if l == nil || l.closed {
		return
	}

	now := time.Now()

	l.lastResponse = now

	if l.state == GroupLinkUnstable && l.stableSince.IsZero() {
		l.stableSince = now
	}
}

// linkAcked is the onACK hook of the links. It is called with the sequence
// number up to which the peer has acknowledged data and is used to trim the
// send buffer.
func (g *Group) linkAcked(c *srtConn, seq circular.Number) {
	g.lock.Lock()
	defer g.lock.Unlock()

	l := g.linkByConn(c)
	if l == nil || l.closed {
		return
	}

	if l.acked.Lt(seq) {
		l.acked = seq

		if g.gt == GroupTypeBackup {
			g.trimBufferLocked()
		}
	}
}

// addLink adds a connected link to the group. It syncs the receive position
// of the link to the position of the group and makes the link the active
// link if it is the first one.
func (g *Group) addLink(conn *srtConn, weight uint16) error {
	g.lock.Lock()
	defer g.lock.Unlock()

	if g.closed {
		return ErrGroupClosed
	}

	conn.syncReceiver(g.recvExpected)

	now := time.Now()

	l := &groupLink{
		conn:         conn,
		weight:       weight,
		lastResponse: now,
		stableSince:  now,
		acked:        g.recvExpected.Dec(),
	}

	if g.gt == GroupTypeBroadcast {
		l.state = GroupLinkRunning
	} else {
		// The first link of a backup group becomes the active link.
		hasActive := false
		for _, o := range g.links {
			if o.state == GroupLinkRunning || o.state == GroupLinkUnstable {
				hasActive = true
				break
			}
		}

		if !hasActive {
			l.state = GroupLinkRunning
		} else {
			l.state = GroupLinkIdle
		}
	}

	g.links = append(g.links, l)

	g.log("group:link", func() string { return fmt.Sprintf("added link %s (weight %d)", conn.RemoteAddr(), weight) })

	return nil
}

// setPeerId remembers the group id of the peer. It is taken from the first
// link that is connected.
func (g *Group) setPeerId(id uint32) {
	g.lock.Lock()
	defer g.lock.Unlock()

	if g.peerId == 0 {
		g.peerId = id
	}
}

// linkClosed removes a closed link from the group. It is called from the
// onShutdown hook of the links.
func (g *Group) linkClosed(c *srtConn) {
	g.lock.Lock()
	defer g.lock.Unlock()

	l := g.linkByConn(c)
	if l == nil || l.closed {
		return
	}

	l.closed = true
	l.state = GroupLinkBroken

	g.log("group:link", func() string { return fmt.Sprintf("link %s closed", c.RemoteAddr()) })
}

// recvState reads the packets delivered by the links, de-duplicates them and
// writes them in order to the read queue.
func (g *Group) recvState() {
	for {
		select {
		case <-g.ctx.Done():
			return
		case p := <-g.recvQueue:
			seq := p.Header().PacketSequenceNumber

			g.lock.Lock()

			if seq.Lt(g.recvExpected) {
				// Duplicate or belated, already delivered by another link.
				g.lock.Unlock()
				g.log("group:recv:drop", func() string { return fmt.Sprintf("dropped duplicate packet %d", seq.Val()) })
				p.Decommission()
				continue
			}

			if seq.Gt(g.recvExpected) {
				// A gap. The per-link receivers deliver in order, so this
				// is only a defensive measure. Park the packet and wait for
				// the gap to be filled.
				if len(g.parked) >= 1024 {
					g.log("group:recv:error", func() string { return "parked packet queue is full, dropping packet" })
					g.lock.Unlock()
					p.Decommission()
					continue
				}

				if _, exists := g.parked[seq.Val()]; exists {
					// A duplicate of a packet that is already parked on
					// this side of the gap. Drop it instead of replacing
					// the parked packet, which would leak its memory.
					g.lock.Unlock()
					g.log("group:recv:drop", func() string { return fmt.Sprintf("dropped duplicate packet %d", seq.Val()) })
					p.Decommission()
					continue
				}

				g.parked[seq.Val()] = p
				g.lock.Unlock()
				continue
			}

			// In order. Deliver the packet and everything that was parked
			// right behind it.
			g.recvExpected = seq.Inc()
			g.lock.Unlock()

			g.pushReadQueue(p)

			for {
				g.lock.Lock()

				parked, ok := g.parked[g.recvExpected.Val()]
				if !ok {
					g.lock.Unlock()
					break
				}

				delete(g.parked, g.recvExpected.Val())
				g.recvExpected = g.recvExpected.Inc()
				g.lock.Unlock()

				g.pushReadQueue(parked)
			}
		}
	}
}

// pushReadQueue pushes a packet to the read queue. It must be called without
// holding the group lock. It returns false and decommissions the packet if
// the queue is full.
func (g *Group) pushReadQueue(p packet.Packet) bool {
	select {
	case g.readQueue <- p:
		return true
	default:
		g.log("group:recv:error", func() string { return "read queue is full, dropping packet" })
		p.Decommission()
		return false
	}
}

// backupState runs the backup state machine. It checks the stability of the
// links and activates replacements.
func (g *Group) backupState() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case now := <-ticker.C:
			g.evaluateLinks(now)
		}
	}
}

// stabilityTimeout returns the stability timeout after which a link is
// considered unstable.
func (g *Group) stabilityTimeout() time.Duration {
	if g.config.GroupStabilityTimeout > 0 {
		return g.config.GroupStabilityTimeout
	}

	return 50 * time.Millisecond
}

// evaluateLinks runs one iteration of the backup state machine.
func (g *Group) evaluateLinks(now time.Time) {
	g.lock.Lock()
	defer g.lock.Unlock()

	g.evaluateLinksLocked(now)
}

// evaluateLinksLocked runs one iteration of the backup state machine. The
// caller must hold the group lock.
func (g *Group) evaluateLinksLocked(now time.Time) {
	timeout := g.stabilityTimeout()

	// Mark running links as unstable when they have not responded for the
	// stability timeout.
	for _, l := range g.links {
		if l.closed {
			continue
		}

		if l.state == GroupLinkRunning && now.Sub(l.lastResponse) > timeout {
			l.state = GroupLinkUnstable
			l.stableSince = time.Time{}

			g.log("group:backup", func() string { return fmt.Sprintf("link %s is unstable", l.conn.RemoteAddr()) })
		}
	}

	// Activate the best idle link when no link is running.
	if !g.hasRunningLocked() {
		if l := g.bestIdleLocked(); l != nil {
			g.activateLocked(l)
		}
	}

	// Recover unstable links that have responded continuously for the
	// stability timeout.
	for _, l := range g.links {
		if l.closed || l.state != GroupLinkUnstable {
			continue
		}

		if now.Sub(l.lastResponse) > timeout {
			// Unstable again, reset the recovery clock.
			l.stableSince = time.Time{}
			continue
		}

		if l.stableSince.IsZero() {
			continue
		}

		if now.Sub(l.stableSince) < timeout {
			continue
		}

		// The link has recovered. It takes over if it is at least as good
		// as the current running link.
		if running := g.runningLocked(); running != nil && running.weight > l.weight {
			l.state = GroupLinkIdle
			g.log("group:backup", func() string { return fmt.Sprintf("link %s recovered, staying idle", l.conn.RemoteAddr()) })
			continue
		}

		g.activateLocked(l)
	}
}

func (g *Group) hasRunningLocked() bool {
	for _, l := range g.links {
		if l.state == GroupLinkRunning {
			return true
		}
	}

	return false
}

func (g *Group) runningLocked() *groupLink {
	for _, l := range g.links {
		if l.state == GroupLinkRunning {
			return l
		}
	}

	return nil
}

// bestIdleLocked returns the idle link with the highest weight.
func (g *Group) bestIdleLocked() *groupLink {
	var best *groupLink

	for _, l := range g.links {
		if l.closed || l.state != GroupLinkIdle {
			continue
		}

		if best == nil || l.weight > best.weight {
			best = l
		}
	}

	return best
}

// activateLocked makes the link the running link, syncs its receive position
// to the position of the group and resends the unacknowledged window.
func (g *Group) activateLocked(l *groupLink) {
	if l.closed || l.state == GroupLinkRunning || l.state == GroupLinkBroken {
		return
	}

	// Only one link can be running at a time. Unstable links keep sending
	// until they have recovered.
	if running := g.runningLocked(); running != nil && running != l {
		running.state = GroupLinkIdle
	}

	l.state = GroupLinkRunning
	l.lastResponse = time.Now()
	l.stableSince = time.Now()
	l.acked = g.recvExpected.Dec()

	g.log("group:backup", func() string { return fmt.Sprintf("link %s is now running", l.conn.RemoteAddr()) })

	// Sync the receive position of the link to the position of the group.
	l.conn.syncReceiver(g.recvExpected)

	// Resend the unacknowledged window.
	g.resendLocked(l)
}

// resendLocked pushes the unacknowledged window to the link. The packets
// keep their original sequence numbers and timestamps so that the peer
// delivers them immediately.
func (g *Group) resendLocked(l *groupLink) {
	for _, p := range g.buffer {
		select {
		case <-l.conn.ctx.Done():
			return
		case l.conn.writeQueue <- p.Clone():
		default:
			// The queue is full. The peer will ask for the missing packets
			// with a NAK.
			g.log("group:backup", func() string { return "write queue of link is full, dropping resend packet" })
		}
	}
}

// trimBufferLocked drops all packets of the send buffer that have been
// acknowledged by all active links.
func (g *Group) trimBufferLocked() {
	var minAcked circular.Number
	first := true

	for _, l := range g.links {
		if l.closed {
			continue
		}

		if l.state != GroupLinkRunning && l.state != GroupLinkUnstable {
			continue
		}

		if first {
			minAcked = l.acked
			first = false
			continue
		}

		if l.acked.Lt(minAcked) {
			minAcked = l.acked
		}
	}

	if first {
		// No active links, keep the buffer for the next activation.
		return
	}

	for len(g.buffer) != 0 {
		p := g.buffer[0]

		if p.Header().PacketSequenceNumber.Gt(minAcked) {
			break
		}

		g.buffer = g.buffer[1:]
		p.Decommission()
	}
}

// getTimestamp returns the elapsed time since the start of the group in
// microseconds.
func (g *Group) getTimestamp() uint64 {
	return uint64(time.Since(g.start).Microseconds())
}

// activeLinksLocked returns the links that are currently sending data.
func (g *Group) activeLinksLocked() []*groupLink {
	active := make([]*groupLink, 0, len(g.links))

	for _, l := range g.links {
		if l.closed {
			continue
		}

		if g.gt == GroupTypeBroadcast {
			active = append(active, l)
		} else if l.state == GroupLinkRunning || l.state == GroupLinkUnstable {
			active = append(active, l)
		}
	}

	return active
}

// payloadSizeLocked returns the maximum payload size that all links of the
// group can carry. The minimum over all links guarantees that packets fit
// every link, including links that become active only after a failover.
func (g *Group) payloadSizeLocked() int {
	size := int(g.config.PayloadSize)

	for _, l := range g.links {
		if l.closed {
			continue
		}

		if int(l.conn.config.PayloadSize) < size {
			size = int(l.conn.config.PayloadSize)
		}
	}

	return size
}

func (g *Group) WritePacket(p packet.Packet) error {
	if p.Header().IsControlPacket {
		return nil
	}

	_, err := g.Write(p.Data())

	return err
}

func (g *Group) Write(b []byte) (int, error) {
	g.lock.Lock()
	defer g.lock.Unlock()

	if g.closed {
		return 0, ErrGroupClosed
	}

	links := g.activeLinksLocked()

	if len(links) == 0 {
		// Backup mode: try to activate the best idle link.
		if l := g.bestIdleLocked(); l != nil {
			g.activateLocked(l)
			links = g.activeLinksLocked()
		}
	}

	if len(links) == 0 {
		return 0, ErrGroupClosed
	}

	payloadSize := g.payloadSizeLocked()

	wrote := 0

	for len(b) != 0 {
		n := len(b)
		if n > payloadSize {
			n = payloadSize
		}

		p := packet.NewPacket(nil)
		p.SetData(b[:n])

		p.Header().IsControlPacket = false
		p.Header().PktTsbpdTime = g.getTimestamp()
		p.Header().PacketSequenceNumber = g.nextSequenceNumber
		g.nextSequenceNumber = g.nextSequenceNumber.Inc()

		if g.gt == GroupTypeBackup {
			// Keep a copy of the packet in the send buffer for a possible
			// resend after a failover.
			g.buffer = append(g.buffer, p.Clone())
		}

		full := 0

		for _, l := range links {
			select {
			case <-l.conn.ctx.Done():
				full++
			case l.conn.writeQueue <- p.Clone():
			default:
				full++
			}
		}

		p.Decommission()

		if g.gt == GroupTypeBackup {
			// Drop the oldest packets when the send buffer exceeds the flow
			// control window. They would not be delivered in time anyway.
			if limit := int(g.config.FC); limit > 0 && len(g.buffer) > limit {
				drop := g.buffer[:len(g.buffer)-limit]
				g.buffer = g.buffer[len(g.buffer)-limit:]

				for _, dp := range drop {
					dp.Decommission()
				}
			}
		}

		if full == len(links) {
			// All links are saturated, report backpressure.
			return wrote, io.EOF
		}

		wrote += n
		b = b[n:]
	}

	return wrote, nil
}

func (g *Group) ReadPacket() (packet.Packet, error) {
	select {
	case <-g.ctx.Done():
		return nil, io.EOF
	case p := <-g.readQueue:
		return p, nil
	}
}

func (g *Group) Read(b []byte) (int, error) {
	if g.readBuffer.Len() != 0 {
		return g.readBuffer.Read(b)
	}

	g.readBuffer.Reset()

	p, err := g.ReadPacket()
	if err != nil {
		return 0, err
	}

	g.readBuffer.Write(p.Data())

	// The packet is out of the group and can be decommissioned
	p.Decommission()

	return g.readBuffer.Read(b)
}

func (g *Group) Close() error {
	g.shutdownOnce.Do(func() {
		g.lock.Lock()
		g.closed = true
		g.lock.Unlock()

		g.cancel()

		g.lock.RLock()
		links := make([]*groupLink, len(g.links))
		copy(links, g.links)
		g.lock.RUnlock()

		for _, l := range links {
			l.conn.close()
		}
	})

	return nil
}

func (g *Group) LocalAddr() net.Addr {
	g.lock.RLock()
	defer g.lock.RUnlock()

	if len(g.links) == 0 {
		return nil
	}

	return g.links[0].conn.LocalAddr()
}

func (g *Group) RemoteAddr() net.Addr {
	g.lock.RLock()
	defer g.lock.RUnlock()

	if len(g.links) == 0 {
		return nil
	}

	return g.links[0].conn.RemoteAddr()
}

func (g *Group) SetDeadline(t time.Time) error      { return nil }
func (g *Group) SetReadDeadline(t time.Time) error  { return nil }
func (g *Group) SetWriteDeadline(t time.Time) error { return nil }

func (g *Group) SocketId() uint32 {
	return g.id
}

func (g *Group) PeerSocketId() uint32 {
	return g.peerId
}

func (g *Group) StreamId() string {
	return g.config.StreamId
}

func (g *Group) Version() uint32 {
	return 5
}

// Stats returns the statistics of the group. The accumulated statistics are
// the sum of the statistics of all links. The instantaneous statistics are
// those of the first link.
func (g *Group) Stats(s *Statistics) {
	if s == nil {
		return
	}

	g.lock.RLock()
	links := make([]*groupLink, len(g.links))
	copy(links, g.links)
	g.lock.RUnlock()

	now := uint64(time.Since(g.start).Milliseconds())
	previous := s.Accumulated

	var acc StatisticsAccumulated

	for _, l := range links {
		var ls Statistics

		l.conn.Stats(&ls)

		acc = sumStatisticsAccumulated(acc, ls.Accumulated)
	}

	s.Accumulated = acc

	s.Interval = StatisticsInterval{
		MsInterval:        now - s.MsTimeStamp,
		PktSent:           s.Accumulated.PktSent - previous.PktSent,
		PktRecv:           s.Accumulated.PktRecv - previous.PktRecv,
		PktSentUnique:     s.Accumulated.PktSentUnique - previous.PktSentUnique,
		PktRecvUnique:     s.Accumulated.PktRecvUnique - previous.PktRecvUnique,
		PktSendLoss:       s.Accumulated.PktSendLoss - previous.PktSendLoss,
		PktRecvLoss:       s.Accumulated.PktRecvLoss - previous.PktRecvLoss,
		PktRetrans:        s.Accumulated.PktRetrans - previous.PktRetrans,
		PktRecvRetrans:    s.Accumulated.PktRecvRetrans - previous.PktRecvRetrans,
		PktSentACK:        s.Accumulated.PktSentACK - previous.PktSentACK,
		PktRecvACK:        s.Accumulated.PktRecvACK - previous.PktRecvACK,
		PktSentNAK:        s.Accumulated.PktSentNAK - previous.PktSentNAK,
		PktRecvNAK:        s.Accumulated.PktRecvNAK - previous.PktRecvNAK,
		UsSndDuration:     s.Accumulated.UsSndDuration - previous.UsSndDuration,
		PktSndDrop:        s.Accumulated.PktSendDrop - previous.PktSendDrop,
		PktRecvDrop:       s.Accumulated.PktRecvDrop - previous.PktRecvDrop,
		PktRecvUndecrypt:  s.Accumulated.PktRecvUndecrypt - previous.PktRecvUndecrypt,
		ByteSent:          s.Accumulated.ByteSent - previous.ByteSent,
		ByteRecv:          s.Accumulated.ByteRecv - previous.ByteRecv,
		ByteSentUnique:    s.Accumulated.ByteSentUnique - previous.ByteSentUnique,
		ByteRecvUnique:    s.Accumulated.ByteRecvUnique - previous.ByteRecvUnique,
		ByteRecvLoss:      s.Accumulated.ByteRecvLoss - previous.ByteRecvLoss,
		ByteRetrans:       s.Accumulated.ByteRetrans - previous.ByteRetrans,
		ByteRecvRetrans:   s.Accumulated.ByteRecvRetrans - previous.ByteRecvRetrans,
		ByteRecvBelated:   s.Accumulated.ByteRecvBelated - previous.ByteRecvBelated,
		ByteSendDrop:      s.Accumulated.ByteSendDrop - previous.ByteSendDrop,
		ByteRecvDrop:      s.Accumulated.ByteRecvDrop - previous.ByteRecvDrop,
		ByteRecvUndecrypt: s.Accumulated.ByteRecvUndecrypt - previous.ByteRecvUndecrypt,
	}

	if len(links) != 0 {
		var ls Statistics

		links[0].conn.Stats(&ls)

		s.Instantaneous = ls.Instantaneous
	}

	s.MsTimeStamp = now
}

func sumStatisticsAccumulated(a, b StatisticsAccumulated) StatisticsAccumulated {
	a.PktSent += b.PktSent
	a.PktRecv += b.PktRecv
	a.PktSentUnique += b.PktSentUnique
	a.PktRecvUnique += b.PktRecvUnique
	a.PktSendLoss += b.PktSendLoss
	a.PktRecvLoss += b.PktRecvLoss
	a.PktRetrans += b.PktRetrans
	a.PktRecvRetrans += b.PktRecvRetrans
	a.PktSentACK += b.PktSentACK
	a.PktRecvACK += b.PktRecvACK
	a.PktSentNAK += b.PktSentNAK
	a.PktRecvNAK += b.PktRecvNAK
	a.PktSentKM += b.PktSentKM
	a.PktRecvKM += b.PktRecvKM
	a.UsSndDuration += b.UsSndDuration
	a.PktRecvBelated += b.PktRecvBelated
	a.PktSendDrop += b.PktSendDrop
	a.PktRecvDrop += b.PktRecvDrop
	a.PktRecvUndecrypt += b.PktRecvUndecrypt
	a.ByteSent += b.ByteSent
	a.ByteRecv += b.ByteRecv
	a.ByteSentUnique += b.ByteSentUnique
	a.ByteRecvUnique += b.ByteRecvUnique
	a.ByteRecvLoss += b.ByteRecvLoss
	a.ByteRetrans += b.ByteRetrans
	a.ByteRecvRetrans += b.ByteRecvRetrans
	a.ByteRecvBelated += b.ByteRecvBelated
	a.ByteSendDrop += b.ByteSendDrop
	a.ByteRecvDrop += b.ByteRecvDrop
	a.ByteRecvUndecrypt += b.ByteRecvUndecrypt

	return a
}

func (g *Group) log(topic string, message func() string) {
	if g.logger == nil {
		return
	}

	g.logger.Print(topic, 0, 2, message)
}

// Check if we implement the Conn interface
var _ Conn = (*Group)(nil)

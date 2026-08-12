package srt

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/require"
)

// newGroupLinkConn builds a connection without any network socket that acts as
// a link of the given group.
func newGroupLinkConn(t testing.TB, g *Group, weight uint16) *srtConn {
	t.Helper()

	var c *srtConn

	c = newHookTestConn(t, srtConnConfig{
		preserveSequenceNumber: true,
		deliverTo:              g.deliver,
		onResponse:             g.linkResponded,
		onACK: func(seq circular.Number) {
			g.linkAcked(c, seq)
		},
		onShutdown: g.linkClosed,
	})

	return c
}

// collectSent returns an onSend hook that collects the payloads of all sent
// data packets in order.
func collectSent(t testing.TB, ch chan<- packet.Packet) func(p packet.Packet) {
	return func(p packet.Packet) {
		if !p.Header().IsControlPacket {
			select {
			case ch <- p.Clone():
			default:
				t.Fatal("send channel is full")
			}
		}
	}
}

func TestGroupWriteNoLinks(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	_, err = g.Write([]byte("hello"))
	require.ErrorIs(t, err, ErrGroupClosed)
}

func TestGroupBroadcastWrite(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	sent1 := make(chan packet.Packet, 16)
	sent2 := make(chan packet.Packet, 16)

	c1 := newGroupLinkConn(t, g, 1)
	setOnSend(c1, collectSent(t, sent1))

	c2 := newGroupLinkConn(t, g, 1)
	setOnSend(c2, collectSent(t, sent2))

	require.NoError(t, g.addLink(c1, 1))
	require.NoError(t, g.addLink(c2, 1))

	n, err := g.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)

	// Both links must have received the same packet with the group sequence
	// number and the group timestamp.
	for _, ch := range []chan packet.Packet{sent1, sent2} {
		select {
		case p := <-ch:
			require.Equal(t, g.isn, p.Header().PacketSequenceNumber)
			require.Equal(t, []byte("hello"), p.Data())
		case <-time.After(time.Second):
			t.Fatal("packet was not sent on link")
		}
	}
}

func TestGroupBroadcastDedup(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	_, err = g.Write([]byte("x"))
	require.ErrorIs(t, err, ErrGroupClosed)

	c := newGroupLinkConn(t, g, 1)
	require.NoError(t, g.addLink(c, 1))

	deliver := func(offset uint32, data byte) {
		p := packet.NewPacket(nil)
		p.Header().PacketSequenceNumber = g.isn.Add(offset)
		p.SetData([]byte{data})
		g.deliver(p)
	}

	// Deliver packets in order as if they came from the peer over two links.
	deliver(0, 'a')
	deliver(1, 'b')
	deliver(2, 'c')

	// A duplicate of a packet that has already been delivered is dropped.
	deliver(0, 'x')

	// An out-of-order packet is parked until the gap is filled.
	deliver(4, 'z')

	// The missing packet arrives and the parked packet is delivered with it.
	deliver(3, 'd')

	read := func() string {
		p, err := g.ReadPacket()
		require.NoError(t, err)

		data := string(p.Data())
		p.Decommission()

		return data
	}

	require.Equal(t, "a", read())
	require.Equal(t, "b", read())
	require.Equal(t, "c", read())
	require.Equal(t, "d", read())
	require.Equal(t, "z", read())
}

func TestGroupBackupFailover(t *testing.T) {
	config := DefaultConfig()
	config.GroupStabilityTimeout = 100 * time.Millisecond

	g, err := NewGroup(GroupTypeBackup, config)
	require.NoError(t, err)

	defer g.Close()

	sent1 := make(chan packet.Packet, 64)
	sent2 := make(chan packet.Packet, 64)

	c1 := newGroupLinkConn(t, g, 1)
	setOnSend(c1, collectSent(t, sent1))

	c2 := newGroupLinkConn(t, g, 2)
	setOnSend(c2, collectSent(t, sent2))

	require.NoError(t, g.addLink(c1, 1))
	require.NoError(t, g.addLink(c2, 2))

	// Only the first link is running.
	require.Equal(t, GroupLinkRunning, g.links[0].state)
	require.Equal(t, GroupLinkIdle, g.links[1].state)

	n, err := g.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)

	// Only the running link must have received the packet.
	select {
	case p := <-sent1:
		require.Equal(t, g.isn, p.Header().PacketSequenceNumber)
	case <-time.After(time.Second):
		t.Fatal("packet was not sent on the running link")
	}

	select {
	case p := <-sent2:
		t.Fatalf("idle link must not send packets (got %q)", p.Data())
	default:
	}

	// The first link fails. The second link must take over. close() calls
	// the onShutdown hook asynchronously, so wait until the link has been
	// marked as closed.
	c1.close()

	require.Eventually(t, func() bool {
		g.lock.RLock()
		defer g.lock.RUnlock()

		return g.links[0].closed
	}, time.Second, time.Millisecond)

	g.lock.Lock()
	g.evaluateLinksLocked(time.Now())
	g.lock.Unlock()

	g.lock.RLock()
	state0, state1 := g.links[0].state, g.links[1].state
	g.lock.RUnlock()

	require.Equal(t, GroupLinkBroken, state0)
	require.Equal(t, GroupLinkRunning, state1)

	// The unacknowledged window must have been resent on the new link with
	// the original sequence number.
	select {
	case p := <-sent2:
		require.Equal(t, g.isn, p.Header().PacketSequenceNumber)
		require.Equal(t, []byte("hello"), p.Data())
	case <-time.After(time.Second):
		t.Fatal("unacknowledged window was not resent on the new link")
	}

	n, err = g.Write([]byte("world"))
	require.NoError(t, err)
	require.Equal(t, 5, n)

	select {
	case p := <-sent2:
		require.Equal(t, g.isn.Add(1), p.Header().PacketSequenceNumber)
		require.Equal(t, []byte("world"), p.Data())
	case <-time.After(time.Second):
		t.Fatal("packet was not sent on the new running link")
	}
}

func TestGroupBackupStability(t *testing.T) {
	config := DefaultConfig()
	config.GroupStabilityTimeout = 100 * time.Millisecond

	g, err := NewGroup(GroupTypeBackup, config)
	require.NoError(t, err)

	defer g.Close()

	c1 := newGroupLinkConn(t, g, 1)
	c2 := newGroupLinkConn(t, g, 1)

	require.NoError(t, g.addLink(c1, 1))
	require.NoError(t, g.addLink(c2, 1))

	respond := func(idx int) {
		g.lock.RLock()
		c := g.links[idx].conn
		g.lock.RUnlock()

		g.linkResponded(c)
	}

	stop := make(chan struct{})
	respondAll := make(chan struct{})

	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				// The second link keeps responding and stays running.
				respond(1)

				// The first link only responds once it is unstable.
				select {
				case <-respondAll:
					respond(0)
				default:
				}
			}
		}
	}()

	defer close(stop)

	// The first link has not responded for longer than the stability
	// timeout. It becomes unstable and the second link takes over.
	g.lock.Lock()
	g.links[0].lastResponse = time.Now().Add(-time.Second)
	g.lock.Unlock()

	require.Eventually(t, func() bool {
		g.lock.RLock()
		defer g.lock.RUnlock()

		return g.links[0].state == GroupLinkUnstable && g.links[1].state == GroupLinkRunning
	}, time.Second, 5*time.Millisecond)

	// The first link starts responding again and recovers after the
	// stability timeout. Both links have the same weight, the recovered
	// link takes over from the second link again.
	close(respondAll)

	require.Eventually(t, func() bool {
		g.lock.RLock()
		defer g.lock.RUnlock()

		return g.links[0].state == GroupLinkRunning && g.links[1].state == GroupLinkIdle
	}, time.Second, 5*time.Millisecond)
}

func TestGroupBackupBufferTrim(t *testing.T) {
	g, err := NewGroup(GroupTypeBackup, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	c := newGroupLinkConn(t, g, 1)
	require.NoError(t, g.addLink(c, 1))

	for i := 0; i < 5; i++ {
		_, err := g.Write([]byte("hello"))
		require.NoError(t, err)
	}

	require.Len(t, g.buffer, 5)

	// The peer acknowledges up to the third packet.
	g.linkAcked(c, g.isn.Add(2))

	require.Len(t, g.buffer, 2)

	require.Equal(t, g.isn.Add(3), g.buffer[0].Header().PacketSequenceNumber)
	require.Equal(t, g.isn.Add(4), g.buffer[1].Header().PacketSequenceNumber)
}

func TestGroupReadAfterClose(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	require.NoError(t, g.Close())

	buffer := make([]byte, 16)

	_, err = g.Read(buffer)
	require.ErrorIs(t, err, io.EOF)

	_, err = g.Write([]byte("hello"))
	require.ErrorIs(t, err, ErrGroupClosed)
}

func TestGroupLinkStates(t *testing.T) {
	g, err := NewGroup(GroupTypeBackup, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	c1 := newGroupLinkConn(t, g, 1)
	c2 := newGroupLinkConn(t, g, 2)

	require.NoError(t, g.addLink(c1, 1))
	require.NoError(t, g.addLink(c2, 2))

	links := g.Links()
	require.Len(t, links, 2)
	require.Equal(t, "running", links[0].State)
	require.Equal(t, "idle", links[1].State)
	require.Equal(t, uint16(2), links[1].Weight)
}

func TestGroupSocketIds(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	require.NotZero(t, g.SocketId()&packet.SRTGROUP_MASK)
	require.Equal(t, uint32(0), g.PeerSocketId())

	g.setPeerId(0x1234 | packet.SRTGROUP_MASK)
	require.Equal(t, uint32(0x1234|packet.SRTGROUP_MASK), g.PeerSocketId())
}

func TestGroupNewGroupError(t *testing.T) {
	// An invalid configuration must be rejected.
	config := DefaultConfig()
	config.PayloadSize = 0

	_, err := NewGroup(GroupTypeBroadcast, config)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid config")

	// An invalid group type must be rejected.
	_, err = NewGroup(packet.GroupTypeUndefined, DefaultConfig())
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid group type")

	// The valid group types are accepted.
	_, err = NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	_, err = NewGroup(GroupTypeBackup, DefaultConfig())
	require.NoError(t, err)
}

func TestGroupConnectErrors(t *testing.T) {
	// A failed connection attempt must propagate the error and must not
	// leave a partial link in the group.
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	addr := udp.LocalAddr().String()
	udp.Close()

	err = g.Connect("srt", addr, 1)
	require.Error(t, err)
	require.Len(t, g.Links(), 0)

	// Connecting a closed group must fail with ErrGroupClosed and must not
	// add a partial link, even if the connection attempt itself succeeds.
	ln, _ := startGroupListener(t, GroupTypeBroadcast, DefaultConfig())
	defer ln.Close()

	g2, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	require.NoError(t, g2.Close())

	err = g2.Connect("srt", ln.Addr().String(), 1)
	require.ErrorIs(t, err, ErrGroupClosed)
	require.Len(t, g2.Links(), 0)
}

func TestGroupBackupRecoveredLowerWeightStaysIdle(t *testing.T) {
	config := DefaultConfig()
	config.GroupStabilityTimeout = 100 * time.Millisecond

	g, err := NewGroup(GroupTypeBackup, config)
	require.NoError(t, err)

	defer g.Close()

	sent1 := make(chan packet.Packet, 8)
	sent2 := make(chan packet.Packet, 8)

	c1 := newGroupLinkConn(t, g, 1)
	setOnSend(c1, collectSent(t, sent1))

	c2 := newGroupLinkConn(t, g, 2)
	setOnSend(c2, collectSent(t, sent2))

	require.NoError(t, g.addLink(c1, 1))
	require.NoError(t, g.addLink(c2, 2))

	respond := func(idx int) {
		g.lock.RLock()
		c := g.links[idx].conn
		g.lock.RUnlock()

		g.linkResponded(c)
	}

	stop := make(chan struct{})
	respondAll := make(chan struct{})

	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				// The second link keeps responding and becomes the running
				// link once the first one goes unstable.
				respond(1)

				select {
				case <-respondAll:
					respond(0)
				default:
				}
			}
		}
	}()

	defer close(stop)

	// The first link stops responding. It becomes unstable and the second
	// link (with the higher weight) takes over.
	g.lock.Lock()
	g.links[0].lastResponse = time.Now().Add(-time.Second)
	g.lock.Unlock()

	require.Eventually(t, func() bool {
		g.lock.RLock()
		defer g.lock.RUnlock()

		return g.links[0].state == GroupLinkUnstable && g.links[1].state == GroupLinkRunning
	}, time.Second, 5*time.Millisecond)

	// The first link responds again and recovers. Since it has a lower
	// weight than the running link, it must be demoted to idle and must
	// not take over again.
	close(respondAll)

	require.Eventually(t, func() bool {
		g.lock.RLock()
		defer g.lock.RUnlock()

		return g.links[0].state == GroupLinkIdle
	}, time.Second, 5*time.Millisecond)

	g.lock.RLock()
	state0, state1 := g.links[0].state, g.links[1].state
	g.lock.RUnlock()

	require.Equal(t, GroupLinkIdle, state0)
	require.Equal(t, GroupLinkRunning, state1)

	// The data continues to be sent only over the running link.
	_, err = g.Write([]byte("hello"))
	require.NoError(t, err)

	select {
	case p := <-sent2:
		require.Equal(t, []byte("hello"), p.Data())
	case <-time.After(time.Second):
		t.Fatal("packet was not sent on the running link")
	}

	select {
	case p := <-sent1:
		t.Fatalf("idle link must not send packets (got %q)", p.Data())
	default:
	}
}

func TestGroupStatsAccumulated(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	c1 := newGroupLinkConn(t, g, 1)
	c2 := newGroupLinkConn(t, g, 1)

	require.NoError(t, g.addLink(c1, 1))
	require.NoError(t, g.addLink(c2, 1))

	c1.statisticsLock.Lock()
	c1.statistics.pktSentACK = 5
	c1.statistics.pktRecvNAK = 2
	c1.statistics.pktSentKM = 1
	c1.statisticsLock.Unlock()

	c2.statisticsLock.Lock()
	c2.statistics.pktSentACK = 3
	c2.statistics.pktRecvNAK = 7
	c2.statistics.pktRecvKM = 4
	c2.statisticsLock.Unlock()

	var s Statistics

	g.Stats(&s)

	// The accumulated statistics are the sum over all links.
	require.Equal(t, uint64(8), s.Accumulated.PktSentACK)
	require.Equal(t, uint64(9), s.Accumulated.PktRecvNAK)
	require.Equal(t, uint64(1), s.Accumulated.PktSentKM)
	require.Equal(t, uint64(4), s.Accumulated.PktRecvKM)
}

func TestGroupParkedDuplicate(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	c := newGroupLinkConn(t, g, 1)
	require.NoError(t, g.addLink(c, 1))

	deliver := func(offset uint32, data byte) {
		p := packet.NewPacket(nil)
		p.Header().PacketSequenceNumber = g.isn.Add(offset)
		p.SetData([]byte{data})
		g.deliver(p)
	}

	deliver(0, 'a')
	deliver(1, 'b')

	// A packet behind a gap is parked.
	deliver(3, 'z')

	// A duplicate of the parked packet must be dropped and must not replace
	// the parked packet.
	deliver(3, 'x')

	// The gap is filled and the original parked packet is delivered.
	deliver(2, 'c')

	read := func() string {
		p, err := g.ReadPacket()
		require.NoError(t, err)

		data := string(p.Data())
		p.Decommission()

		return data
	}

	require.Equal(t, "a", read())
	require.Equal(t, "b", read())
	require.Equal(t, "c", read())
	require.Equal(t, "z", read())

	// Nothing else must be delivered, in particular not the duplicate.
	select {
	case p := <-g.readQueue:
		t.Fatalf("received unexpected packet %q", p.Data())
	case <-time.After(200 * time.Millisecond):
	}
}

func TestGroupPushReadQueueFull(t *testing.T) {
	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	first := packet.NewPacket(nil)
	first.SetData([]byte("first"))

	require.True(t, g.pushReadQueue(first))

	// Fill the read queue.
	for range 1023 {
		p := packet.NewPacket(nil)
		p.SetData([]byte("x"))

		require.True(t, g.pushReadQueue(p))
	}

	// The queue is full: the packet must be rejected immediately and must
	// not be delivered.
	p := packet.NewPacket(nil)
	p.SetData([]byte("overflow"))

	require.False(t, g.pushReadQueue(p))
	require.Len(t, g.readQueue, 1024)

	// Draining the queue makes room for new packets.
	<-g.readQueue

	require.True(t, g.pushReadQueue(packet.NewPacket(nil)))
}

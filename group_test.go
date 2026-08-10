package srt

import (
	"io"
	"testing"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/require"
)

// newGroupLinkConn builds a connection without any network socket that acts as
// a link of the given group.
func newGroupLinkConn(t *testing.T, g *Group, weight uint16) *srtConn {
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
func collectSent(t *testing.T, ch chan<- packet.Packet) func(p packet.Packet) {
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
	c1.onSend = collectSent(t, sent1)

	c2 := newGroupLinkConn(t, g, 1)
	c2.onSend = collectSent(t, sent2)

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
	c1.onSend = collectSent(t, sent1)

	c2 := newGroupLinkConn(t, g, 2)
	c2.onSend = collectSent(t, sent2)

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

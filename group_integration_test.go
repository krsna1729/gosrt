package srt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startGroupListener starts a listener that accepts a bonding group of the
// given type and returns the mirror group of the listener.
func startGroupListener(t *testing.T, gt GroupType, config Config) (*listener, chan Conn) {
	t.Helper()

	config.GroupConnect = true

	ln, err := Listen("srt", "127.0.0.1:0", config)
	require.NoError(t, err)

	groups := make(chan Conn, 4)

	go func() {
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}

			conn, err := req.Accept()
			if err != nil {
				continue
			}

			select {
			case groups <- conn:
			default:
			}
		}
	}()

	return ln.(*listener), groups
}

// readPackets collects the payloads of all packets read from the group.
func readPackets(t *testing.T, c Conn) <-chan string {
	t.Helper()

	ch := make(chan string, 64)

	go func() {
		for {
			p, err := c.ReadPacket()
			if err != nil {
				close(ch)
				return
			}

			data := string(p.Data())
			p.Decommission()

			ch <- data
		}
	}()

	return ch
}

func nextPacket(t *testing.T, ch <-chan string) string {
	t.Helper()

	select {
	case data, ok := <-ch:
		require.True(t, ok, "connection was closed unexpectedly")
		return data
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for packet")
		return ""
	}
}

func TestGroupListenReject(t *testing.T) {
	ln, err := Listen("srt", "127.0.0.1:0", DefaultConfig())
	require.NoError(t, err)

	defer ln.Close()

	go func() {
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}

			req.Reject(REJ_GROUP)
		}
	}()

	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	err = g.Connect("srt", ln.Addr().String(), 1)
	require.Error(t, err)
	require.ErrorContains(t, err, "REJECT (GROUP)")
}

func TestGroupBroadcastIntegration(t *testing.T) {
	ln, groups := startGroupListener(t, GroupTypeBroadcast, DefaultConfig())
	defer ln.Close()

	g, err := NewGroup(GroupTypeBroadcast, DefaultConfig())
	require.NoError(t, err)

	defer g.Close()

	// Connect two links to the listener. The listener accepts the first
	// link and automatically accepts the second one.
	require.NoError(t, g.Connect("srt", ln.Addr().String(), 1))
	require.NoError(t, g.Connect("srt", ln.Addr().String(), 1))

	mirror := <-groups

	received := readPackets(t, mirror)

	// Data written to the group is sent over both links and must be
	// received exactly once.
	for range 10 {
		_, err := g.Write([]byte("hello world"))
		require.NoError(t, err)

		require.Equal(t, "hello world", nextPacket(t, received))
	}

	// The reverse direction works as well.
	sent := readPackets(t, g)

	_, err = mirror.Write([]byte("back at you"))
	require.NoError(t, err)

	require.Equal(t, "back at you", nextPacket(t, sent))
}

func TestGroupBackupFailoverIntegration(t *testing.T) {
	config := DefaultConfig()
	config.GroupStabilityTimeout = 100 * time.Millisecond

	ln, groups := startGroupListener(t, GroupTypeBackup, config)
	defer ln.Close()

	g, err := NewGroup(GroupTypeBackup, config)
	require.NoError(t, err)

	defer g.Close()

	require.NoError(t, g.Connect("srt", ln.Addr().String(), 1))
	require.NoError(t, g.Connect("srt", ln.Addr().String(), 2))

	mirror := <-groups

	received := readPackets(t, mirror)

	// Only the first link is active and sends the data.
	_, err = g.Write([]byte("one"))
	require.NoError(t, err)

	require.Equal(t, "one", nextPacket(t, received))

	// The first link fails. The second link takes over and resends the
	// unacknowledged window. The peer must not receive duplicates.
	g.lock.Lock()
	link0 := g.links[0]
	g.lock.Unlock()

	link0.conn.close()

	require.Eventually(t, func() bool {
		g.lock.RLock()
		defer g.lock.RUnlock()

		return g.links[0].closed && g.links[0].state == GroupLinkBroken
	}, time.Second, time.Millisecond)

	require.Eventually(t, func() bool {
		g.lock.RLock()
		defer g.lock.RUnlock()

		return g.links[1].state == GroupLinkRunning
	}, time.Second, time.Millisecond)

	_, err = g.Write([]byte("two"))
	require.NoError(t, err)

	require.Equal(t, "two", nextPacket(t, received))

	// The unacknowledged window of the first link must have been resent on
	// the second link. The peer has to drop the duplicates.
	g.lock.RLock()
	state0, state1 := g.links[0].state, g.links[1].state
	g.lock.RUnlock()

	require.Equal(t, GroupLinkBroken, state0)
	require.Equal(t, GroupLinkRunning, state1)

	_, err = g.Write([]byte("three"))
	require.NoError(t, err)

	require.Equal(t, "three", nextPacket(t, received))

	// Ensure that the packets were delivered exactly once and in order.
	select {
	case data := <-received:
		t.Fatalf("received unexpected duplicate packet %q", data)
	case <-time.After(500 * time.Millisecond):
	}

}

package srt

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/crypto"
	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/assert"
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

// TestGroupEncrypted verifies that a bonding group encrypts the data of all
// its links and that the mirror group decrypts and de-duplicates it.
func TestGroupEncrypted(t *testing.T) {
	passphrase := "foobarfoobar"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()
	config.EnforcedEncryption = true
	config.GroupConnect = true

	server := Server{
		Addr:   "127.0.0.1:0",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					return REJECT
				}
			}

			switch req.StreamId() {
			case "publish":
				return PUBLISH
			case "subscribe":
				return SUBSCRIBE
			}

			return REJECT
		},
		HandlePublish: func(conn Conn) {
			channel.Publish(conn)

			conn.Close()
		},
		HandleSubscribe: func(conn Conn) {
			channel.Subscribe(conn)

			conn.Close()
		},
	}

	err := server.Listen()
	require.NoError(t, err)

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	// A group with the wrong passphrase must not connect.
	wrong := DefaultConfig()
	wrong.StreamId = "publish"
	wrong.Passphrase = "barfoobarfoo"

	bad, err := NewGroup(GroupTypeBackup, wrong)
	require.NoError(t, err)

	defer bad.Close()

	err = bad.Connect("srt", server.ln.Addr().String(), 1)
	require.Error(t, err)

	// The encrypted group connects both links and delivers the data. In
	// broadcast mode the same encrypted packet arrives over both links and
	// the mirror group must deliver it only once.
	gconfig := DefaultConfig()
	gconfig.StreamId = "publish"
	gconfig.Passphrase = passphrase

	g, err := NewGroup(GroupTypeBroadcast, gconfig)
	require.NoError(t, err)

	defer g.Close()

	require.NoError(t, g.Connect("srt", server.ln.Addr().String(), 1))
	require.NoError(t, g.Connect("srt", server.ln.Addr().String(), 2))

	readerConnected := make(chan struct{})
	received := make(chan string, 16)

	go func() {
		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = passphrase

		conn, err := Dial("srt", server.ln.Addr().String(), config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				received <- string(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		conn.Close()
	}()

	<-readerConnected

	message := "Hello Group!"

	_, err = g.Write([]byte(message))
	require.NoError(t, err)

	select {
	case data := <-received:
		require.Equal(t, message, data)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the encrypted message")
	}

	g.Close()

	// Wait for the subscriber connection to wind down.
	select {
	case <-received:
	case <-time.After(5 * time.Second):
	}
}

// TestGroupAutoAcceptWrongPassphrase sends a forged second link of an already
// accepted group with a wrong passphrase. The listener must reject it with
// REJ_BADSECRET and must not touch the mirror group.
func TestGroupAutoAcceptWrongPassphrase(t *testing.T) {
	passphrase := "correctsecret"

	config := DefaultConfig()
	config.EnforcedEncryption = true
	config.GroupConnect = true

	ln, err := Listen("srt", "127.0.0.1:0", config)
	require.NoError(t, err)

	defer ln.Close()

	groups := make(chan Conn, 1)

	go func() {
		for {
			req, err := ln.Accept2()
			if err != nil {
				return
			}

			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					req.Reject(REJ_BADSECRET)
					continue
				}
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

	// Connect the first link with the correct passphrase. The listener
	// creates the mirror group for it.
	gconfig := DefaultConfig()
	gconfig.Passphrase = passphrase

	g, err := NewGroup(GroupTypeBackup, gconfig)
	require.NoError(t, err)

	defer g.Close()

	require.NoError(t, g.Connect("srt", ln.Addr().String(), 1))

	mirror := <-groups

	groupID := mirror.PeerSocketId()
	require.NotZero(t, groupID&packet.SRTGROUP_MASK)

	// Send handshakes from a raw UDP socket and return the parsed response.
	ua, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)

	defer ua.Close()

	write := func(cif *packet.CIFHandshake) *packet.CIFHandshake {
		p := packet.NewPacket(ua.LocalAddr())
		p.Header().IsControlPacket = true
		p.Header().ControlType = packet.CTRLTYPE_HANDSHAKE
		p.Header().DestinationSocketId = 0

		require.NoError(t, p.MarshalCIF(cif))

		var buf bytes.Buffer

		require.NoError(t, p.Marshal(&buf))

		_, err := ua.WriteTo(buf.Bytes(), ln.Addr())
		require.NoError(t, err)

		ua.SetReadDeadline(time.Now().Add(2 * time.Second))

		raw := make([]byte, 2048)

		n, err := ua.Read(raw)
		require.NoError(t, err)

		rp, err := packet.NewPacketFromData(ua.LocalAddr(), raw[:n])
		require.NoError(t, err)

		rcif := &packet.CIFHandshake{}

		require.NoError(t, rp.UnmarshalCIF(rcif))

		return rcif
	}

	// Induction. The listener answers with a SYN cookie.
	rcif := write(&packet.CIFHandshake{
		Version:       5,
		HandshakeType: packet.HSTYPE_INDUCTION,
		SRTSocketId:   0x12345678,
	})

	require.NotZero(t, rcif.SynCookie)

	// Conclusion of the forged second link: the same group id as the
	// accepted group, but a key material derived from a wrong passphrase.
	km := &packet.CIFKeyMaterialExtension{}

	cr, err := crypto.New(16)
	require.NoError(t, err)

	require.NoError(t, cr.MarshalKM(km, "wrongsecret", packet.EvenKeyEncrypted))

	rcif = write(&packet.CIFHandshake{
		Version:                     5,
		HandshakeType:               packet.HSTYPE_CONCLUSION,
		InitialPacketSequenceNumber: circular.New(1, packet.MAX_SEQUENCENUMBER),
		MaxTransmissionUnitSize:     1500,
		MaxFlowWindowSize:           8192,
		SRTSocketId:                 0x12345678,
		SynCookie:                   rcif.SynCookie,
		HasHS:                       true,
		SRTHS: &packet.CIFHandshakeExtension{
			SRTVersion: SRT_VERSION,
			SRTFlags: packet.CIFHandshakeExtensionFlags{
				TSBPDSND:    true,
				TSBPDRCV:    true,
				TLPKTDROP:   true,
				PERIODICNAK: true,
				REXMITFLG:   true,
			},
			RecvTSBPDDelay: 120,
			SendTSBPDDelay: 120,
		},
		HasGroup: true,
		SRTGroup: &packet.CIFGroupExtension{
			GroupId:    groupID,
			GroupType:  GroupTypeBackup,
			LinkWeight: 1,
		},
		HasKM: true,
		SRTKM: km,
	})

	require.Equal(t, packet.HandshakeType(REJ_BADSECRET), rcif.HandshakeType)

	// The mirror group must not have gained a link.
	mirrorGroup, ok := mirror.(*Group)
	require.True(t, ok)

	require.Eventually(t, func() bool {
		mirrorGroup.lock.RLock()
		defer mirrorGroup.lock.RUnlock()

		return len(mirrorGroup.links) == 1
	}, time.Second, 10*time.Millisecond)
}

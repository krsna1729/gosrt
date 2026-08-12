package srt

import (
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryption(t *testing.T) {
	message := "Hello World!"
	passphrase := "foobarfoobar"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()
	config.EnforcedEncryption = true

	server := Server{
		Addr:   "127.0.0.1:0",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					return REJECT
				}
			}

			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
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
	addr := server.ln.Addr().String()

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	{
		// Reject connection if wrong password is set
		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "barfoobarfoo"

		_, err := Dial("srt", addr, config)
		require.Error(t, err)
	}
	// Test transmitting an encrypted message

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	dataReader1 := bytes.Buffer{}

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		err = conn.Close()
		require.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		n, err := conn.Write([]byte(message))
		if !assert.NoError(t, err) {
			panic(err.Error())
		}
		assert.Equal(t, 12, n)

		time.Sleep(3 * time.Second)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, message, reader1)
}

// Test for https://github.com/datarhei/gosrt/pull/94
func TestEncryptionRetransmit(t *testing.T) {
	message := "Hello World!"
	passphrase := "foobarfoobar"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()
	config.EnforcedEncryption = true

	server := Server{
		Addr:   "127.0.0.1:0",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					return REJECT
				}
			}

			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
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
	addr := server.ln.Addr().String()

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	{
		// Reject connection if wrong password is set
		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "barfoobarfoo"

		_, err := Dial("srt", addr, config)
		require.Error(t, err)
	}

	// Test transmitting an encrypted message

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	dataReader1 := bytes.Buffer{}

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		err = conn.Close()
		require.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		counter := 0

		dialer, _ := conn.(*dialer)
		dialer.conn.onSendLock.Lock()
		originalOnSend := dialer.conn.onSend
		dialer.conn.onSend = func(p packet.Packet) {
			if !p.Header().IsControlPacket {
				// Drop every 2nd original packet
				if !p.Header().RetransmittedPacketFlag {
					counter++
					if counter%2 == 0 {
						return
					}
				}
			}

			originalOnSend(p)
		}
		dialer.conn.onSendLock.Unlock()

		for range 5 {
			n, err := conn.Write([]byte(message))
			if !assert.NoError(t, err) {
				panic(err.Error())
			}
			assert.Equal(t, 12, n)
		}

		time.Sleep(3 * time.Second)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, message+message+message+message+message, reader1)
}

func TestEncryptionKeySwap(t *testing.T) {
	message := "Hello World!"
	passphrase := "foobarfoobar"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()
	config.EnforcedEncryption = true

	server := Server{
		Addr:   "127.0.0.1:0",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			if req.IsEncrypted() {
				if err := req.SetPassphrase(passphrase); err != nil {
					return REJECT
				}
			}

			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
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
	addr := server.ln.Addr().String()

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	// Test transmitting encrypted messages with key swap in between

	dataReader1 := bytes.Buffer{}

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"
		config.Passphrase = "foobarfoobar"

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		buffer := make([]byte, 2048)

		close(readerConnected)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"
		config.Passphrase = "foobarfoobar"
		// Swap encryption key after 50 sent messages
		config.KMPreAnnounce = 10
		config.KMRefreshRate = 30

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		// Send 150 messages
		for range 150 {
			n, err := conn.Write([]byte(message))
			if !assert.NoError(t, err) {
				panic(err.Error())
			}
			assert.Equal(t, 12, n)
		}

		time.Sleep(3 * time.Second)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, strings.Repeat(message, 150), reader1)
}

func TestStats(t *testing.T) {
	message := "Hello World!"
	channel := NewPubSub(PubSubConfig{})

	config := DefaultConfig()

	server := Server{
		Addr:   "127.0.0.1:0",
		Config: &config,
		HandleConnect: func(req ConnRequest) ConnType {
			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
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
	addr := server.ln.Addr().String()

	defer server.Shutdown()

	go func() {
		err := server.Serve()
		if err == ErrServerClosed {
			return
		}
		require.NoError(t, err)
	}()

	statsReader := Statistics{}
	statsWriter := Statistics{}

	readerConnected := make(chan struct{})
	readerDone := make(chan struct{})

	dataReader1 := bytes.Buffer{}

	go func() {
		defer close(readerDone)

		config := DefaultConfig()
		config.StreamId = "subscribe"

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		close(readerConnected)

		buffer := make([]byte, 2048)

		for {
			n, err := conn.Read(buffer)
			if n != 0 {
				dataReader1.Write(buffer[:n])
			}

			if err != nil {
				break
			}
		}

		conn.Stats(&statsReader)

		err = conn.Close()
		require.NoError(t, err)
	}()

	<-readerConnected

	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)

		config := DefaultConfig()
		config.StreamId = "publish"

		conn, err := Dial("srt", addr, config)
		if !assert.NoError(t, err) {
			panic(err.Error())
		}

		n, err := conn.Write([]byte(message))
		if !assert.NoError(t, err) {
			panic(err.Error())
		}
		assert.Equal(t, 12, n)

		time.Sleep(3 * time.Second)

		conn.Stats(&statsWriter)

		err = conn.Close()
		assert.NoError(t, err)
	}()

	<-writerDone
	<-readerDone

	reader1 := dataReader1.String()

	require.Equal(t, message, reader1)

	require.Equal(t, uint64(len(message)+44), statsReader.Accumulated.ByteRecv)
	require.Equal(t, uint64(1), statsReader.Accumulated.PktRecv)

	require.Equal(t, uint64(len(message)+44), statsWriter.Accumulated.ByteSent)
	require.Equal(t, uint64(1), statsWriter.Accumulated.PktSent)
}

// newHookTestConn builds a connection without any network socket in order to
// test the internal hooks. The default onSend hook swallows all packets.
func newHookTestConn(t testing.TB, config srtConnConfig) *srtConn {
	t.Helper()

	if config.config.PayloadSize == 0 {
		config.config.PayloadSize = MAX_PAYLOAD_SIZE
	}

	if config.config.PeerIdleTimeout == 0 {
		config.config.PeerIdleTimeout = time.Minute
	}

	if config.logger == nil {
		config.logger = NewLogger(nil)
	}

	if config.localAddr == nil {
		addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:7000")
		require.NoError(t, err)
		config.localAddr = addr
	}

	if config.remoteAddr == nil {
		addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:7001")
		require.NoError(t, err)
		config.remoteAddr = addr
	}

	if config.initialPacketSequenceNumber.Val() == 0 {
		config.initialPacketSequenceNumber = circular.New(1, packet.MAX_SEQUENCENUMBER)
	}

	if config.onSend == nil {
		config.onSend = func(p packet.Packet) {}
	}

	c := newSRTConn(config)

	t.Cleanup(c.close)

	return c
}

// setOnSend replaces the onSend hook of a running test connection. The
// connection's sender goroutine reads onSend under the onSend lock (see
// srtConn.pop), so direct field assignment would race with it.
func setOnSend(c *srtConn, onSend func(packet.Packet)) {
	c.onSendLock.Lock()
	c.onSend = onSend
	c.onSendLock.Unlock()
}

func TestDeliverToHook(t *testing.T) {
	delivered := make(chan packet.Packet, 1)

	c := newHookTestConn(t, srtConnConfig{
		deliverTo: func(p packet.Packet) {
			delivered <- p
		},
	})

	p := packet.NewPacket(nil)
	p.SetData([]byte("hello"))

	c.deliver(p)

	select {
	case dp := <-delivered:
		require.Equal(t, []byte("hello"), dp.Data())
	case <-time.After(time.Second):
		t.Fatal("packet was not handed to the deliverTo hook")
	}

	// The packet must not end up in the read queue
	select {
	case <-c.readQueue:
		t.Fatal("packet ended up in the read queue despite a deliverTo hook")
	default:
	}
}

func TestOnResponseHook(t *testing.T) {
	responses := make(chan struct{}, 1)

	c := newHookTestConn(t, srtConnConfig{
		onResponse: func(*srtConn) {
			responses <- struct{}{}
		},
	})

	p := packet.NewPacket(nil)
	p.Header().IsControlPacket = true
	p.Header().ControlType = packet.CTRLTYPE_KEEPALIVE
	p.Header().DestinationSocketId = c.socketId

	c.handlePacket(p)

	select {
	case <-responses:
	case <-time.After(time.Second):
		t.Fatal("onResponse hook was not called on packet reception")
	}
}

func TestOnACKHook(t *testing.T) {
	acks := make(chan circular.Number, 1)

	c := newHookTestConn(t, srtConnConfig{
		onACK: func(seq circular.Number) {
			acks <- seq
		},
	})

	p := packet.NewPacket(nil)
	p.Header().IsControlPacket = true
	p.Header().ControlType = packet.CTRLTYPE_ACK

	cif := packet.CIFACK{
		LastACKPacketSequenceNumber: circular.New(42, packet.MAX_SEQUENCENUMBER),
		IsLite:                      true,
	}
	p.MarshalCIF(&cif)

	c.handlePacket(p)

	select {
	case seq := <-acks:
		require.Equal(t, uint32(42), seq.Val())
	case <-time.After(time.Second):
		t.Fatal("onACK hook was not called on packet acknowledgement")
	}
}

func TestSyncReceiver(t *testing.T) {
	delivered := make(chan packet.Packet, 2)

	c := newHookTestConn(t, srtConnConfig{
		deliverTo: func(p packet.Packet) {
			delivered <- p
		},
	})

	c.syncReceiver(circular.New(100, packet.MAX_SEQUENCENUMBER))

	// A packet behind the sync position must be dropped
	p := packet.NewPacket(nil)
	p.Header().PacketSequenceNumber = circular.New(99, packet.MAX_SEQUENCENUMBER)
	p.Header().MessageNumber = 1
	p.SetData([]byte("belated"))

	c.handlePacket(p)

	// A packet at the sync position must be delivered
	p = packet.NewPacket(nil)
	p.Header().PacketSequenceNumber = circular.New(100, packet.MAX_SEQUENCENUMBER)
	p.Header().MessageNumber = 1
	p.SetData([]byte("fresh"))

	c.handlePacket(p)

	select {
	case dp := <-delivered:
		require.Equal(t, []byte("fresh"), dp.Data())
	case <-time.After(time.Second):
		t.Fatal("packet at the synced position was not delivered")
	}
}

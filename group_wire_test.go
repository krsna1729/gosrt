package srt

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/datarhei/gosrt/circular"
	"github.com/datarhei/gosrt/packet"
	"github.com/stretchr/testify/require"
)

// groupHandshake performs a manual HSv5 handshake with a group extension and
// returns the CIF of the final response of the listener.
func groupHandshake(t *testing.T, ln Listener, groupId uint32, groupType GroupType, weight uint16) (*packet.CIFHandshake, error) {
	t.Helper()

	conn, err := net.Dial("udp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// send induction request
	p := packet.NewPacket(conn.RemoteAddr())
	p.Header().IsControlPacket = true
	p.Header().ControlType = packet.CTRLTYPE_HANDSHAKE
	p.Header().SubType = 0
	p.Header().TypeSpecific = 0
	p.Header().Timestamp = 0
	p.Header().DestinationSocketId = 0
	sendcif := &packet.CIFHandshake{
		IsRequest:                   true,
		Version:                     4,
		EncryptionField:             0,
		ExtensionField:              2,
		InitialPacketSequenceNumber: circular.New(10000, packet.MAX_SEQUENCENUMBER),
		MaxTransmissionUnitSize:     MAX_MSS_SIZE,
		MaxFlowWindowSize:           25600,
		HandshakeType:               packet.HSTYPE_INDUCTION,
		SRTSocketId:                 55555,
		SynCookie:                   0,
	}
	sendcif.PeerIP.FromNetAddr(conn.LocalAddr())
	p.MarshalCIF(sendcif)
	var buf bytes.Buffer
	err = p.Marshal(&buf)
	require.NoError(t, err)
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)

	// read induction response
	inbuf := make([]byte, MAX_MSS_SIZE)
	n, err := conn.Read(inbuf)
	require.NoError(t, err)
	p, err = packet.NewPacketFromData(conn.RemoteAddr(), inbuf[:n])
	require.NoError(t, err)
	recvcif := &packet.CIFHandshake{}
	err = p.UnmarshalCIF(recvcif)
	require.NoError(t, err)

	// send conclusion with group extension
	p.Header().IsControlPacket = true
	p.Header().ControlType = packet.CTRLTYPE_HANDSHAKE
	p.Header().SubType = 0
	p.Header().TypeSpecific = 0
	p.Header().Timestamp = 0
	p.Header().DestinationSocketId = 0
	sendcif.Version = 5
	sendcif.ExtensionField = recvcif.ExtensionField
	sendcif.HandshakeType = packet.HSTYPE_CONCLUSION
	sendcif.SynCookie = recvcif.SynCookie
	sendcif.HasHS = true
	sendcif.SRTHS = &packet.CIFHandshakeExtension{
		SRTVersion: SRT_VERSION,
		SRTFlags: packet.CIFHandshakeExtensionFlags{
			TSBPDSND:    true,
			TSBPDRCV:    true,
			CRYPT:       true,
			TLPKTDROP:   true,
			PERIODICNAK: true,
			REXMITFLG:   true,
		},
	}
	sendcif.HasGroup = true
	sendcif.SRTGroup = &packet.CIFGroupExtension{
		GroupId:    groupId,
		GroupType:  groupType,
		LinkWeight: weight,
	}
	p.MarshalCIF(sendcif)
	buf.Reset()
	err = p.Marshal(&buf)
	require.NoError(t, err)
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)

	// read the final response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err = conn.Read(inbuf)
	if err != nil {
		return nil, err
	}
	p, err = packet.NewPacketFromData(conn.RemoteAddr(), inbuf[:n])
	require.NoError(t, err)
	recvcif = &packet.CIFHandshake{}
	err = p.UnmarshalCIF(recvcif)
	require.NoError(t, err)

	return recvcif, nil
}

func TestGroupHandshakeWire(t *testing.T) {
	config := DefaultConfig()
	config.GroupConnect = true

	ln, err := Listen("srt", "127.0.0.1:0", config)
	require.NoError(t, err)

	defer ln.Close()

	mirrors := make(chan *Group, 1)

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
			case mirrors <- conn.(*Group):
			default:
			}
		}
	}()

	groupId := uint32(0xDEADBEEF | packet.SRTGROUP_MASK)

	recvcif, err := groupHandshake(t, ln, groupId, GroupTypeBroadcast, 5)
	require.NoError(t, err)

	// The listener accepts the link and responds with the group membership
	// of the mirror group. Each side announces its own group id.
	require.Equal(t, packet.HSTYPE_CONCLUSION, recvcif.HandshakeType)
	require.True(t, recvcif.HasGroup)
	require.NotNil(t, recvcif.SRTGroup)
	require.Equal(t, GroupTypeBroadcast, recvcif.SRTGroup.GroupType)
	require.Equal(t, uint16(5), recvcif.SRTGroup.LinkWeight)
	require.NotEqual(t, 0, recvcif.SRTGroup.GroupId&packet.SRTGROUP_MASK)
	require.NotEqual(t, groupId, recvcif.SRTGroup.GroupId)

	// The mirror group of the listener carries the announced group id.
	mirror := <-mirrors
	require.Equal(t, recvcif.SRTGroup.GroupId, mirror.id)
	require.Equal(t, GroupTypeBroadcast, mirror.gt)
}

func TestGroupHandshakeReject(t *testing.T) {
	testcases := []struct {
		name         string
		groupConnect bool
		groupId      uint32
		groupType    GroupType
		reject       packet.HandshakeType
	}{
		{
			name:      "listener without group connect",
			groupId:   0xDEADBEEF | packet.SRTGROUP_MASK,
			groupType: GroupTypeBroadcast,
			reject:    packet.HandshakeType(REJ_GROUP),
		},
		{
			name:         "invalid group type",
			groupConnect: true,
			groupId:      0xDEADBEEF | packet.SRTGROUP_MASK,
			groupType:    GroupType(3),
			reject:       packet.HandshakeType(REJ_GROUP),
		},
		{
			name:         "invalid group id",
			groupConnect: true,
			groupId:      0x0BADBEEF,
			groupType:    GroupTypeBroadcast,
			reject:       packet.HandshakeType(REJ_ROGUE),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultConfig()
			config.GroupConnect = tc.groupConnect

			ln, err := Listen("srt", "127.0.0.1:0", config)
			require.NoError(t, err)

			defer ln.Close()

			go func() {
				for {
					req, err := ln.Accept2()
					if err != nil {
						return
					}

					req.Reject(REJ_PEER)
				}
			}()

			recvcif, err := groupHandshake(t, ln, tc.groupId, tc.groupType, 1)
			require.NoError(t, err)

			require.Equal(t, tc.reject, recvcif.HandshakeType)
		})
	}
}

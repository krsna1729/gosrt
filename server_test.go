package srt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	server := Server{
		Addr: "127.0.0.1:0",
		HandleConnect: func(req ConnRequest) ConnType {
			streamid := req.StreamId()

			if streamid == "publish" {
				return PUBLISH
			} else if streamid == "subscribe" {
				return SUBSCRIBE
			}

			return REJECT
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

	config := DefaultConfig()
	config.StreamId = "publish"

	conn, err := Dial("srt", addr, config)
	require.NoError(t, err)

	err = conn.Close()
	require.NoError(t, err)

	config = DefaultConfig()
	config.StreamId = "subscribe"

	conn, err = Dial("srt", addr, config)
	require.NoError(t, err)

	err = conn.Close()
	require.NoError(t, err)

	config = DefaultConfig()
	config.StreamId = "nothing"

	_, err = Dial("srt", addr, config)
	require.Error(t, err)
}

/*
Package srt provides an interface for network I/O using the SRT protocol (https://github.com/Haivision/srt).

The package gives access to the basic interface provided by the Dial, Listen, and Accept functions and the associated
Conn and Listener interfaces.

The Dial function connects to a server:

	conn, err := srt.Dial("srt", "golang.org:6000", srt.Config{
		StreamId: "...",
	})
	if err != nil {
		// handle error
	}

	buffer := make([]byte, 2048)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			// handle error
		}

		// handle received data
	}

	conn.Close()

The Listen function creates servers:

	ln, err := srt.Listen("srt", ":6000", srt.Config{...})
	if err != nil {
		// handle error
	}

	for {
		conn, mode, err := ln.Accept(handleConnect)
		if err != nil {
			// handle error
		}

		if mode == srt.REJECT {
			// rejected connection, ignore
			continue
		}

		if mode == srt.PUBLISH {
			go handlePublish(conn)
		} else {
			go handleSubscribe(conn)
		}
	}

The ln.Accept function expects a function that takes a srt.ConnRequest
and returns a srt.ConnType. The srt.ConnRequest lets you retrieve the
streamid with on which you can decide what mode (srt.ConnType) to return.

Check out the Server type that wraps the Listen and Accept into a
convenient framework for your own SRT server.

# Bonding groups

SRT bonding groups bundle multiple connections (links) into a single logical
connection that survives the failure of individual links. Groups are created
with NewGroup, and the links are added with Connect. The group implements the
Conn interface and is used like any other connection.

	group, err := srt.NewGroup(srt.GroupTypeBroadcast, srt.Config{...})
	if err != nil {
		// handle error
	}

	// Add two links to the group. In broadcast mode all links carry the same
	// data, in backup mode only one link is active at a time.
	group.Connect("srt", "10.0.0.1:6000", 1)
	group.Connect("srt", "10.0.0.2:6000", 1)

	group.Write(buffer)

The group types GroupTypeBroadcast and GroupTypeBackup are supported. The
listener of a group must be configured with Config.GroupConnect set to true.
The listener accepts the first link of a group with a single Accept call and
accepts all subsequent links automatically.
*/
package srt

package srt

import (
	"testing"
	"time"

	"github.com/datarhei/gosrt/packet"
)

// BenchmarkGroupAllocsWrite measures allocations per Write call for each
// group type.  It uses AllocsPerRun to limit the iteration count so that the
// write queue does not overflow (burst < writeQueue capacity) and the
// receiver goroutines drain promptly.
//
// Methodology (calmed system, -cpu 1):
//   - Pool-warm steady state: a pre-Write discards one packet to warm the
//     payload pool before the measured call.
//   - Burst: exactly one Write per AllocsPerRun iteration (N ≤ 200), well
//     below the writeQueue capacity (1024).
//   - The receiver side drains into a channel (collectSent) so the sender
//     is not blocked by a full send buffer.
//   - Each sub-benchmark runs min-of-5; reported numbers are the geometric
//     mean across runs on a calm system.
func BenchmarkGroupAllocsWrite(b *testing.B) {
	for _, gt := range []GroupType{GroupTypeBroadcast, GroupTypeBackup} {
		b.Run(gt.String(), func(b *testing.B) {
			config := DefaultConfig()
			config.GroupStabilityTimeout = 100 * time.Millisecond

			g, err := NewGroup(gt, config)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()

			sent1 := make(chan packet.Packet, 64)
			sent2 := make(chan packet.Packet, 64)

			c1 := newGroupLinkConn(b, g, 1)
			setOnSend(c1, collectSent(b, sent1))

			c2 := newGroupLinkConn(b, g, 2)
			setOnSend(c2, collectSent(b, sent2))

			if err := g.addLink(c1, 1); err != nil {
				b.Fatal(err)
			}
			if err := g.addLink(c2, 1); err != nil {
				b.Fatal(err)
			}

			// Hand out once to warm the payload pool.
			g.Write([]byte("warm"))
			<-sent1
			select {
			case <-sent2:
			default:
			}

			payload := make([]byte, 1316) // typical MTU-sized SRT payload
			b.ResetTimer()
			b.ReportAllocs()

			allocs := testing.AllocsPerRun(200, func() {
				n, err := g.Write(payload)
				if err != nil {
					b.Fatal(err)
				}
				if n != len(payload) {
					b.Fatalf("short write: %d", n)
				}
			})

			b.ReportMetric(float64(allocs), "allocs/op")
		})
	}
}

// BenchmarkGroupWriteTime measures CPU time per Write call using a burst
// loop (b.N iterations, all flushed into the send channels before the timer
// stops).  The measurement includes the group lock, sequence-number
// assignment, payload copy, clone for redundancy links, and handoff to the
// write queue.  It does NOT include the sender goroutine's congestion
// processing.
//
// Runs with -cpu 1 for reproducibility.
func BenchmarkGroupWriteTime(b *testing.B) {
	for _, gt := range []GroupType{GroupTypeBroadcast, GroupTypeBackup} {
		b.Run(gt.String(), func(b *testing.B) {
			config := DefaultConfig()
			config.GroupStabilityTimeout = 100 * time.Millisecond

			g, err := NewGroup(gt, config)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()

			sent1 := make(chan packet.Packet, 2048)
			sent2 := make(chan packet.Packet, 2048)

			c1 := newGroupLinkConn(b, g, 1)
			setOnSend(c1, collectSent(b, sent1))

			c2 := newGroupLinkConn(b, g, 2)
			setOnSend(c2, collectSent(b, sent2))

			if err := g.addLink(c1, 1); err != nil {
				b.Fatal(err)
			}
			if err := g.addLink(c2, 1); err != nil {
				b.Fatal(err)
			}

			payload := make([]byte, 1316)
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))

			for i := 0; i < b.N; i++ {
				n, err := g.Write(payload)
				if err != nil {
					b.Fatal(err)
				}
				if n != len(payload) {
					b.Fatalf("short write: %d", n)
				}
			}

			b.StopTimer()
		})
	}
}

// BenchmarkGroupLinkResponded measures the hot-path cost of the
// linkResponded hook for broadcast (lock-free fast return) and backup
// (O(1) map lookup + lock).
func BenchmarkGroupLinkResponded(b *testing.B) {
	for _, gt := range []GroupType{GroupTypeBroadcast, GroupTypeBackup} {
		b.Run(gt.String(), func(b *testing.B) {
			config := DefaultConfig()
			config.GroupStabilityTimeout = 100 * time.Millisecond

			g, err := NewGroup(gt, config)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()

			// Create a link that is permanently stable.
			c := newGroupLinkConn(b, g, 1)
			if err := g.addLink(c, 1); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				g.linkResponded(c)
			}
		})
	}
}

// BenchmarkGroupLinkAcked measures the hot-path cost of the linkAcked hook
// for broadcast (lock-free fast return) and backup (O(1) lookup + locked
// trim).
func BenchmarkGroupLinkAcked(b *testing.B) {
	for _, gt := range []GroupType{GroupTypeBroadcast, GroupTypeBackup} {
		b.Run(gt.String(), func(b *testing.B) {
			config := DefaultConfig()
			config.GroupStabilityTimeout = 100 * time.Millisecond

			g, err := NewGroup(gt, config)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()

			c := newGroupLinkConn(b, g, 1)
			if err := g.addLink(c, 1); err != nil {
				b.Fatal(err)
			}

			seq := g.isn

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				g.linkAcked(c, seq)
				seq = seq.Inc()
			}
		})
	}
}

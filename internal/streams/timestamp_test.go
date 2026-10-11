package streams

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
)

func TestTimestampContinuityAcrossProducerGeneration(t *testing.T) {
	clock := new(timestampContinuity)
	var got []uint32
	track1 := core.NewReceiver(nil, &core.Codec{Name: core.CodecH264})
	track1.Input = clock.wrap(track1.Input)
	sink := &core.Node{Input: func(packet *core.Packet) {
		got = append(got, packet.Timestamp)
	}}
	track1.AppendChild(sink)

	track1.Input(&core.Packet{Header: rtp.Header{Timestamp: 900000}})
	track1.Input(&core.Packet{Header: rtp.Header{Timestamp: 903600}})

	// reconnect() creates a new physical receiver, transfers the existing
	// downstream nodes with Receiver.Replace, and leaves those consumers
	// attached while the new producer's source clock starts over.
	track2 := core.NewReceiver(nil, &core.Codec{Name: core.CodecH264})
	track2.Input = clock.wrap(track2.Input)
	// The old producer remains live while the replacement track is prepared.
	// A final old-generation packet must retain normal cadence rather than
	// being mistaken for the replacement's first packet.
	track1.Input(&core.Packet{Header: rtp.Header{Timestamp: 907200}})
	track1.Replace(track2)
	track2.Input(&core.Packet{Header: rtp.Header{Timestamp: 0}})
	track2.Input(&core.Packet{Header: rtp.Header{Timestamp: 3600}})
	// A stale callback after replacement must not mutate or reach the logical
	// receiver, even though stopping the old physical producer is asynchronous.
	track1.Input(&core.Packet{Header: rtp.Header{Timestamp: 910800}})

	want := []uint32{900000, 903600, 907200, 910800, 914400}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("timestamp[%d]=%d, want %d (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestTimestampContinuityReceiverGraphReplacementRace(t *testing.T) {
	clock := new(timestampContinuity)
	oldTrack := core.NewReceiver(nil, &core.Codec{Name: core.CodecH264})
	oldTrack.Input = clock.wrap(oldTrack.Input)
	newTrack := core.NewReceiver(nil, &core.Codec{Name: core.CodecH264})
	newTrack.Input = clock.wrap(newTrack.Input)

	child := &core.Node{Input: func(*core.Packet) {
		for range 100 {
			runtime.Gosched()
		}
	}}
	oldTrack.AppendChild(child)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			oldTrack.Input(&core.Packet{Header: rtp.Header{Timestamp: 90000}})
			newTrack.Input(&core.Packet{Header: rtp.Header{Timestamp: 90000}})
		}
	}()
	for range 1000 {
		clock.replaceReceiver(oldTrack, newTrack)
		clock.replaceReceiver(newTrack, oldTrack)
	}
	<-done
}

func TestTimestampContinuityPreservesSourceRollover(t *testing.T) {
	clock := new(timestampContinuity)
	generation := clock.allocateGeneration()
	if got, _ := clock.normalize(generation, 0xfffffff0); got != 0xfffffff0 {
		t.Fatalf("initial timestamp=%#x", got)
	}
	if got, _ := clock.normalize(generation, 0x00000020); got != 0x00000020 {
		t.Fatalf("wrapped timestamp=%#x, want natural rollover", got)
	}
}

func TestTimestampContinuityUsesOneTickWithoutObservedCadence(t *testing.T) {
	clock := new(timestampContinuity)
	first := clock.allocateGeneration()
	if got, _ := clock.normalize(first, 1234); got != 1234 {
		t.Fatalf("initial timestamp=%d", got)
	}
	second := clock.allocateGeneration()
	if got, _ := clock.normalize(second, 0); got != 1235 {
		t.Fatalf("replacement timestamp=%d, want 1235", got)
	}
}

func TestTimestampContinuitySerializesReplacementWithInflightForward(t *testing.T) {
	clock := new(timestampContinuity)
	track1 := core.NewReceiver(nil, &core.Codec{Name: core.CodecH264})
	track1.Input = clock.wrap(track1.Input)

	oldEntered := make(chan struct{})
	releaseOld := make(chan struct{})
	delivered := make(chan uint32, 2)
	var once sync.Once
	sink := &core.Node{Input: func(packet *core.Packet) {
		if packet.Payload[0] == 1 {
			once.Do(func() { close(oldEntered) })
			<-releaseOld
		}
		delivered <- packet.Timestamp
	}}
	track1.AppendChild(sink)

	track1.Input(&core.Packet{Header: rtp.Header{Timestamp: 900000}, Payload: []byte{0}})
	<-delivered

	track2 := core.NewReceiver(nil, &core.Codec{Name: core.CodecH264})
	track2.Input = clock.wrap(track2.Input)
	go track1.Input(&core.Packet{Header: rtp.Header{Timestamp: 903600}, Payload: []byte{1}})
	<-oldEntered
	track1.Replace(track2)

	newDone := make(chan struct{})
	go func() {
		track2.Input(&core.Packet{Header: rtp.Header{Timestamp: 0}, Payload: []byte{2}})
		close(newDone)
	}()

	select {
	case got := <-delivered:
		t.Fatalf("replacement overtook in-flight timestamp; delivered %d before old frame", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseOld)

	if got := <-delivered; got != 903600 {
		t.Fatalf("old timestamp=%d, want 903600", got)
	}
	if got := <-delivered; got != 907200 {
		t.Fatalf("replacement timestamp=%d, want 907200", got)
	}
	<-newDone
}

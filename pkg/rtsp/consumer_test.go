package rtsp

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
)

type packetWriterConn struct {
	mu         sync.Mutex
	writes     int
	written    int
	writeN     int
	writeErr   error
	closeCalls int
}

func (c *packetWriterConn) Read([]byte) (int, error) { return 0, net.ErrClosed }
func (c *packetWriterConn) Close() error {
	c.mu.Lock()
	c.closeCalls++
	c.mu.Unlock()
	return nil
}
func (c *packetWriterConn) LocalAddr() net.Addr              { return nil }
func (c *packetWriterConn) RemoteAddr() net.Addr             { return nil }
func (c *packetWriterConn) SetDeadline(time.Time) error      { return nil }
func (c *packetWriterConn) SetReadDeadline(time.Time) error  { return nil }
func (c *packetWriterConn) SetWriteDeadline(time.Time) error { return nil }
func (c *packetWriterConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	n := len(p)
	if c.writeN > 0 && c.writeN < n {
		n = c.writeN
	}
	c.written += n
	return n, nil
}

func (c *packetWriterConn) counts() (writes, written, closes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes, c.written, c.closeCalls
}

func testRTPPacket() *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{
		Version: 2, Marker: true, SequenceNumber: 7, Timestamp: 9000, SSRC: 42,
	}, Payload: []byte{1, 2, 3, 4}}
}

func TestPacketWriterCountsSuccessfulInterleavedWrite(t *testing.T) {
	transport := new(packetWriterConn)
	c := &Conn{conn: transport, state: StatePlay, playOK: true}
	codec := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}

	c.packetWriter(codec, 0, 96)(testRTPPacket())

	const want = intHdr + rtpHdr + 4
	writes, written, closes := transport.counts()
	if sent := c.bytesSent(); sent != want || written != want || writes != 1 {
		t.Fatalf("Send=%d written=%d writes=%d, want %d/1", sent, written, writes, want)
	}
	if closes != 0 {
		t.Fatalf("successful writer closed transport %d times", closes)
	}
}

func TestPacketWriterClosesConsumerAfterWriteFailure(t *testing.T) {
	transport := &packetWriterConn{writeErr: errors.New("injected write failure")}
	c := &Conn{conn: transport, state: StatePlay, playOK: true}
	codec := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}
	handler := c.packetWriter(codec, 0, 96)

	handler(testRTPPacket())
	handler(testRTPPacket())

	if sent := c.bytesSent(); sent != 0 {
		t.Fatalf("failed writes counted as sent bytes: %d", sent)
	}
	writes, _, closes := transport.counts()
	if writes != 1 || closes != 1 {
		t.Fatalf("writes=%d closes=%d, want one failed write and one close", writes, closes)
	}
}

func TestPacketWritersAccountConcurrentAudioAndVideo(t *testing.T) {
	transport := new(packetWriterConn)
	c := &Conn{conn: transport, state: StatePlay, playOK: true}
	video := c.packetWriter(&core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}, 0, 96)
	audio := c.packetWriter(&core.Codec{Name: core.CodecPCMA, ClockRate: 8000, PayloadType: 8}, 2, 8)

	const packets = 250
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for range packets {
			video(testRTPPacket())
		}
	}()
	go func() {
		defer wg.Done()
		for range packets {
			audio(testRTPPacket())
		}
	}()
	go func() {
		defer wg.Done()
		for range packets {
			_, _ = json.Marshal(c)
		}
	}()
	wg.Wait()

	const packetBytes = intHdr + rtpHdr + 4
	if want := packets * 2 * packetBytes; c.bytesSent() != want {
		t.Fatalf("concurrent Send=%d, want %d", c.bytesSent(), want)
	}
}

func TestPacketWriterStopsAfterCapacityFlushFailure(t *testing.T) {
	transport := &packetWriterConn{writeErr: errors.New("injected capacity flush failure")}
	c := &Conn{conn: transport, state: StatePlay, playOK: true}
	codec := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}
	handler := c.packetWriter(codec, 0, 96)

	// Seventeen non-marker packets fill the maximum one-megabyte buffer.
	// The next packet forces the internal capacity flush; its Marker would
	// otherwise make the same handler fall through and attempt a second write.
	for i := 0; i < 18; i++ {
		packet := testRTPPacket()
		packet.Payload = make([]byte, 60_000)
		packet.Marker = i == 17
		handler(packet)
	}

	writes, _, closes := transport.counts()
	if writes != 1 || closes != 1 {
		t.Fatalf("capacity failure writes=%d closes=%d, want one failed write and one close", writes, closes)
	}
}

func TestWriteInterleavedDataRejectsShortWrite(t *testing.T) {
	transport := &packetWriterConn{writeN: 1}
	c := &Conn{conn: transport}
	if err := c.writeInterleavedData([]byte{1, 2}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short-write error=%v, want io.ErrShortWrite", err)
	}
}

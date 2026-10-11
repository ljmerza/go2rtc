package streams

import (
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

// timestampContinuity keeps one logical go2rtc receiver's RTP clock monotonic
// when its physical producer is replaced. Producer-local clocks commonly
// restart near zero; downstream senders remain attached across replacement and
// therefore need a continuous timeline. Sequence continuity remains the
// responsibility of their existing RTP packetizer.
type timestampContinuity struct {
	mu sync.Mutex

	initialized       bool
	nextGeneration    uint64
	activeGeneration  uint64
	lastSource        uint32
	lastOutput        uint32
	lastPositiveDelta uint32
}

func (c *timestampContinuity) allocateGeneration() uint64 {
	c.mu.Lock()
	c.nextGeneration++
	generation := c.nextGeneration
	c.mu.Unlock()
	return generation
}

func (c *timestampContinuity) normalize(generation uint64, source uint32) (uint32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.normalizeLocked(generation, source)
}

func (c *timestampContinuity) normalizeLocked(generation uint64, source uint32) (uint32, bool) {
	if !c.initialized {
		c.initialized = true
		c.activeGeneration = generation
		c.lastSource = source
		c.lastOutput = source
		return source, true
	}

	if generation < c.activeGeneration {
		// A replaced physical producer can finish an in-flight callback after
		// its children have moved. It must not move the logical stream clock.
		return 0, false
	}

	if generation > c.activeGeneration {
		delta := c.lastPositiveDelta
		if delta == 0 {
			delta = 1
		}
		c.lastOutput += delta
		c.lastSource = source
		c.activeGeneration = generation
		return c.lastOutput, true
	}

	delta := source - c.lastSource
	c.lastSource = source
	c.lastOutput += delta
	if delta != 0 && delta < 0x80000000 {
		c.lastPositiveDelta = delta
	}
	return c.lastOutput, true
}

func (c *timestampContinuity) wrap(next core.HandlerFunc) core.HandlerFunc {
	generation := c.allocateGeneration()
	return func(packet *core.Packet) {
		// Serialize normalization and synchronous forwarding together. If an old
		// producer is already forwarding while replacement begins, the new frame
		// cannot overtake it; if the replacement wins first, the old generation is
		// rejected before reaching downstream consumers.
		c.mu.Lock()
		defer c.mu.Unlock()
		timestamp, current := c.normalizeLocked(generation, packet.Timestamp)
		if !current {
			return
		}
		clone := *packet
		clone.Timestamp = timestamp
		next(&clone)
	}
}

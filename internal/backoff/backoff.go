package backoff

import (
	"math"
	"math/rand"
	"time"
)

// Policy defines an exponential backoff strategy with jitter.
type Policy struct {
	Base       time.Duration
	Max        time.Duration
	Multiplier float64
	Jitter     float64
}

// DefaultPolicy returns a sensible default backoff policy.
func DefaultPolicy() Policy {
	return Policy{
		Base:       1 * time.Second,
		Max:        5 * time.Minute,
		Multiplier: 2.0,
		Jitter:     0.25,
	}
}

// Duration computes the backoff duration for the given attempt (0-indexed).
// The delay is Base * Multiplier^attempt, capped at Max, with random jitter
// applied in the range [-Jitter*delay, +Jitter*delay]. The result is always
// at least Base.
func (p Policy) Duration(attempt int) time.Duration {
	delay := float64(p.Base) * math.Pow(p.Multiplier, float64(attempt))
	if delay > float64(p.Max) {
		delay = float64(p.Max)
	}

	if p.Jitter > 0 {
		jitter := delay * p.Jitter * (2*rand.Float64() - 1) // range [-j*d, +j*d]
		delay += jitter
	}

	if delay < float64(p.Base) {
		delay = float64(p.Base)
	}

	return time.Duration(delay)
}

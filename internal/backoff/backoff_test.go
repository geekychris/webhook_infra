package backoff

import (
	"testing"
	"time"
)

func TestFirstAttemptNearBase(t *testing.T) {
	p := DefaultPolicy()
	// Run multiple times to account for jitter.
	for i := 0; i < 100; i++ {
		d := p.Duration(0)
		// Attempt 0: base=1s, jitter=25% => range [750ms, 1250ms]
		if d < 750*time.Millisecond || d > 1250*time.Millisecond {
			t.Fatalf("attempt 0: duration %v out of expected range [750ms, 1250ms]", d)
		}
	}
}

func TestExponentialGrowth(t *testing.T) {
	p := Policy{
		Base:       1 * time.Second,
		Max:        5 * time.Minute,
		Multiplier: 2.0,
		Jitter:     0, // disable jitter for deterministic test
	}

	d0 := p.Duration(0)
	d1 := p.Duration(1)
	d2 := p.Duration(2)

	if d0 != 1*time.Second {
		t.Errorf("attempt 0 = %v, want 1s", d0)
	}
	if d1 != 2*time.Second {
		t.Errorf("attempt 1 = %v, want 2s", d1)
	}
	if d2 != 4*time.Second {
		t.Errorf("attempt 2 = %v, want 4s", d2)
	}
}

func TestCappedAtMax(t *testing.T) {
	p := Policy{
		Base:       1 * time.Second,
		Max:        10 * time.Second,
		Multiplier: 2.0,
		Jitter:     0,
	}

	d := p.Duration(100) // 2^100 seconds >> 10s
	if d != 10*time.Second {
		t.Errorf("attempt 100 = %v, want capped at 10s", d)
	}
}

func TestAlwaysAtLeastBase(t *testing.T) {
	p := DefaultPolicy()
	for i := 0; i < 1000; i++ {
		d := p.Duration(0)
		if d < p.Base {
			t.Fatalf("duration %v is less than base %v", d, p.Base)
		}
	}
}

func TestDefaultPolicy(t *testing.T) {
	p := DefaultPolicy()
	if p.Base != 1*time.Second {
		t.Errorf("Base = %v, want 1s", p.Base)
	}
	if p.Max != 5*time.Minute {
		t.Errorf("Max = %v, want 5m", p.Max)
	}
	if p.Multiplier != 2.0 {
		t.Errorf("Multiplier = %v, want 2.0", p.Multiplier)
	}
	if p.Jitter != 0.25 {
		t.Errorf("Jitter = %v, want 0.25", p.Jitter)
	}
}

func TestCappedWithJitter(t *testing.T) {
	p := Policy{
		Base:       1 * time.Second,
		Max:        10 * time.Second,
		Multiplier: 2.0,
		Jitter:     0.25,
	}

	for i := 0; i < 100; i++ {
		d := p.Duration(100)
		// Max=10s, jitter=25% => range [7.5s, 12.5s] but clamped >= Base
		if d < p.Base {
			t.Fatalf("duration %v is less than base %v", d, p.Base)
		}
		// With max=10s and 25% jitter, the upper bound is 12.5s
		if d > 13*time.Second {
			t.Fatalf("duration %v exceeds reasonable upper bound", d)
		}
	}
}

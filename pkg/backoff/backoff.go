// Package backoff computes retry delays using exponential backoff with full jitter.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy is exponential backoff with "full jitter":
//
//	delay = random(0, min(Cap, Base * 2^(attempt-1)))
//
// The randomness spreads retries out, so many jobs that failed together
// (for example during an outage) do not all retry at the same moment.
type Policy struct {
	Base time.Duration
	Cap  time.Duration
	// Rand returns a random number in [0, n). Nil uses math/rand/v2.
	// Tests replace it to get predictable results.
	Rand func(n int64) int64
}

// Default is the policy from the design: base 2s, cap 5 minutes.
var Default = Policy{Base: 2 * time.Second, Cap: 5 * time.Minute}

// Ceiling returns the largest delay possible for this attempt
// (attempt 1 is the first retry).
func (p Policy) Ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.Base
	for i := 1; i < attempt; i++ {
		// Stop doubling once we reach the cap; this also prevents overflow.
		if d >= p.Cap/2 {
			return p.Cap
		}
		d *= 2
	}
	return min(d, p.Cap)
}

// Delay returns a random delay in [0, Ceiling(attempt)].
func (p Policy) Delay(attempt int) time.Duration {
	ceiling := p.Ceiling(attempt)
	if ceiling <= 0 {
		return 0
	}
	random := p.Rand
	if random == nil {
		random = rand.Int64N
	}
	return time.Duration(random(int64(ceiling) + 1))
}

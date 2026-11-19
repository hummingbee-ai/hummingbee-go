package hummingbee

import (
	"context"
	"time"
)

// SetClock replaces the client's clock, jitter and wait, so retry specs
// neither sleep nor depend on chance. A nil func keeps the real one.
func SetClock(c *Client, now func() time.Time, jitter func(time.Duration) time.Duration, wait func(context.Context, time.Duration) error) {
	if now != nil {
		c.clock.now = now
	}
	if jitter != nil {
		c.clock.jitter = jitter
	}
	if wait != nil {
		c.clock.wait = wait
	}
}

var (
	BackoffStep = backoffStep
	FullJitter  = fullJitter
	Sleep       = sleep
)

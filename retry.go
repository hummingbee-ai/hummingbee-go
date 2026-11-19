package hummingbee

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	backoffBase   = 500 * time.Millisecond
	backoffCap    = 30 * time.Second
	retryAfterCap = 60 * time.Second
)

// clock is everything the retry loop takes from time and chance, so specs can
// replace it and neither sleep nor depend on luck.
type clock struct {
	jitter func(step time.Duration) time.Duration
	now    func() time.Time
	wait   func(ctx context.Context, d time.Duration) error
}

var realClock = clock{jitter: fullJitter, now: time.Now, wait: sleep}

// sendWithRetries posts body until it succeeds, fails in a way a retry cannot
// fix, or runs out of attempts or time. Every attempt sends the same bytes,
// which is safe because the endpoint keeps the first copy of each event id.
func (c *Client) sendWithRetries(ctx context.Context, body []byte, count int) ([]Result, error) {
	for attempt := 1; ; attempt++ {
		results, err := c.attempt(ctx, body, count)
		if err == nil {
			return results, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(err, ctxErr) {
				return nil, err
			}
			return nil, fmt.Errorf("%w, after: %w", ctxErr, err)
		}
		if !isRetryable(err) {
			return nil, err
		}
		if attempt >= c.maxAttempts {
			return nil, gaveUp(attempt, err)
		}
		wait := c.nextWait(attempt, err)
		if deadline, ok := ctx.Deadline(); ok && c.clock.now().Add(wait).After(deadline) {
			return nil, fmt.Errorf("next attempt would pass the context deadline: %w", gaveUp(attempt, err))
		}
		if waitErr := c.clock.wait(ctx, wait); waitErr != nil {
			return nil, fmt.Errorf("%w, after: %w", waitErr, gaveUp(attempt, err))
		}
	}
}

func gaveUp(attempts int, err error) error {
	if attempts == 1 {
		return err
	}
	return fmt.Errorf("gave up after %d attempts: %w", attempts, err)
}

// isRetryable reports whether a later attempt might succeed: a transport
// error, or a status that means the server or a proxy could not answer now.
func isRetryable(err error) bool {
	var permanent *permanentError
	if errors.As(err, &permanent) {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return isRetryableStatus(apiErr.StatusCode)
	}
	return true
}

func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}

// nextWait is the server's Retry-After when it sent one, else full jitter
// over the backoff step.
func (c *Client) nextWait(attempt int, err error) time.Duration {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if wait, ok := parseRetryAfter(apiErr.retryAfter, c.clock.now()); ok {
			return wait
		}
	}
	return c.clock.jitter(backoffStep(attempt))
}

// backoffStep is the most the wait after the given attempt may be: 500 ms
// doubling to 30 s.
func backoffStep(attempt int) time.Duration {
	step := backoffBase
	for i := 1; i < attempt && step < backoffCap; i++ {
		step *= 2
	}
	return min(step, backoffCap)
}

// fullJitter draws a wait uniformly from zero to step, so clients that failed
// together do not retry together.
func fullJitter(step time.Duration) time.Duration {
	if step <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(step) + 1)) //nolint:gosec // G404: spreading retries needs no cryptographic randomness.
}

// parseRetryAfter reads a Retry-After header in seconds or as an HTTP date,
// capped at 60 s. A date in the past means retry now.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int(retryAfterCap/time.Second) {
			return retryAfterCap, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if at, err := http.ParseTime(value); err == nil {
		return min(max(at.Sub(now), 0), retryAfterCap), true
	}
	return 0, false
}

// sleep waits d, or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

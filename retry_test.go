package hummingbee_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/hummingbee-ai/hummingbee-go"
)

// waits records each wait the client asked for instead of sleeping.
type waits struct {
	mu   sync.Mutex
	list []time.Duration
}

func (w *waits) wait(ctx context.Context, d time.Duration) error {
	w.mu.Lock()
	w.list = append(w.list, d)
	w.mu.Unlock()
	return ctx.Err()
}

func (w *waits) List() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.list...)
}

// maxJitter always waits the whole step, so the waits are predictable.
func maxJitter(step time.Duration) time.Duration { return step }

// retryClient is a client whose waits are recorded, never slept.
func retryClient(url string, options ...hummingbee.Option) (*hummingbee.Client, *waits) {
	client := hummingbee.New(token, append([]hummingbee.Option{hummingbee.WithBaseURL(url)}, options...)...)
	recorded := &waits{}
	hummingbee.SetClock(client, nil, maxJitter, recorded.wait)
	return client, recorded
}

// failFirst answers the first times requests with status, then accepts.
func failFirst(times, status int, header ...string) func(http.ResponseWriter, int, []byte) {
	return func(w http.ResponseWriter, attempt int, body []byte) {
		if attempt > times {
			acceptAll(w, attempt, body)
			return
		}
		for i := 0; i+1 < len(header); i += 2 {
			w.Header().Set(header[i], header[i+1])
		}
		writeError(w, status, "SPEC_FAILURE")
	}
}

// refuseFirst fails the first dial as a refused connection, then sends.
type refuseFirst struct {
	mu    sync.Mutex
	calls int
}

func (t *refuseFirst) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.calls++
	first := t.calls == 1
	t.mu.Unlock()
	if first {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	return http.DefaultTransport.RoundTrip(r)
}

func (t *refuseFirst) Calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

var _ = Describe("Send's retries", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("retries a 503 and returns the results, sending the same bytes both times", func() {
		rec := newRecorder(failFirst(1, http.StatusServiceUnavailable))
		client, recorded := retryClient(rec.server.URL)

		results, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(2))

		requests := rec.Requests()
		Expect(requests).To(HaveLen(2))
		Expect(requests[1].RawBody).To(Equal(requests[0].RawBody))
		Expect(recorded.List()).To(Equal([]time.Duration{500 * time.Millisecond}))
	})

	DescribeTable("retries a status that may pass",
		func(status int) {
			rec := newRecorder(failFirst(1, status))
			client, _ := retryClient(rec.server.URL)

			_, err := client.Send(ctx, twoEvents())
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.Requests()).To(HaveLen(2))
		},
		Entry("408", http.StatusRequestTimeout),
		Entry("429", http.StatusTooManyRequests),
		Entry("500", http.StatusInternalServerError),
		Entry("502", http.StatusBadGateway),
		Entry("503", http.StatusServiceUnavailable),
		Entry("504", http.StatusGatewayTimeout),
	)

	DescribeTable("returns at once on a status a retry cannot fix",
		func(status int) {
			rec := newRecorder(failFirst(1, status))
			client, recorded := retryClient(rec.server.URL)

			_, err := client.Send(ctx, twoEvents())
			var apiErr *hummingbee.APIError
			Expect(errors.As(err, &apiErr)).To(BeTrue())
			Expect(apiErr.StatusCode).To(Equal(status))
			Expect(rec.Requests()).To(HaveLen(1))
			Expect(recorded.List()).To(BeEmpty())
		},
		Entry("400", http.StatusBadRequest),
		Entry("401", http.StatusUnauthorized),
		Entry("403", http.StatusForbidden),
		Entry("413", http.StatusRequestEntityTooLarge),
		Entry("415", http.StatusUnsupportedMediaType),
	)

	It("retries a refused connection", func() {
		rec := newRecorder(acceptAll)
		transport := &refuseFirst{}
		client, recorded := retryClient(rec.server.URL, hummingbee.WithHTTPClient(&http.Client{Transport: transport}))

		results, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(2))
		Expect(transport.Calls()).To(Equal(2))
		Expect(rec.Requests()).To(HaveLen(1))
		Expect(recorded.List()).To(HaveLen(1))
	})

	It("waits as long as Retry-After says in seconds", func() {
		rec := newRecorder(failFirst(1, http.StatusTooManyRequests, "Retry-After", "2"))
		client, recorded := retryClient(rec.server.URL)

		_, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.List()).To(Equal([]time.Duration{2 * time.Second}))
	})

	It("waits until the time a Retry-After date names", func() {
		now := time.Date(2026, 11, 17, 9, 30, 0, 0, time.UTC)
		rec := newRecorder(failFirst(1, http.StatusServiceUnavailable,
			"Retry-After", now.Add(3*time.Second).Format(http.TimeFormat)))
		client, recorded := retryClient(rec.server.URL)
		hummingbee.SetClock(client, func() time.Time { return now }, nil, nil)

		_, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.List()).To(Equal([]time.Duration{3 * time.Second}))
	})

	It("retries at once for a Retry-After date in the past", func() {
		now := time.Date(2026, 11, 17, 9, 30, 0, 0, time.UTC)
		rec := newRecorder(failFirst(1, http.StatusServiceUnavailable,
			"Retry-After", now.Add(-time.Minute).Format(http.TimeFormat)))
		client, recorded := retryClient(rec.server.URL)
		hummingbee.SetClock(client, func() time.Time { return now }, nil, nil)

		_, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.List()).To(Equal([]time.Duration{0}))
	})

	It("caps Retry-After at 60 seconds", func() {
		rec := newRecorder(failFirst(1, http.StatusServiceUnavailable, "Retry-After", "3600"))
		client, recorded := retryClient(rec.server.URL)

		_, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.List()).To(Equal([]time.Duration{60 * time.Second}))
	})

	It("falls back to the backoff for a Retry-After it cannot read", func() {
		rec := newRecorder(failFirst(1, http.StatusServiceUnavailable, "Retry-After", "soon"))
		client, recorded := retryClient(rec.server.URL)

		_, err := client.Send(ctx, twoEvents())
		Expect(err).NotTo(HaveOccurred())
		Expect(recorded.List()).To(Equal([]time.Duration{500 * time.Millisecond}))
	})

	It("returns the last *APIError after five 503s, backing off between them", func() {
		rec := newRecorder(failFirst(10, http.StatusServiceUnavailable))
		client, recorded := retryClient(rec.server.URL)

		_, err := client.Send(ctx, twoEvents())
		var apiErr *hummingbee.APIError
		Expect(errors.As(err, &apiErr)).To(BeTrue())
		Expect(apiErr.StatusCode).To(Equal(http.StatusServiceUnavailable))
		Expect(err.Error()).To(ContainSubstring("after 5 attempts"))
		Expect(rec.Requests()).To(HaveLen(5))
		Expect(recorded.List()).To(Equal([]time.Duration{
			500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second,
		}))
	})

	It("makes one request with WithMaxAttempts(1)", func() {
		rec := newRecorder(failFirst(10, http.StatusServiceUnavailable))
		client, recorded := retryClient(rec.server.URL, hummingbee.WithMaxAttempts(1))

		_, err := client.Send(ctx, twoEvents())
		var apiErr *hummingbee.APIError
		Expect(errors.As(err, &apiErr)).To(BeTrue())
		Expect(rec.Requests()).To(HaveLen(1))
		Expect(recorded.List()).To(BeEmpty())
	})

	It("returns the last error at once when the next wait would pass the deadline", func() {
		rec := newRecorder(failFirst(10, http.StatusServiceUnavailable, "Retry-After", "30"))
		client, recorded := retryClient(rec.server.URL)
		deadlineCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		start := time.Now()
		_, err := client.Send(deadlineCtx, twoEvents())
		var apiErr *hummingbee.APIError
		Expect(errors.As(err, &apiErr)).To(BeTrue())
		Expect(apiErr.StatusCode).To(Equal(http.StatusServiceUnavailable))
		Expect(err.Error()).To(ContainSubstring("deadline"))
		Expect(time.Since(start)).To(BeNumerically("<", time.Second))
		Expect(rec.Requests()).To(HaveLen(1))
		Expect(recorded.List()).To(BeEmpty())
	})

	It("does not retry once the context is cancelled during a request", func() {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		rec := newRecorder(func(w http.ResponseWriter, _ int, _ []byte) {
			cancel()
			writeError(w, http.StatusServiceUnavailable, "SPEC_FAILURE")
		})
		client, recorded := retryClient(rec.server.URL)

		_, err := client.Send(cancelCtx, twoEvents())
		Expect(err).To(MatchError(context.Canceled))
		Expect(err.Error()).To(ContainSubstring("failed to send 2 events"))
		Expect(rec.Requests()).To(HaveLen(1))
		Expect(recorded.List()).To(BeEmpty())
	})

	It("stops when the context is cancelled while waiting", func() {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		rec := newRecorder(failFirst(10, http.StatusServiceUnavailable))
		client := hummingbee.New(token, hummingbee.WithBaseURL(rec.server.URL))
		hummingbee.SetClock(client, nil, maxJitter, func(waitCtx context.Context, _ time.Duration) error {
			cancel()
			return waitCtx.Err()
		})

		_, err := client.Send(cancelCtx, twoEvents())
		Expect(err).To(MatchError(context.Canceled))
		var apiErr *hummingbee.APIError
		Expect(errors.As(err, &apiErr)).To(BeTrue(), "the error keeps the last failure")
		Expect(rec.Requests()).To(HaveLen(1))
	})

	It("makes no request for a context already cancelled", func() {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		rec := newRecorder(acceptAll)
		client, recorded := retryClient(rec.server.URL)

		_, err := client.Send(cancelCtx, twoEvents())
		Expect(err).To(MatchError(context.Canceled))
		Expect(rec.Requests()).To(BeEmpty())
		Expect(recorded.List()).To(BeEmpty())
	})
})

var _ = Describe("The backoff", func() {
	It("doubles the step from 500 ms and never exceeds 30 seconds", func() {
		var steps []time.Duration
		for attempt := 1; attempt <= 8; attempt++ {
			steps = append(steps, hummingbee.BackoffStep(attempt))
		}
		Expect(steps).To(Equal([]time.Duration{
			500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second,
			8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second,
		}))
		Expect(hummingbee.BackoffStep(1000)).To(Equal(30 * time.Second))
	})

	It("draws each wait from zero to the step", func() {
		for attempt := 1; attempt <= 10; attempt++ {
			step := hummingbee.BackoffStep(attempt)
			for range 200 {
				wait := hummingbee.FullJitter(step)
				Expect(wait).To(BeNumerically(">=", 0))
				Expect(wait).To(BeNumerically("<=", step))
			}
		}
	})

	It("waits no time for a cancelled context", func() {
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		Expect(hummingbee.Sleep(cancelled, time.Hour)).To(MatchError(context.Canceled))
		Expect(time.Since(start)).To(BeNumerically("<", time.Second))
		Expect(hummingbee.Sleep(context.Background(), 0)).To(Succeed())
	})
})

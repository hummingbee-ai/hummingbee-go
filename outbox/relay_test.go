package outbox_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/gorm"

	"github.com/hummingbee-ai/hummingbee-go"
	"github.com/hummingbee-ai/hummingbee-go/hummingbeetest"
	"github.com/hummingbee-ai/hummingbee-go/outbox"
)

type outboxRowState struct {
	Attempts       int
	EventID        string
	ID             int64
	LastError      sql.NullString
	LeaseToken     sql.NullString
	LeasedUntil    sql.NullTime
	RejectedAt     sql.NullTime
	RejectedField  sql.NullString
	RejectedReason sql.NullString
}

type senderFunc func(ctx context.Context, events []hummingbee.Event) ([]hummingbee.Result, error)

func (f senderFunc) Send(ctx context.Context, events []hummingbee.Event) ([]hummingbee.Result, error) {
	return f(ctx, events)
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type syncBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type cancelOnClaimScanConnector struct {
	base   driver.Connector
	onScan func() error
}

func (c *cancelOnClaimScanConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &cancelOnClaimScanConn{
		Conn:   conn,
		onScan: c.onScan,
	}, nil
}

func (c *cancelOnClaimScanConnector) Driver() driver.Driver {
	return c.base.Driver()
}

type cancelOnClaimScanConn struct {
	driver.Conn
	onScan func() error
}

func (c *cancelOnClaimScanConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *cancelOnClaimScanConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &cancelOnClaimScanRows{
		Rows:   rows,
		onScan: c.onScan,
	}, nil
}

type cancelOnClaimScanRows struct {
	driver.Rows
	onScan func() error
}

func (r *cancelOnClaimScanRows) Next(dest []driver.Value) error {
	if err := r.onScan(); err != nil {
		return err
	}
	return r.Rows.Next(dest)
}

func countOutboxRows(ctx context.Context) int {
	var count int
	ExpectWithOffset(1, testPool.QueryRow(ctx, "SELECT count(*) FROM "+outbox.Table).Scan(&count)).To(Succeed())
	return count
}

func readOutboxRowStates(ctx context.Context) []outboxRowState {
	rows, err := testPool.Query(ctx, `
		SELECT id, attempts, event_id, last_error, lease_token, leased_until, rejected_at, rejected_field, rejected_reason
		FROM `+outbox.Table+`
		ORDER BY id
	`)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer rows.Close()

	var result []outboxRowState
	for rows.Next() {
		var s outboxRowState
		ExpectWithOffset(1, rows.Scan(
			&s.ID,
			&s.Attempts,
			&s.EventID,
			&s.LastError,
			&s.LeaseToken,
			&s.LeasedUntil,
			&s.RejectedAt,
			&s.RejectedField,
			&s.RejectedReason,
		)).To(Succeed())
		result = append(result, s)
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	return result
}

func addEventsBulk(ctx context.Context, events []hummingbee.Event) {
	tx, err := testPool.Begin(ctx)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	for _, ev := range events {
		ExpectWithOffset(1, outbox.Add(ctx, tx, ev)).To(Succeed())
	}
	ExpectWithOffset(1, tx.Commit(ctx)).To(Succeed())
}

func largeValidEvent(id string) hummingbee.Event {
	ev := sampleEvent(id)
	ev.Changes = make([]hummingbee.Change, 100)
	fromVal := strings.Repeat("a", 1000)
	toVal := strings.Repeat("b", 1000)
	for i := range ev.Changes {
		ev.Changes[i] = hummingbee.Change{
			Field: fmt.Sprintf("field_%03d", i),
			From:  fromVal,
			To:    toVal,
		}
	}
	return ev
}

var _ = Describe("Relay", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		server *hummingbeetest.Server
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		server = hummingbeetest.NewServer()
	})

	AfterEach(func() {
		cancel()
		server.Close()
	})

	It("sends only the committed event from GORM on pgx and leaves the table empty", Label("done-when"), func() {
		committed := sampleEvent("order-done-when-committed")
		rolledBack := sampleEvent("order-done-when-rolled-back")

		Expect(testGormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return outbox.Add(ctx, tx, committed)
		})).To(Succeed())

		rollbackErr := errors.New("abort transaction")
		err := testGormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			Expect(outbox.Add(ctx, tx, rolledBack)).To(Succeed())
			return rollbackErr
		})
		Expect(errors.Is(err, rollbackErr)).To(BeTrue())

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testGormDB, server.Client(), outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		events := server.Events()
		Expect(events).To(HaveLen(1))
		Expect(events[0]).To(Equal(committed))
		requestsAfterFirst := server.Requests()
		Expect(requestsAfterFirst).To(Equal(1))

		secondCtx, stopSecond := context.WithTimeout(ctx, 80*time.Millisecond)
		defer stopSecond()
		err = outbox.Relay(secondCtx, testGormDB, server.Client(), outbox.WithPollInterval(10*time.Millisecond))
		Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())
		Expect(server.Events()).To(HaveLen(1))
		Expect(server.Requests()).To(Equal(requestsAfterFirst))
	})

	It("claims 1,250 waiting rows in batches of 500, 500 and 250 and empties the table", func() {
		events := make([]hummingbee.Event, 1250)
		for i := range events {
			events[i] = sampleEvent(fmt.Sprintf("order-batch-%04d", i))
		}
		addEventsBulk(ctx, events)

		client := server.Client()
		var mu sync.Mutex
		var batchSizes []int
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			mu.Lock()
			batchSizes = append(batchSizes, len(batch))
			mu.Unlock()
			return client.Send(sendCtx, batch)
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, sender, outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return countOutboxRows(ctx) }, 10*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		mu.Lock()
		gotSizes := append([]int(nil), batchSizes...)
		mu.Unlock()
		Expect(gotSizes).To(Equal([]int{500, 500, 250}))
		Expect(server.Events()).To(HaveLen(1250))
	})

	It("honours WithBatchSize(100) by claiming at most 100 rows at a time", func() {
		events := make([]hummingbee.Event, 250)
		for i := range events {
			events[i] = sampleEvent(fmt.Sprintf("order-limit-%03d", i))
		}
		addEventsBulk(ctx, events)

		client := server.Client()
		var mu sync.Mutex
		var batchSizes []int
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			mu.Lock()
			batchSizes = append(batchSizes, len(batch))
			mu.Unlock()
			return client.Send(sendCtx, batch)
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(
				relayCtx,
				testPool,
				sender,
				outbox.WithBatchSize(100),
				outbox.WithPollInterval(10*time.Millisecond),
			)
		}()

		Eventually(func() int { return countOutboxRows(ctx) }, 10*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		mu.Lock()
		gotSizes := append([]int(nil), batchSizes...)
		mu.Unlock()
		Expect(gotSizes).To(Equal([]int{100, 100, 50}))
		Expect(server.Events()).To(HaveLen(250))
	})

	It("sends each of 1,250 rows exactly once across four concurrent relays", Label("concurrent"), func() {
		const totalEvents = 1250
		events := make([]hummingbee.Event, totalEvents)
		for i := range events {
			events[i] = sampleEvent(fmt.Sprintf("order-concurrent-%04d", i))
		}
		addEventsBulk(ctx, events)

		client := server.Client()
		var mu sync.Mutex
		sendCounts := make(map[string]int, totalEvents)
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			mu.Lock()
			for _, ev := range batch {
				sendCounts[ev.ID]++
			}
			mu.Unlock()
			return client.Send(sendCtx, batch)
		})

		relayCtx, stopRelays := context.WithCancel(ctx)
		const workers = 4
		errs := make(chan error, workers)
		for range workers {
			go func() {
				errs <- outbox.Relay(
					relayCtx,
					testPool,
					sender,
					outbox.WithBatchSize(100),
					outbox.WithPollInterval(10*time.Millisecond),
				)
			}()
		}

		Eventually(func() int { return countOutboxRows(ctx) }, 15*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelays()
		for range workers {
			Expect(errors.Is(<-errs, context.Canceled)).To(BeTrue())
		}

		mu.Lock()
		defer mu.Unlock()
		Expect(sendCounts).To(HaveLen(totalEvents))
		for _, ev := range events {
			Expect(sendCounts[ev.ID]).To(Equal(1), "event %s sent %d times", ev.ID, sendCounts[ev.ID])
		}
		Expect(server.Events()).To(HaveLen(totalEvents))
	})

	It("holds no idle transaction while sending and lets a second relay claim other rows", func() {
		first := sampleEvent("order-noblock-1")
		second := sampleEvent("order-noblock-2")
		addEventsBulk(ctx, []hummingbee.Event{first, second})

		client := server.Client()
		entered := make(chan struct{})
		release := make(chan struct{})
		var enteredOnce sync.Once

		blockingSender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-release:
				return client.Send(sendCtx, batch)
			case <-sendCtx.Done():
				return nil, sendCtx.Err()
			}
		})

		relay1Ctx, stopRelay1 := context.WithCancel(ctx)
		done1 := make(chan error, 1)
		go func() {
			done1 <- outbox.Relay(
				relay1Ctx,
				testPool,
				blockingSender,
				outbox.WithBatchSize(1),
				outbox.WithPollInterval(10*time.Millisecond),
			)
		}()

		Eventually(entered, 5*time.Second).Should(BeClosed())

		var idleInTx int
		Expect(testPool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE datname = $1
			  AND state = 'idle in transaction'
		`, testDBName).Scan(&idleInTx)).To(Succeed())
		Expect(idleInTx).To(Equal(0))

		relay2Ctx, stopRelay2 := context.WithCancel(ctx)
		done2 := make(chan error, 1)
		go func() {
			done2 <- outbox.Relay(
				relay2Ctx,
				testPool,
				client,
				outbox.WithBatchSize(1),
				outbox.WithPollInterval(10*time.Millisecond),
			)
		}()

		Eventually(func() []hummingbee.Event { return server.Events() }, 5*time.Second, 10*time.Millisecond).Should(HaveLen(1))
		Expect(server.Events()[0].ID).To(Equal(second.ID))

		close(release)
		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(0))

		stopRelay1()
		stopRelay2()
		Expect(errors.Is(<-done1, context.Canceled)).To(BeTrue())
		Expect(errors.Is(<-done2, context.Canceled)).To(BeTrue())
		Expect(server.Events()).To(HaveLen(2))
	})

	It("reclaims expired leases from a stalled relay and ignores the stalled relay's late settle", func() {
		accepted := sampleEvent("order-lease-accepted")
		rejected := sampleEvent("order-lease-rejected")
		addEventsBulk(ctx, []hummingbee.Event{accepted, rejected})
		server.RejectID(rejected.ID, "subject.id", "second_relay_reason")

		client := server.Client()
		entered := make(chan struct{})
		release := make(chan struct{})
		lateSettled := make(chan struct{})
		var enteredOnce sync.Once
		var settledOnce sync.Once

		// First relay stalls past its lease, then returns a stale accepted result
		// for both events (including the rejected one that relay 2 already marked).
		stalledSender := senderFunc(func(_ context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			enteredOnce.Do(func() { close(entered) })
			<-release

			// Send accepted again to verify deduplication on the server, and return
			// synthetic accepted results so a non-conditional DELETE would wipe the
			// rejected row.
			_, err := client.Send(ctx, []hummingbee.Event{accepted})
			Expect(err).NotTo(HaveOccurred())

			results := make([]hummingbee.Result, len(batch))
			for i, ev := range batch {
				results[i] = hummingbee.Result{
					ID:     ev.ID,
					Index:  i,
					Status: hummingbee.StatusAccepted,
				}
			}
			settledOnce.Do(func() { close(lateSettled) })
			return results, nil
		})

		var relay1Logs syncBuffer
		relay1Logger := slog.New(slog.NewTextHandler(&relay1Logs, nil))

		relay1Ctx, stopRelay1 := context.WithCancel(ctx)
		done1 := make(chan error, 1)
		go func() {
			done1 <- outbox.Relay(
				relay1Ctx,
				testPool,
				stalledSender,
				outbox.WithLease(250*time.Millisecond),
				outbox.WithLogger(relay1Logger),
				outbox.WithPollInterval(20*time.Millisecond),
			)
		}()

		Eventually(entered, 5*time.Second).Should(BeClosed())

		relay2Ctx, stopRelay2 := context.WithCancel(ctx)
		done2 := make(chan error, 1)
		go func() {
			done2 <- outbox.Relay(
				relay2Ctx,
				testPool,
				client,
				outbox.WithLease(5*time.Second),
				outbox.WithPollInterval(20*time.Millisecond),
			)
		}()

		Eventually(func() []outboxRowState { return readOutboxRowStates(ctx) }, 5*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			HaveLen(1),
			WithTransform(func(rows []outboxRowState) bool {
				return rows[0].EventID == rejected.ID && rows[0].RejectedAt.Valid
			}, BeTrue()),
		))
		stopRelay2()
		Expect(errors.Is(<-done2, context.Canceled)).To(BeTrue())

		// Unblock relay 1 and wait for its late settle round to complete against
		// Postgres with the expired token before stopping it.
		close(release)
		Eventually(lateSettled, 5*time.Second).Should(BeClosed())
		Eventually(relay1Logs.String, 5*time.Second, 10*time.Millisecond).Should(ContainSubstring("outbox relay round completed"))

		stopRelay1()
		Expect(errors.Is(<-done1, context.Canceled)).To(BeTrue())

		// Server recorded the accepted event once, and relay 1's late settle did
		// not delete or alter the rejected row.
		Expect(server.Events()).To(HaveLen(1))
		Expect(server.Events()[0].ID).To(Equal(accepted.ID))

		rows := readOutboxRowStates(ctx)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].EventID).To(Equal(rejected.ID))
		Expect(rows[0].RejectedAt.Valid).To(BeTrue())
		Expect(rows[0].RejectedField.String).To(Equal("subject.id"))
		Expect(rows[0].RejectedReason.String).To(Equal("second_relay_reason"))
	})

	It("keeps rejected rows with field and reason, deletes the rest, and never sends rejected rows again", func() {
		good1 := sampleEvent("order-reject-good-1")
		bad := sampleEvent("order-reject-bad")
		good2 := sampleEvent("order-reject-good-2")
		addEventsBulk(ctx, []hummingbee.Event{good1, bad, good2})

		server.RejectID(bad.ID, "subject.id", "invalid_uuid")

		client := server.Client()
		var mu sync.Mutex
		sentCount := map[string]int{}
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			mu.Lock()
			for _, ev := range batch {
				sentCount[ev.ID]++
			}
			mu.Unlock()
			return client.Send(sendCtx, batch)
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, sender, outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return len(server.Events()) }, 5*time.Second, 10*time.Millisecond).Should(Equal(2))
		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(1))

		// Add another event so at least one more active round runs while the
		// rejected row sits in the table.
		after := sampleEvent("order-reject-after")
		addEventsBulk(ctx, []hummingbee.Event{after})
		Eventually(func() int { return len(server.Events()) }, 5*time.Second, 10*time.Millisecond).Should(Equal(3))

		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		rows := readOutboxRowStates(ctx)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].EventID).To(Equal(bad.ID))
		Expect(rows[0].RejectedAt.Valid).To(BeTrue())
		Expect(rows[0].RejectedField.Valid).To(BeTrue())
		Expect(rows[0].RejectedField.String).To(Equal("subject.id"))
		Expect(rows[0].RejectedReason.Valid).To(BeTrue())
		Expect(rows[0].RejectedReason.String).To(Equal("invalid_uuid"))
		Expect(rows[0].LeaseToken.Valid).To(BeFalse())
		Expect(rows[0].LeasedUntil.Valid).To(BeFalse())

		mu.Lock()
		defer mu.Unlock()
		Expect(sentCount[bad.ID]).To(Equal(1))
	})

	It("deletes rows reported as duplicate by the server", func() {
		ev := sampleEvent("order-duplicate-1")
		results, err := server.Client().Send(ctx, []hummingbee.Event{ev})
		Expect(err).NotTo(HaveOccurred())
		Expect(results).To(HaveLen(1))
		Expect(results[0].Status).To(Equal(hummingbee.StatusAccepted))

		addEventsBulk(ctx, []hummingbee.Event{ev})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, server.Client(), outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		Expect(server.Events()).To(HaveLen(1))
		Expect(server.Events()[0].ID).To(Equal(ev.ID))
	})

	It("records last_error and future lease on failure, then delivers once the server recovers", func() {
		events := []hummingbee.Event{
			sampleEvent("order-fail-1"),
			sampleEvent("order-fail-2"),
		}
		addEventsBulk(ctx, events)

		server.FailNext(100, http.StatusInternalServerError)

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(
				relayCtx,
				testPool,
				server.Client(hummingbee.WithMaxAttempts(1)),
				outbox.WithPollInterval(40*time.Millisecond),
			)
		}()

		Eventually(func() bool {
			rows := readOutboxRowStates(ctx)
			if len(rows) != 2 {
				return false
			}
			var dbNow time.Time
			Expect(testPool.QueryRow(ctx, "SELECT now()").Scan(&dbNow)).To(Succeed())
			for _, r := range rows {
				if !r.LastError.Valid || r.LastError.String == "" {
					return false
				}
				if r.LeaseToken.Valid {
					return false
				}
				if !r.LeasedUntil.Valid || !r.LeasedUntil.Time.After(dbNow) {
					return false
				}
			}
			return true
		}, 5*time.Second, 10*time.Millisecond).Should(BeTrue())

		server.FailNext(0, 0)

		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())
		Expect(server.Events()).To(HaveLen(2))
	})

	It("keeps rows and keeps running when the server answers 401", func() {
		addEventsBulk(ctx, []hummingbee.Event{sampleEvent("order-unauth-1")})

		badClient := hummingbee.New("wrong-token", hummingbee.WithBaseURL(server.URL()))
		var attempts atomic.Int32
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			attempts.Add(1)
			return badClient.Send(sendCtx, batch)
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, sender, outbox.WithPollInterval(20*time.Millisecond))
		}()

		Eventually(attempts.Load, 5*time.Second, 10*time.Millisecond).Should(BeNumerically(">=", 2))
		Consistently(done, 50*time.Millisecond).ShouldNot(Receive())

		rows := readOutboxRowStates(ctx)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].LastError.Valid).To(BeTrue())
		Expect(rows[0].LastError.String).To(ContainSubstring("401"))

		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())
		Expect(server.Events()).To(BeEmpty())
	})

	It("splits a claim over 1 MiB into multiple requests each under 1 MiB", func() {
		const count = 7
		events := make([]hummingbee.Event, count)
		for i := range events {
			events[i] = largeValidEvent(fmt.Sprintf("order-split-%02d", i))
		}
		addEventsBulk(ctx, events)

		client := server.Client()
		var mu sync.Mutex
		var requestBytes []int
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			raw, err := json.Marshal(batch)
			Expect(err).NotTo(HaveOccurred())
			mu.Lock()
			requestBytes = append(requestBytes, len(raw))
			mu.Unlock()
			return client.Send(sendCtx, batch)
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, sender, outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return countOutboxRows(ctx) }, 10*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		mu.Lock()
		gotBytes := append([]int(nil), requestBytes...)
		mu.Unlock()
		Expect(len(gotBytes)).To(BeNumerically(">", 1))
		for _, size := range gotBytes {
			Expect(size).To(BeNumerically("<=", 1<<20))
		}
		Expect(server.Events()).To(HaveLen(count))
	})

	It("shares one claim-anchored send deadline across sub-batches within the single claim lease", func() {
		const count = 7
		events := make([]hummingbee.Event, count)
		for i := range events {
			events[i] = largeValidEvent(fmt.Sprintf("order-deadline-%02d", i))
		}
		addEventsBulk(ctx, events)

		const lease = 500 * time.Millisecond

		var mu sync.Mutex
		var deadlines []time.Time
		var firstSendStart time.Time
		var secondTimedOutAt time.Time
		var callNum int
		secondTimedOut := make(chan struct{})
		var secondOnce sync.Once

		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			deadline, ok := sendCtx.Deadline()
			Expect(ok).To(BeTrue())

			mu.Lock()
			callNum++
			n := callNum
			deadlines = append(deadlines, deadline)
			if n == 1 {
				firstSendStart = time.Now()
			}
			mu.Unlock()

			if n == 1 {
				timer := time.NewTimer(150 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-sendCtx.Done():
					return nil, sendCtx.Err()
				}
				results := make([]hummingbee.Result, len(batch))
				for i, ev := range batch {
					results[i] = hummingbee.Result{
						ID:     ev.ID,
						Index:  i,
						Status: hummingbee.StatusAccepted,
					}
				}
				return results, nil
			}

			<-sendCtx.Done()
			mu.Lock()
			secondTimedOutAt = time.Now()
			mu.Unlock()
			secondOnce.Do(func() { close(secondTimedOut) })
			return nil, sendCtx.Err()
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(
				relayCtx,
				testPool,
				sender,
				outbox.WithLease(lease),
				outbox.WithPollInterval(5*time.Second),
			)
		}()

		Eventually(secondTimedOut, 5*time.Second).Should(BeClosed())
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		mu.Lock()
		gotDeadlines := append([]time.Time(nil), deadlines...)
		elapsed := secondTimedOutAt.Sub(firstSendStart)
		mu.Unlock()

		Expect(gotDeadlines).To(HaveLen(2))
		Expect(gotDeadlines[1]).To(Equal(gotDeadlines[0]))
		Expect(elapsed).To(BeNumerically("<", lease))
	})

	It("stops sending subsequent sub-batches after a slow sub-batch exhausts the single claim lease send deadline", func() {
		const count = 7
		events := make([]hummingbee.Event, count)
		for i := range events {
			events[i] = largeValidEvent(fmt.Sprintf("order-exhausted-%02d", i))
		}
		addEventsBulk(ctx, events)

		const lease = 150 * time.Millisecond
		var sendCalls atomic.Int32
		sender := senderFunc(func(_ context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			sendCalls.Add(1)
			time.Sleep(160 * time.Millisecond)
			results := make([]hummingbee.Result, len(batch))
			for i, ev := range batch {
				results[i] = hummingbee.Result{
					ID:     ev.ID,
					Index:  i,
					Status: hummingbee.StatusAccepted,
				}
			}
			return results, nil
		})

		var logBuf syncBuffer
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(
				relayCtx,
				testPool,
				sender,
				outbox.WithLease(lease),
				outbox.WithLogger(logger),
				outbox.WithPollInterval(5*time.Second),
			)
		}()

		Eventually(logBuf.String, 5*time.Second, 10*time.Millisecond).Should(ContainSubstring("outbox relay round failed"))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		Expect(sendCalls.Load()).To(Equal(int32(1)))
		remaining := readOutboxRowStates(ctx)
		Expect(remaining).NotTo(BeEmpty())
		for _, row := range remaining {
			Expect(row.LastError.Valid).To(BeTrue())
			Expect(row.LastError.String).To(ContainSubstring(context.DeadlineExceeded.Error()))
		}
	})

	It("marks a single event over 1 MiB rejected with too_large and sends the rest", func() {
		huge := sampleEvent("order-too-large")
		huge.Changes = []hummingbee.Change{
			{Field: "payload", From: nil, To: strings.Repeat("x", (1<<20)+1024)},
		}
		normal1 := sampleEvent("order-normal-1")
		normal2 := sampleEvent("order-normal-2")
		addEventsBulk(ctx, []hummingbee.Event{normal1, huge, normal2})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, server.Client(), outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return len(server.Events()) }, 5*time.Second, 10*time.Millisecond).Should(Equal(2))
		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(1))

		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		rows := readOutboxRowStates(ctx)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].EventID).To(Equal(huge.ID))
		Expect(rows[0].RejectedAt.Valid).To(BeTrue())
		Expect(rows[0].RejectedField.Valid).To(BeTrue())
		Expect(rows[0].RejectedField.String).To(Equal(""))
		Expect(rows[0].RejectedReason.Valid).To(BeTrue())
		Expect(rows[0].RejectedReason.String).To(Equal("too_large"))
		Expect(rows[0].LeaseToken.Valid).To(BeFalse())
		Expect(rows[0].LeasedUntil.Valid).To(BeFalse())
	})

	It("releases held leases when ctx is cancelled mid-send", func() {
		addEventsBulk(ctx, []hummingbee.Event{
			sampleEvent("order-shutdown-1"),
			sampleEvent("order-shutdown-2"),
		})

		entered := make(chan struct{})
		var enteredOnce sync.Once
		blockingSender := senderFunc(func(sendCtx context.Context, _ []hummingbee.Event) ([]hummingbee.Result, error) {
			enteredOnce.Do(func() { close(entered) })
			<-sendCtx.Done()
			return nil, sendCtx.Err()
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, blockingSender, outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(entered, 5*time.Second).Should(BeClosed())

		leasedRows := readOutboxRowStates(ctx)
		Expect(leasedRows).To(HaveLen(2))
		for _, r := range leasedRows {
			Expect(r.LeaseToken.Valid).To(BeTrue())
			Expect(r.LeasedUntil.Valid).To(BeTrue())
		}

		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		releasedRows := readOutboxRowStates(ctx)
		Expect(releasedRows).To(HaveLen(2))
		for _, r := range releasedRows {
			Expect(r.LeaseToken.Valid).To(BeFalse())
			Expect(r.LeasedUntil.Valid).To(BeFalse())
		}
	})

	It("releases held leases when ctx is cancelled while reading claim RETURNING rows", func() {
		addEventsBulk(ctx, []hummingbee.Event{
			sampleEvent("order-claim-shutdown-1"),
			sampleEvent("order-claim-shutdown-2"),
		})

		pgxCfg, err := pgx.ParseConfig(testDatabaseURL)
		Expect(err).NotTo(HaveOccurred())

		relayCtx, stopRelay := context.WithCancel(ctx)
		var cancelOnce sync.Once
		sqlDB := sql.OpenDB(&cancelOnClaimScanConnector{
			base: stdlib.GetConnector(*pgxCfg),
			onScan: func() error {
				cancelOnce.Do(stopRelay)
				return relayCtx.Err()
			},
		})
		defer func() { Expect(sqlDB.Close()).To(Succeed()) }()

		err = outbox.Relay(relayCtx, sqlDB, server.Client(), outbox.WithPollInterval(10*time.Millisecond))
		Expect(errors.Is(err, context.Canceled)).To(BeTrue())

		rows := readOutboxRowStates(ctx)
		Expect(rows).To(HaveLen(2))
		for _, r := range rows {
			Expect(r.Attempts).To(Equal(1))
			Expect(r.LeaseToken.Valid).To(BeFalse())
			Expect(r.LeasedUntil.Valid).To(BeFalse())
		}
		Expect(server.Events()).To(BeEmpty())
	})

	It("delivers through *sql.DB, *pgxpool.Pool and *gorm.DB and rejects unsupported db handles", func() {
		handles := []struct {
			db      any
			eventID string
		}{
			{db: testSQLDB, eventID: "order-db-sql"},
			{db: testPool, eventID: "order-db-pgx"},
			{db: testGormDB, eventID: "order-db-gorm"},
		}

		for _, tc := range handles {
			addEventsBulk(ctx, []hummingbee.Event{sampleEvent(tc.eventID)})
			relayCtx, stopRelay := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				done <- outbox.Relay(relayCtx, tc.db, server.Client(), outbox.WithPollInterval(10*time.Millisecond))
			}()

			Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(0))
			stopRelay()
			Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())
		}
		Expect(server.Events()).To(HaveLen(len(handles)))

		var nilSQLDB *sql.DB
		var nilPool *pgxpool.Pool
		var nilGorm *gorm.DB
		unsupported := []any{
			nil,
			nilSQLDB,
			nilPool,
			nilGorm,
			&gorm.DB{},
			"not-a-db",
			42,
		}
		for _, db := range unsupported {
			err := outbox.Relay(ctx, db, server.Client())
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, outbox.ErrUnsupportedDB)).To(BeTrue(), "expected ErrUnsupportedDB for %T, got %v", db, err)
		}

		var nilClient *hummingbee.Client
		Expect(outbox.Relay(ctx, testPool, nil)).To(HaveOccurred())
		Expect(outbox.Relay(ctx, testPool, nilClient)).To(HaveOccurred())
	})

	It("never logs event change values in round logs", func() {
		const canary = "CANARY-CHANGE-VALUE-739184"
		accepted := sampleEvent("order-log-ok")
		rejected := sampleEvent("order-log-rejected")
		rejected.Changes = []hummingbee.Change{
			{Field: "status", From: "open", To: canary},
		}
		addEventsBulk(ctx, []hummingbee.Event{accepted, rejected})
		server.RejectID(rejected.ID, "changes[0].to", "invalid_change_value")

		var logBuf syncBuffer
		logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(
				relayCtx,
				testPool,
				server.Client(),
				outbox.WithLogger(logger),
				outbox.WithPollInterval(10*time.Millisecond),
			)
		}()

		Eventually(func() int { return len(server.Events()) }, 5*time.Second, 10*time.Millisecond).Should(Equal(1))
		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(1))

		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		logged := logBuf.String()
		Expect(logged).To(ContainSubstring("sent=2"))
		Expect(logged).To(ContainSubstring("settled=1"))
		Expect(logged).To(ContainSubstring("rejected=1"))
		Expect(logged).To(ContainSubstring("failed=0"))
		Expect(logged).NotTo(ContainSubstring(canary))
	})

	It("preserves >53-bit integer and high-precision decimal change values verbatim on the wire", func() {
		const (
			largeInt    int64  = (1 << 53) + 1 // 9007199254740993; float64 rounds this to 9007199254740992
			largeUint   uint64 = math.MaxUint64
			highPrecNum        = json.Number("12345678901234567890.123456789")
		)
		ev := sampleEvent("order-precise-numbers")
		ev.Changes = []hummingbee.Change{
			{Field: "big_int", From: largeInt, To: largeUint},
			{Field: "amount", From: json.Number("0.000000000000000001"), To: highPrecNum},
		}
		addEventsBulk(ctx, []hummingbee.Event{ev})

		var mu sync.Mutex
		var wireBodies []string
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rawBody, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				req.Body = io.NopCloser(bytes.NewReader(rawBody))

				zr, err := gzip.NewReader(bytes.NewReader(rawBody))
				if err != nil {
					return nil, err
				}
				decoded, err := io.ReadAll(zr)
				_ = zr.Close()
				if err != nil {
					return nil, err
				}
				mu.Lock()
				wireBodies = append(wireBodies, string(decoded))
				mu.Unlock()

				return http.DefaultTransport.RoundTrip(req)
			}),
		}

		client := server.Client(hummingbee.WithHTTPClient(httpClient))
		var sentChanges []hummingbee.Change
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			mu.Lock()
			if len(batch) == 1 {
				sentChanges = append([]hummingbee.Change(nil), batch[0].Changes...)
			}
			mu.Unlock()
			return client.Send(sendCtx, batch)
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, sender, outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		Expect(server.Events()).To(HaveLen(1))
		mu.Lock()
		defer mu.Unlock()
		Expect(sentChanges).To(Equal([]hummingbee.Change{
			{Field: "big_int", From: json.Number("9007199254740993"), To: json.Number("18446744073709551615")},
			{Field: "amount", From: json.Number("0.000000000000000001"), To: highPrecNum},
		}))
		Expect(wireBodies).To(HaveLen(1))
		Expect(wireBodies[0]).To(ContainSubstring(`"from":9007199254740993`))
		Expect(wireBodies[0]).To(ContainSubstring(`"to":18446744073709551615`))
		Expect(wireBodies[0]).To(ContainSubstring(`"from":0.000000000000000001`))
		Expect(wireBodies[0]).To(ContainSubstring(`"to":12345678901234567890.123456789`))
	})

	It("rejects a single event whose HTML-escaped wire JSON exceeds 1 MiB as too_large without hitting 413", func() {
		// Postgres jsonb stores literal '<' (1 byte, ~360 KiB total <= 1 MiB),
		// while Client.Send HTML-escapes each '<' to \u003c (6 bytes, ~2.16 MiB > 2 MiB).
		htmlHuge := sampleEvent("order-html-too-large")
		htmlHuge.Changes = []hummingbee.Change{
			{Field: "payload", From: nil, To: strings.Repeat("<", 360*1024)},
		}
		normal := sampleEvent("order-after-html-too-large")
		addEventsBulk(ctx, []hummingbee.Event{htmlHuge, normal})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(
				relayCtx,
				testPool,
				server.Client(hummingbee.WithMaxAttempts(1)),
				outbox.WithPollInterval(10*time.Millisecond),
			)
		}()

		Eventually(func() int { return len(server.Events()) }, 5*time.Second, 10*time.Millisecond).Should(Equal(1))
		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(1))

		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		Expect(server.Events()[0].ID).To(Equal(normal.ID))
		Expect(server.Requests()).To(Equal(1))

		rows := readOutboxRowStates(ctx)
		Expect(rows).To(HaveLen(1))
		Expect(rows[0].EventID).To(Equal(htmlHuge.ID))
		Expect(rows[0].RejectedAt.Valid).To(BeTrue())
		Expect(rows[0].RejectedField.Valid).To(BeTrue())
		Expect(rows[0].RejectedField.String).To(Equal(""))
		Expect(rows[0].RejectedReason.Valid).To(BeTrue())
		Expect(rows[0].RejectedReason.String).To(Equal("too_large"))
		Expect(rows[0].LeaseToken.Valid).To(BeFalse())
		Expect(rows[0].LeasedUntil.Valid).To(BeFalse())
	})

	It("splits valid events whose combined HTML-escaped wire JSON exceeds 1 MiB even when jsonb bytes are under 1 MiB", func() {
		// Four valid wire-format-v1 events: ~102 KiB each in jsonb (~408 KiB total),
		// but ~602 KiB each on the wire after HTML escaping (~2.4 MiB combined > 2 MiB).
		const count = 4
		events := make([]hummingbee.Event, count)
		fromVal := strings.Repeat("<", 1000)
		toVal := strings.Repeat(">", 500) + strings.Repeat("&", 500)
		for i := range events {
			ev := sampleEvent(fmt.Sprintf("order-html-split-%02d", i))
			ev.Changes = make([]hummingbee.Change, 50)
			for j := range ev.Changes {
				ev.Changes[j] = hummingbee.Change{
					Field: fmt.Sprintf("field_%02d", j),
					From:  fromVal,
					To:    toVal,
				}
			}
			events[i] = ev
		}
		addEventsBulk(ctx, events)

		client := server.Client(hummingbee.WithMaxAttempts(1))
		var mu sync.Mutex
		var requestBytes []int
		sender := senderFunc(func(sendCtx context.Context, batch []hummingbee.Event) ([]hummingbee.Result, error) {
			raw, err := json.Marshal(batch)
			Expect(err).NotTo(HaveOccurred())
			mu.Lock()
			requestBytes = append(requestBytes, len(raw))
			mu.Unlock()
			return client.Send(sendCtx, batch)
		})

		relayCtx, stopRelay := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- outbox.Relay(relayCtx, testPool, sender, outbox.WithPollInterval(10*time.Millisecond))
		}()

		Eventually(func() int { return countOutboxRows(ctx) }, 5*time.Second, 10*time.Millisecond).Should(Equal(0))
		stopRelay()
		Expect(errors.Is(<-done, context.Canceled)).To(BeTrue())

		mu.Lock()
		gotBytes := append([]int(nil), requestBytes...)
		mu.Unlock()
		Expect(len(gotBytes)).To(BeNumerically(">", 1))
		for _, size := range gotBytes {
			Expect(size).To(BeNumerically("<=", 1<<20))
		}
		Expect(server.Events()).To(HaveLen(count))
	})
})

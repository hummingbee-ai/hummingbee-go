package outbox

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"

	"github.com/hummingbee-ai/hummingbee-go"
)

const (
	defaultLease           = 5 * time.Minute
	defaultPollInterval    = time.Second
	maxBackoff             = time.Minute
	maxSubBatchBytes       = 1 << 20
	reasonInvalidJSON      = "invalid_json"
	reasonTooLarge         = "too_large"
	shutdownReleaseTimeout = 2 * time.Second
)

const (
	claimRowsSQL = `
		UPDATE ` + Table + `
		SET attempts = attempts + 1,
		    lease_token = $1,
		    leased_until = now() + ($2 * interval '1 microsecond')
		WHERE id IN (
			SELECT id
			FROM ` + Table + `
			WHERE rejected_at IS NULL
			  AND (leased_until IS NULL OR leased_until < now())
			ORDER BY id
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, event`

	deleteSettledSQL = `
		DELETE FROM ` + Table + `
		WHERE lease_token = $1
		  AND id = ANY(string_to_array($2, ',')::bigint[])`

	failRowsSQL = `
		UPDATE ` + Table + `
		SET last_error = $2,
		    lease_token = NULL,
		    leased_until = now() + ($3 * interval '1 microsecond')
		WHERE lease_token = $1`

	markRejectedSQL = `
		UPDATE ` + Table + ` AS o
		SET rejected_at = now(),
		    rejected_field = r.field,
		    rejected_reason = r.reason,
		    lease_token = NULL,
		    leased_until = NULL
		FROM jsonb_to_recordset($2::jsonb) AS r(id bigint, field text, reason text)
		WHERE o.id = r.id
		  AND o.lease_token = $1`

	releaseLeasesSQL = `
		UPDATE ` + Table + `
		SET lease_token = NULL,
		    leased_until = NULL
		WHERE lease_token = $1`
)

// ErrUnsupportedDB is returned by [Relay] when given a database handle that is
// nil or not one of *sql.DB, *pgxpool.Pool or *gorm.DB.
var ErrUnsupportedDB = errors.New("outbox: unsupported database type")

// Sender sends a batch of events to Hummingbee's events endpoint.
// [*hummingbee.Client] satisfies this interface.
type Sender interface {
	Send(ctx context.Context, events []hummingbee.Event) ([]hummingbee.Result, error)
}

// Option configures [Relay].
type Option func(*relayConfig)

type relayConfig struct {
	batchSize    int
	lease        time.Duration
	logger       *slog.Logger
	pollInterval time.Duration
}

// WithBatchSize sets the maximum number of outbox rows claimed in one round.
// Values below 1 are clamped to 1 and values above [hummingbee.MaxBatch] are
// clamped to [hummingbee.MaxBatch]. The default is [hummingbee.MaxBatch].
func WithBatchSize(n int) Option {
	return func(c *relayConfig) {
		c.batchSize = max(min(n, hummingbee.MaxBatch), 1)
	}
}

// WithLease sets how long a claimed batch is leased before another relay may
// reclaim it. Non-positive durations are ignored. The default is 5 minutes.
func WithLease(d time.Duration) Option {
	return func(c *relayConfig) {
		if d > 0 {
			c.lease = d
		}
	}
}

// WithLogger sets the logger used for round summaries and errors. A nil logger
// is ignored. The default is [slog.Default].
func WithLogger(logger *slog.Logger) Option {
	return func(c *relayConfig) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithPollInterval sets how long [Relay] waits between polls when fewer than a
// full batch of rows were waiting. Non-positive durations are ignored. The
// default is 1 second.
func WithPollInterval(d time.Duration) Option {
	return func(c *relayConfig) {
		if d > 0 {
			c.pollInterval = d
		}
	}
}

type claimedRow struct {
	id  int64
	raw []byte
}

type claimedEvent struct {
	event hummingbee.Event
	id    int64
	size  int
}

type rejectionParam struct {
	Field  string `json:"field"`
	ID     int64  `json:"id"`
	Reason string `json:"reason"`
}

type roundStats struct {
	failed   int
	rejected int
	sent     int
	settled  int
}

type querier interface {
	claim(ctx context.Context, token string, leaseMicros int64, limit int) ([]claimedRow, error)
	exec(ctx context.Context, query string, args ...any) error
}

type sqlQuerier struct {
	db *sql.DB
}

func (q *sqlQuerier) claim(ctx context.Context, token string, leaseMicros int64, limit int) ([]claimedRow, error) {
	rows, err := q.db.QueryContext(ctx, claimRowsSQL, token, leaseMicros, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to claim outbox rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var claimed []claimedRow
	for rows.Next() {
		var id int64
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("failed to scan claimed outbox row: %w", err)
		}
		claimed = append(claimed, claimedRow{id: id, raw: raw})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate claimed outbox rows: %w", err)
	}
	slices.SortFunc(claimed, func(a, b claimedRow) int {
		return cmp.Compare(a.id, b.id)
	})
	return claimed, nil
}

func (q *sqlQuerier) exec(ctx context.Context, query string, args ...any) error {
	_, err := q.db.ExecContext(ctx, query, args...)
	return err
}

type pgxQuerier struct {
	pool *pgxpool.Pool
}

func (q *pgxQuerier) claim(ctx context.Context, token string, leaseMicros int64, limit int) ([]claimedRow, error) {
	rows, err := q.pool.Query(ctx, claimRowsSQL, token, leaseMicros, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to claim outbox rows: %w", err)
	}
	defer rows.Close()

	var claimed []claimedRow
	for rows.Next() {
		var id int64
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, fmt.Errorf("failed to scan claimed outbox row: %w", err)
		}
		claimed = append(claimed, claimedRow{id: id, raw: raw})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate claimed outbox rows: %w", err)
	}
	slices.SortFunc(claimed, func(a, b claimedRow) int {
		return cmp.Compare(a.id, b.id)
	})
	return claimed, nil
}

func (q *pgxQuerier) exec(ctx context.Context, query string, args ...any) error {
	_, err := q.pool.Exec(ctx, query, args...)
	return err
}

// Relay continuously claims waiting rows from [Table], sends them through
// sender in batches, deletes accepted and duplicate rows, and marks rejected
// rows. It is safe to run concurrently across several processes.
//
// db must be a *sql.DB, *pgxpool.Pool or *gorm.DB; any other handle returns
// [ErrUnsupportedDB] immediately. Otherwise Relay runs until ctx is cancelled,
// releasing any held leases before returning ctx.Err().
func Relay(ctx context.Context, db any, sender Sender, options ...Option) error {
	q, err := resolveDB(db)
	if err != nil {
		return fmt.Errorf("failed to start outbox relay: %w", err)
	}
	if isTypedNil(sender) {
		return errors.New("failed to start outbox relay: sender is nil")
	}

	cfg := relayConfig{
		batchSize:    hummingbee.MaxBatch,
		lease:        defaultLease,
		logger:       slog.Default(),
		pollInterval: defaultPollInterval,
	}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}

	consecutiveFailures := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		claimedCount, shutdown, roundErr := runRound(ctx, q, sender, cfg, &consecutiveFailures)
		if shutdown {
			return ctx.Err()
		}

		var wait time.Duration
		switch {
		case roundErr != nil:
			wait = nextBackoff(cfg.pollInterval, consecutiveFailures)
		case claimedCount >= cfg.batchSize:
			continue
		default:
			wait = cfg.pollInterval
		}

		if err := sleepContext(ctx, wait); err != nil {
			return err
		}
	}
}

func resolveDB(db any) (querier, error) {
	switch handle := db.(type) {
	case *sql.DB:
		if handle == nil {
			return nil, fmt.Errorf("%w: nil *sql.DB", ErrUnsupportedDB)
		}
		return &sqlQuerier{db: handle}, nil

	case *pgxpool.Pool:
		if handle == nil {
			return nil, fmt.Errorf("%w: nil *pgxpool.Pool", ErrUnsupportedDB)
		}
		return &pgxQuerier{pool: handle}, nil

	case *gorm.DB:
		if handle == nil {
			return nil, fmt.Errorf("%w: nil *gorm.DB", ErrUnsupportedDB)
		}
		if handle.Config == nil {
			return nil, fmt.Errorf("%w: uninitialized *gorm.DB", ErrUnsupportedDB)
		}
		sqlDB, err := handle.DB()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnsupportedDB, err)
		}
		if sqlDB == nil {
			return nil, fmt.Errorf("%w: gorm.DB has nil *sql.DB", ErrUnsupportedDB)
		}
		return &sqlQuerier{db: sqlDB}, nil

	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedDB, db)
	}
}

func runRound(
	ctx context.Context,
	q querier,
	sender Sender,
	cfg relayConfig,
	consecutiveFailures *int,
) (claimedCount int, shutdown bool, roundErr error) {
	token := newLeaseToken()
	leaseMicros := max(cfg.lease.Microseconds(), 1)

	claimed, err := q.claim(ctx, token, leaseMicros, cfg.batchSize)
	if err != nil {
		if ctx.Err() != nil {
			return 0, true, nil
		}
		*consecutiveFailures++
		logRound(ctx, cfg.logger, roundStats{}, err)
		return 0, false, err
	}
	if len(claimed) == 0 {
		return 0, false, nil
	}

	var stats roundStats
	subBatches, localRejections := splitClaimedRows(claimed)

	if len(localRejections) > 0 {
		if err := markRejected(ctx, q, token, localRejections); err != nil {
			if ctx.Err() != nil {
				releaseLeases(ctx, q, token, cfg.logger)
				return len(claimed), true, nil
			}
			*consecutiveFailures++
			backoff := nextBackoff(cfg.pollInterval, *consecutiveFailures)
			stats.failed = len(claimed)
			recordFailure(ctx, q, token, err, backoff, cfg.logger)
			logRound(ctx, cfg.logger, stats, err)
			return len(claimed), false, err
		}
		stats.rejected += len(localRejections)
	}

	sendTimeout := max(cfg.lease*4/5, time.Millisecond)

	for i, batch := range subBatches {
		if ctx.Err() != nil {
			releaseLeases(ctx, q, token, cfg.logger)
			return len(claimed), true, nil
		}

		events := make([]hummingbee.Event, len(batch))
		for j, item := range batch {
			events[j] = item.event
		}

		sendCtx, cancelSend := context.WithTimeout(ctx, sendTimeout)
		results, err := sender.Send(sendCtx, events)
		cancelSend()

		stats.sent += len(batch)
		if err != nil {
			if ctx.Err() != nil {
				releaseLeases(ctx, q, token, cfg.logger)
				return len(claimed), true, nil
			}
			*consecutiveFailures++
			backoff := nextBackoff(cfg.pollInterval, *consecutiveFailures)
			stats.failed += countRemaining(subBatches[i:])
			recordFailure(ctx, q, token, err, backoff, cfg.logger)
			logRound(ctx, cfg.logger, stats, err)
			return len(claimed), false, err
		}

		settledIDs, rejections, err := classifyResults(batch, results)
		if err != nil {
			*consecutiveFailures++
			backoff := nextBackoff(cfg.pollInterval, *consecutiveFailures)
			stats.failed += countRemaining(subBatches[i:])
			recordFailure(ctx, q, token, err, backoff, cfg.logger)
			logRound(ctx, cfg.logger, stats, err)
			return len(claimed), false, err
		}

		if len(settledIDs) > 0 {
			if err := deleteSettled(ctx, q, token, settledIDs); err != nil {
				if ctx.Err() != nil {
					releaseLeases(ctx, q, token, cfg.logger)
					return len(claimed), true, nil
				}
				*consecutiveFailures++
				backoff := nextBackoff(cfg.pollInterval, *consecutiveFailures)
				stats.failed += countRemaining(subBatches[i:])
				recordFailure(ctx, q, token, err, backoff, cfg.logger)
				logRound(ctx, cfg.logger, stats, err)
				return len(claimed), false, err
			}
			stats.settled += len(settledIDs)
		}

		if len(rejections) > 0 {
			if err := markRejected(ctx, q, token, rejections); err != nil {
				if ctx.Err() != nil {
					releaseLeases(ctx, q, token, cfg.logger)
					return len(claimed), true, nil
				}
				*consecutiveFailures++
				backoff := nextBackoff(cfg.pollInterval, *consecutiveFailures)
				stats.failed += countRemaining(subBatches[i:])
				recordFailure(ctx, q, token, err, backoff, cfg.logger)
				logRound(ctx, cfg.logger, stats, err)
				return len(claimed), false, err
			}
			stats.rejected += len(rejections)
		}
	}

	*consecutiveFailures = 0
	logRound(ctx, cfg.logger, stats, nil)
	return len(claimed), false, nil
}

func splitClaimedRows(rows []claimedRow) ([][]claimedEvent, []rejectionParam) {
	var batches [][]claimedEvent
	var rejections []rejectionParam
	var current []claimedEvent
	currentBytes := 0

	for _, row := range rows {
		if len(row.raw) > maxSubBatchBytes {
			rejections = append(rejections, rejectionParam{
				ID:     row.id,
				Reason: reasonTooLarge,
			})
			continue
		}

		var ev hummingbee.Event
		if err := json.Unmarshal(row.raw, &ev); err != nil {
			rejections = append(rejections, rejectionParam{
				ID:     row.id,
				Reason: reasonInvalidJSON,
			})
			continue
		}

		item := claimedEvent{
			event: ev,
			id:    row.id,
			size:  len(row.raw),
		}
		if len(current) > 0 && currentBytes+item.size > maxSubBatchBytes {
			batches = append(batches, current)
			current = nil
			currentBytes = 0
		}
		current = append(current, item)
		currentBytes += item.size
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches, rejections
}

func classifyResults(batch []claimedEvent, results []hummingbee.Result) ([]int64, []rejectionParam, error) {
	if len(results) != len(batch) {
		return nil, nil, fmt.Errorf("sender returned %d results for %d events", len(results), len(batch))
	}

	seen := make([]bool, len(batch))
	var settledIDs []int64
	var rejections []rejectionParam

	for _, res := range results {
		if res.Index < 0 || res.Index >= len(batch) {
			return nil, nil, fmt.Errorf("sender returned result index %d out of range [0, %d)", res.Index, len(batch))
		}
		if seen[res.Index] {
			return nil, nil, fmt.Errorf("sender returned duplicate result index %d", res.Index)
		}
		seen[res.Index] = true

		row := batch[res.Index]
		switch res.Status {
		case hummingbee.StatusAccepted, hummingbee.StatusDuplicate:
			settledIDs = append(settledIDs, row.id)
		case hummingbee.StatusRejected:
			rejections = append(rejections, rejectionParam{
				Field:  res.Field,
				ID:     row.id,
				Reason: res.Reason,
			})
		default:
			return nil, nil, fmt.Errorf("sender returned unknown status %q for index %d", res.Status, res.Index)
		}
	}

	return settledIDs, rejections, nil
}

func deleteSettled(ctx context.Context, q querier, token string, ids []int64) error {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	if err := q.exec(ctx, deleteSettledSQL, token, strings.Join(parts, ",")); err != nil {
		return fmt.Errorf("failed to delete %d settled outbox rows: %w", len(ids), err)
	}
	return nil
}

func markRejected(ctx context.Context, q querier, token string, rejections []rejectionParam) error {
	payload, err := json.Marshal(rejections)
	if err != nil {
		return fmt.Errorf("failed to encode %d outbox rejections: %w", len(rejections), err)
	}
	if err := q.exec(ctx, markRejectedSQL, token, string(payload)); err != nil {
		return fmt.Errorf("failed to mark %d outbox rows rejected: %w", len(rejections), err)
	}
	return nil
}

func recordFailure(ctx context.Context, q querier, token string, sendErr error, backoff time.Duration, logger *slog.Logger) {
	backoffMicros := max(backoff.Microseconds(), 1)
	if err := q.exec(ctx, failRowsSQL, token, sendErr.Error(), backoffMicros); err != nil {
		if ctx.Err() != nil {
			releaseLeases(ctx, q, token, logger)
			return
		}
		logger.ErrorContext(ctx, "outbox relay failed to record send failure", slog.Any("err", err))
	}
}

func releaseLeases(ctx context.Context, q querier, token string, logger *slog.Logger) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownReleaseTimeout)
	defer cancel()
	if err := q.exec(releaseCtx, releaseLeasesSQL, token); err != nil {
		logger.ErrorContext(releaseCtx, "outbox relay failed to release leases on shutdown", slog.Any("err", err))
	}
}

func logRound(ctx context.Context, logger *slog.Logger, stats roundStats, err error) {
	if err != nil {
		logger.ErrorContext(ctx, "outbox relay round failed",
			slog.Int("sent", stats.sent),
			slog.Int("settled", stats.settled),
			slog.Int("rejected", stats.rejected),
			slog.Int("failed", stats.failed),
			slog.Any("err", err),
		)
		return
	}
	logger.InfoContext(ctx, "outbox relay round completed",
		slog.Int("sent", stats.sent),
		slog.Int("settled", stats.settled),
		slog.Int("rejected", stats.rejected),
		slog.Int("failed", stats.failed),
	)
}

func nextBackoff(pollInterval time.Duration, failures int) time.Duration {
	if pollInterval >= maxBackoff {
		return pollInterval
	}
	d := pollInterval
	for i := 1; i < failures; i++ {
		if d >= maxBackoff/2 {
			return maxBackoff
		}
		d *= 2
	}
	return d
}

func countRemaining(batches [][]claimedEvent) int {
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	return total
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func newLeaseToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails.
	return hex.EncodeToString(b[:])
}

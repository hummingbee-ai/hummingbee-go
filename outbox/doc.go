// Package outbox provides a transactional Postgres outbox for sending
// Hummingbee events reliably across one or more application replicas.
//
// Calling [hummingbee.Client.Send] directly inside a request can lose an event
// if the process crashes or Hummingbee is unreachable, or send an event for a
// change that later rolls back. The outbox avoids both: [Add] writes the event
// to the firm's own Postgres database inside the transaction that makes the
// change, and [Relay] sends committed rows in batches afterwards.
//
// This package is optional and brings pgx and GORM. Software that imports only
// package hummingbee compiles with the standard library alone.
//
// # Schema
//
// Apply [Schema] in the firm's own database migrations. It creates [Table]
// (hummingbee_outbox) and its partial index on pending rows using idempotent
// DDL, so applying it more than once is safe:
//
//	if _, err := db.ExecContext(ctx, outbox.Schema); err != nil {
//		return fmt.Errorf("failed to apply outbox schema: %w", err)
//	}
//
// # Adding events
//
// Call [Add] inside the transaction that records the business change. It
// accepts a *sql.Tx, a pgx.Tx or a *gorm.DB inside an active transaction,
// encodes the event as JSON, and inserts one row without opening a transaction
// or connection of its own:
//
//	event := hummingbee.Event{
//		Actor: hummingbee.System("order-sync"),
//		Changes: []hummingbee.Change{{
//			Field: "status", From: "submitted", To: "paid",
//		}},
//		ID:         "order-status-" + change.ID,
//		OccurredAt: change.CreatedAt,
//		Related: []hummingbee.Ref{
//			{ID: order.CustomerID, Kind: "customer"},
//		},
//		Subject: hummingbee.Ref{ID: order.ID, Kind: "order"},
//		Type:    "order.status_changed",
//	}
//
//	// database/sql (*sql.Tx), pgx (pgx.Tx) or GORM (*gorm.DB inside Transaction):
//	if err := outbox.Add(ctx, tx, event); err != nil {
//		return err
//	}
//
// Passing a *gorm.DB outside a transaction returns [ErrNotInTransaction].
// Passing nil or any other handle returns [ErrUnsupportedTx]. [Add] does not
// validate event fields; the server is the authority.
//
// # Relaying
//
// Start [Relay] once per application replica with a *sql.DB, *pgxpool.Pool or
// *gorm.DB and a [Sender] (satisfied by [*hummingbee.Client]). Any other
// database handle returns [ErrUnsupportedDB] immediately:
//
//	go func() {
//		if err := outbox.Relay(ctx, db, client); err != nil && !errors.Is(err, context.Canceled) {
//			slog.Error("outbox relay stopped", slog.Any("err", err))
//		}
//	}()
//
// Each round claims up to a batch of waiting rows in one autocommitted
// statement with FOR UPDATE SKIP LOCKED and a lease, so no transaction stays
// open during HTTP calls and several replicas never hold the same row at once.
// When ctx is cancelled, [Relay] abandons any in-flight request, releases the
// leases it still holds so another replica can pick them up immediately, and
// returns ctx.Err().
//
// # Options
//
// [Relay] accepts [WithBatchSize] (1 to 500, default 500), [WithLease]
// (default 5 minutes), [WithPollInterval] (default 1 second) and [WithLogger]
// (default [slog.Default]). Logs include counts and errors only, never event
// values.
//
// # Delivery semantics and scope
//
// Claimed rows are split into sub-batches under 1 MiB to stay beneath the 2 MiB
// wire limit. Accepted and duplicate rows are deleted. Rejected rows stay in
// [Table] with rejected_at, rejected_field and rejected_reason populated and
// are never claimed again; a single event whose JSON exceeds 1 MiB is rejected
// locally with reason "too_large". Failed requests keep rows with last_error
// set and push leased_until forward with exponential backoff up to one minute.
//
// This package supports Postgres 10 or later only, uses polling rather than
// LISTEN/NOTIFY, makes no ordering guarantee across replicas, places no unique
// constraint on event_id, and does not automatically purge or replay rejected
// rows.
package outbox

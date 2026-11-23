# hummingbee-go

The Go client for Hummingbee's events endpoint. A firm's software sends an event the moment something changes, such as an order's status, and Hummingbee uses it to open or close tasks.

- `hummingbee`: the event types and the client. `Send` posts a batch of up to 500 events with gzip, retries and backoff, and reports each event as accepted, a duplicate or rejected with a reason.
- `hummingbeetest`: a fake events server for a firm's own tests, and a dry-run HTTP client that logs events without sending them.
- `outbox`: an optional transactional Postgres outbox. `Add` writes an event inside the firm's own database transaction, and `Relay` sends waiting rows in batches across one or more replicas.

`hummingbee` and `hummingbeetest` import the standard library only. `outbox` is optional and brings pgx and GORM; software that imports only `hummingbee` never compiles them.

## Install

```sh
go get github.com/hummingbee-ai/hummingbee-go
```

The module supports the two Go releases Go itself supports, currently 1.26 and 1.27.

## Quick start

```go
import "github.com/hummingbee-ai/hummingbee-go"

// Once, at start-up. The token is the sending system's own key.
client := hummingbee.New(token, hummingbee.WithOrganizationID(orgID))

// Fields in alphabetical order. Ids in refs are UUIDs.
results, err := client.Send(ctx, []hummingbee.Event{{
	Actor: hummingbee.System("order-sync"),
	Changes: []hummingbee.Change{{
		Field: "status", From: "submitted", To: "paid",
	}},
	ID:         "order-status-" + change.ID,
	OccurredAt: change.CreatedAt,
	Related: []hummingbee.Ref{
		{ID: order.CustomerID, Kind: "customer"},
	},
	Subject: hummingbee.Ref{ID: order.ID, Kind: "order"},
	Type:    "order.status_changed",
}})
if err != nil {
	return fmt.Errorf("failed to send order %s event: %w", order.ID, err)
}
```

`hummingbee.User(id)` names a person as the actor instead of a system. `example_test.go` runs this quick start against `hummingbeetest`, so `go test` proves it.

### Options

| Option | Default |
| --- | --- |
| `WithBaseURL(url)` | `https://api.hummingbee.ai` |
| `WithHTTPClient(client)` | an `*http.Client` with a 30 second timeout per attempt |
| `WithMaxAttempts(n)` | 5 |
| `WithOrganizationID(id)` | none: the key's own organisation |

## Results

`Send` returns one `Result` per event, in the order given, with the event's `ID` and `Index`.

| `Status` | Meaning | What to do |
| --- | --- | --- |
| `accepted` | Hummingbee kept the event. | Nothing. |
| `duplicate` | Hummingbee already had an event with this id, and kept the first copy. | Nothing. A retry or a restart sent it again. |
| `rejected` | The event broke a rule. `Field` names the field and `Reason` the rule. | Fix the code that builds it. Sending the same event again is rejected again. |

`Result.IsSettled()` is true for `accepted` and `duplicate`: the event never needs sending again.

## Retries and errors

One `Send` is one request. More than 500 events return `ErrTooManyEvents` before any request, and an empty batch sends nothing.

Transport errors and the statuses `408`, `429`, `500`, `502`, `503` and `504` are retried, up to the attempt limit. Each wait is drawn at random from zero to a step that starts at 500 ms and doubles up to 30 s. A `Retry-After` header, in seconds or as an HTTP date, replaces the drawn wait, up to 60 s. `Send` never waits past the context's deadline: when the next wait would pass it, `Send` returns the last error at once. A cancelled context is never retried.

Every other failure returns at once. A response from the endpoint that is not a success comes back as an `*APIError` with the `StatusCode`, the server's error `Code` and its `Message`:

```go
var apiErr *hummingbee.APIError
if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusUnauthorized {
	// The key is wrong or revoked.
}
```

The client never logs, and the token never appears in an error or in the client's `String()`.

## Outbox

Sending directly inside a request can lose an event if the process stops or Hummingbee is unreachable, or send an event for a change that then rolls back. Package `github.com/hummingbee-ai/hummingbee-go/outbox` provides a transactional Postgres outbox: `outbox.Add` writes the event inside the transaction that makes the change, and `outbox.Relay` sends committed rows afterwards in batches of up to 500.

### 1. Apply the schema

`outbox.Schema` holds the idempotent Postgres DDL that creates `outbox.Table` (`hummingbee_outbox`) and its partial index on pending rows (`WHERE rejected_at IS NULL`). There is no built-in migration runner: apply `outbox.Schema` in your own database migrations. Running it more than once is safe:

```go
import "github.com/hummingbee-ai/hummingbee-go/outbox"

if _, err := db.ExecContext(ctx, outbox.Schema); err != nil {
	return fmt.Errorf("failed to apply outbox schema: %w", err)
}
```

### 2. Add events inside a transaction

`outbox.Add(ctx, tx, event)` encodes `event` as JSON and inserts one row into `hummingbee_outbox` through `tx`, so the event commits or rolls back with the change. It never opens a transaction or connection of its own and does not validate event fields: the server is the authority.

```go
event := hummingbee.Event{
	Actor: hummingbee.System("order-sync"),
	Changes: []hummingbee.Change{{
		Field: "status", From: "submitted", To: "paid",
	}},
	ID:         "order-status-" + change.ID,
	OccurredAt: change.CreatedAt,
	Related: []hummingbee.Ref{
		{ID: order.CustomerID, Kind: "customer"},
	},
	Subject: hummingbee.Ref{ID: order.ID, Kind: "order"},
	Type:    "order.status_changed",
}
```

`Add` accepts all three Postgres transaction kinds:

- **`database/sql` (`*sql.Tx`)**:

  ```go
  tx, err := db.BeginTx(ctx, nil)
  if err != nil {
  	return err
  }
  defer tx.Rollback()

  // ... update the order using tx ...

  if err := outbox.Add(ctx, tx, event); err != nil {
  	return err
  }
  return tx.Commit()
  ```

- **`pgx` (`pgx.Tx`, including transactions from `*pgxpool.Pool`)**:

  ```go
  tx, err := pool.Begin(ctx)
  if err != nil {
  	return err
  }
  defer tx.Rollback(ctx)

  // ... update the order using tx ...

  if err := outbox.Add(ctx, tx, event); err != nil {
  	return err
  }
  return tx.Commit(ctx)
  ```

- **GORM (`*gorm.DB` inside `Transaction`)**:

  ```go
  err := gormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
  	// ... update the order using tx ...

  	return outbox.Add(ctx, tx, event)
  })
  ```

Passing a `*gorm.DB` outside a transaction returns `outbox.ErrNotInTransaction`. Passing `nil` or any other handle (such as `*sql.DB` or `*pgxpool.Pool`) returns `outbox.ErrUnsupportedTx`.

### 3. Run the relay once per replica

Start `outbox.Relay(ctx, db, client, options...)` once per application replica. `db` may be a `*sql.DB`, a `*pgxpool.Pool` or a `*gorm.DB`, and `sender` is an `outbox.Sender` (which `*hummingbee.Client` satisfies). Any other database handle returns `outbox.ErrUnsupportedDB` at once.

```go
go func() {
	err := outbox.Relay(ctx, db, client,
		outbox.WithLogger(slog.Default()),
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("outbox relay stopped", slog.Any("err", err))
	}
}()
```

Each round claims waiting rows in one autocommitted statement with `FOR UPDATE SKIP LOCKED`, a random lease token and a database-clock lease, so no transaction stays open during HTTP calls and several replicas never hold the same row at once.

`Relay` runs until `ctx` is cancelled. On shutdown it abandons any in-flight request, releases the leases it still holds on a short detached timeout (two seconds) so another replica can send those rows at once, and returns `ctx.Err()`.

### Options

| Option | Default | Notes |
| --- | --- | --- |
| `WithBatchSize(n)` | `500` (`hummingbee.MaxBatch`) | Maximum rows claimed per round, clamped to `1`..`500`. |
| `WithLease(d)` | `5m` | How long a claimed batch is leased before another relay may reclaim it. Each `Send` times out at four fifths of the lease. Non-positive values are ignored. |
| `WithPollInterval(d)` | `1s` | Wait between polls when a claim returned fewer than a full batch; a full batch claims again at once. Non-positive values are ignored. |
| `WithLogger(logger)` | `slog.Default()` | Logs per-round counts and errors only, never event values. A `nil` logger is ignored. |

### Delivery semantics

Claimed rows are split into sub-batches of at most 1 MiB of event JSON to stay beneath the endpoint's 2 MiB wire limit. All deletes and updates match the claim's `lease_token`, so a relay whose lease expired never overwrites a row reclaimed by another replica.

- **Accepted and duplicate**: rows whose result is `accepted` or `duplicate` are deleted from `hummingbee_outbox`.
- **Rejected**: rows rejected by Hummingbee stay in `hummingbee_outbox` with `rejected_at`, `rejected_field` and `rejected_reason` set and are never claimed again. An event whose encoded JSON exceeds 1 MiB on its own is rejected locally with `rejected_reason = 'too_large'` without being sent.
- **Failed requests**: when `Send` returns an error (after the client's own retries, or on a non-retryable error such as `401`), every un-settled row stays in the table with `last_error` set, its lease token cleared and `leased_until` pushed forward with exponential backoff (starting at the poll interval and doubling per consecutive failed round up to one minute, resetting on success). The relay logs the failure and keeps running.

To inspect rejected rows:

```sql
SELECT id, event_id, attempts, created_at, rejected_at, rejected_field, rejected_reason
FROM hummingbee_outbox
WHERE rejected_at IS NOT NULL
ORDER BY rejected_at DESC;
```

### Scope limits

- **Postgres 10+ only**: uses `GENERATED ALWAYS AS IDENTITY` and `FOR UPDATE SKIP LOCKED`. There is no MySQL or SQLite adapter.
- **Polling, no `LISTEN`/`NOTIFY`**: the relay polls on `WithPollInterval` when idle and claims again immediately after a full batch.
- **No ordering guarantee across replicas**: events carry `occurred_at`, and Hummingbee keeps both event time and arrival time.
- **No unique constraint on `event_id`**: adding the same `event_id` twice never fails the firm's transaction; Hummingbee keeps the first copy and answers `duplicate` for later ones, which the relay deletes.
- **No automatic purge or replay of rejected rows**: read them with SQL above and delete or fix them manually.

## Testing with hummingbeetest

`hummingbeetest.NewServer()` starts a fake events endpoint with its own token and memory. It checks requests as the real endpoint does, applies the per-event rules of the wire format below, keeps the first copy of each event id and records what it accepted.

```go
server := hummingbeetest.NewServer()
defer server.Close()

client := server.Client() // a *hummingbee.Client pointed at the server, with its token
results, err := client.Send(ctx, events)

server.Events()   // the accepted events, in arrival order
server.Requests() // every request, including failed ones
```

To test failure handling:

- `server.FailNext(times, status)` answers the next `times` requests with that status and an error envelope, with `Retry-After: 0`, so the client retries at once.
- `server.RejectID(id, field, reason)` rejects that event id from then on.
- `server.Reset()` forgets everything but the token.

Every method is safe to call from several goroutines.

## The dry run

`hummingbeetest.DryRun(logger)` returns an `*http.Client` that sends nothing. It logs one line per event, with the event's id, type and subject kind but never its values, and answers every event `accepted`.

```go
client := hummingbee.New(token, hummingbee.WithHTTPClient(hummingbeetest.DryRun(slog.Default())))
```

## Wire format v1

Software in other languages can post the same JSON. The format changes only with a new version of the endpoint.

**Request.** `POST https://api.hummingbee.ai/v1/events` with `Authorization: Bearer <key>`, `Content-Type: application/json` and, optionally, `Content-Encoding: gzip`. The body is at most 2 MiB:

```json
{
  "events": [
    {
      "actor": {"kind": "system", "name": "order-sync"},
      "changes": [{"field": "status", "from": "submitted", "to": "paid"}],
      "id": "order-status-0193a0b2-7c1d-7e3f-9a4b-5c6d7e8f9a0b",
      "occurred_at": "2026-11-17T09:30:00Z",
      "related": [{"id": "0193a0b2-7c1d-7e3f-9a4b-000000000001", "kind": "customer"}],
      "subject": {"id": "0193a0b2-7c1d-7e3f-9a4b-000000000002", "kind": "order"},
      "type": "order.status_changed"
    }
  ],
  "organization_id": "0193a0b2-7c1d-7e3f-9a4b-000000000003"
}
```

`organization_id` is optional. Each event has:

| Field | Required | Rule |
| --- | --- | --- |
| `actor` | no | `{"kind": "system", "name": …}` or `{"kind": "user", "id": "<uuid>"}` |
| `changes` | no | at most 100, each `{"field", "from", "to"}`, where `from` and `to` are a string of at most 1,000 characters, a number, a boolean or `null` |
| `id` | yes | the firm's own id: 1 to 200 of `A-Z a-z 0-9 . _ : -`, starting with a letter or digit |
| `occurred_at` | yes | RFC 3339, at most five minutes ahead |
| `related` | no | at most 50 refs |
| `subject` | yes | a ref |
| `type` | yes | dotted lower case, such as `order.status_changed`, at most 100 characters |

A ref is `{"id": "<uuid>", "kind": …}`.

**Response.** `200` with one result per event, in order. `field` and `reason` are present only on `rejected`:

```json
{
  "results": [
    {"id": "order-status-1", "index": 0, "status": "accepted"},
    {"id": "order-status-1", "index": 1, "status": "duplicate"},
    {"field": "subject.id", "id": "order-status-2", "index": 2, "reason": "invalid_uuid", "status": "rejected"}
  ]
}
```

| Reason | Rule |
| --- | --- |
| `required` | a required field is missing |
| `invalid_uuid` | `subject.id`, a related ref's `id` or a user actor's `id` is not a UUID |
| `invalid_id` | `id` is outside its character set or length, or does not start with a letter or digit |
| `invalid_type` | `type` is not dotted lower case, or is over 100 characters |
| `in_future` | `occurred_at` is more than five minutes ahead |
| `too_many_changes` | more than 100 changes |
| `too_many_related` | more than 50 related refs |
| `invalid_change_value` | a change value is not a scalar or `null`: an object, an array, or a string over 1,000 characters |
| `invalid_actor` | an actor kind other than `system` or `user` |
| `not_an_object` | an entry of `events` is not a JSON object; its result has an empty id |

**Errors.** A request that fails as a whole answers `{"error": {"code": …, "message": …}}`:

| Status | Code |
| --- | --- |
| `400` | `BAD_REQUEST` |
| `401` | `UNAUTHENTICATED`, with `WWW-Authenticate: Bearer` |
| `403` | `FORBIDDEN` |
| `413` | `PAYLOAD_TOO_LARGE` |
| `415` | `UNSUPPORTED_MEDIA_TYPE` |
| `500` | `INTERNAL` |

Retry `408`, `429` and `5xx`, honouring `Retry-After`. Retrying is always safe, since the endpoint keeps the first copy of each event id.

## Licence

MIT. See [LICENSE](LICENSE).

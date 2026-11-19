# hummingbee-go

The Go client for Hummingbee's events endpoint. A firm's software sends an event the moment something changes, such as an order's status, and Hummingbee uses it to open or close tasks.

- `hummingbee`: the event types and the client. `Send` posts a batch of up to 500 events with gzip, retries and backoff, and reports each event as accepted, a duplicate or rejected with a reason.
- `hummingbeetest`: a fake events server for a firm's own tests, and a dry-run HTTP client that logs events without sending them.

Both packages import the standard library only.

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

# hummingbee-go

The public Go client for Hummingbee's events endpoint, `POST /v1/events`. A firm's software builds `hummingbee.Event` values and sends them in batches directly or through the optional transactional Postgres `outbox`; its tests send them to the fake server in `hummingbeetest`. `README.md` is the user's guide and holds the wire format.

## Layout

```
compose.yaml     Postgres 14 on port 5434 for make db-up and make test-db
doc.go           package documentation and the quick start
event.go         Event, Change, Ref and Actor, and System and User, which build actors
client.go        Client, New and the options
send.go          Send, the request and response shapes, and Result
retry.go         which failures retry, the backoff and Retry-After
errors.go        APIError and ErrTooManyEvents
version.go       the User-Agent, with the module version from the build info
export_test.go   the hooks the specs use to replace the clock, the jitter and the wait
example_test.go  runnable examples, so go test proves the README's quick start
hummingbeetest/  NewServer, the fake events endpoint, and DryRun, the HTTP client that sends nothing
outbox/          optional transactional Postgres outbox (pgx v5 and GORM)
  doc.go                 package documentation and quick start
  schema.go              Table and Schema (idempotent Postgres DDL)
  outbox.go              Add, ErrNotInTransaction and ErrUnsupportedTx
  relay.go               Relay, Sender, options, ErrUnsupportedDB, claim, settle and backoff
  outbox_suite_test.go   throwaway Postgres database and CI skip/fail rule
  outbox_test.go         Add and rollback specs
  relay_test.go          relay, lease, batching, concurrency and done-when specs
```

## Commands

| Command | What it does |
| --- | --- |
| `make check` | `make check-fmt`, `make vet`, `make lint`, `make check-tidy`, `make check-stdlib` and `make test`. **The only quality gate.** CI runs it on Go 1.26 and 1.27 against a Postgres 14 service with `HUMMINGBEE_TEST_DATABASE_URL` set |
| `make lint` | golangci-lint at the pinned version (`GOLANGCI_LINT_VERSION`), with `.golangci.yml`. It refuses any other version |
| `make check-tidy` | Fail if `go mod tidy` would change `go.mod` or `go.sum` |
| `make check-stdlib` | Fail, naming them, if `hummingbee` or `hummingbeetest` import anything outside the standard library and this module. Test files do not count |
| `make test` | The specs, with the race detector (outbox database specs skip unless `HUMMINGBEE_TEST_DATABASE_URL` is set, and fail if `CI` is set without it) |
| `make db-up` | Start the local Postgres 14 on port 5434 (`compose.yaml`) |
| `make db-down` | Stop the local Postgres, keeping its data |
| `make test-db` | Run the specs against Postgres (`TEST_PKGS` defaults to `./...`; `GINKGO_LABEL_FILTER` optional), pointing `HUMMINGBEE_TEST_DATABASE_URL` at `make db-up`'s Postgres unless already set |
| `make fix` | `gofmt -w .` |
| `make setup-worktree` | Copy `.env` from the main checkout and download Go modules into a worktree |

`HUMMINGBEE_TEST_DATABASE_URL` names a Postgres server whose login can create and drop databases. The outbox suite creates a throwaway database for each run, applies `outbox.Schema`, truncates `hummingbee_outbox` between specs and drops the database at the end.

## Conventions

- Standard library only in `hummingbee` and `hummingbeetest`. A firm imports them, so they must bring no dependencies. `make check-stdlib` is the one enforcement point. `outbox` is the only package allowed pgx (`github.com/jackc/pgx/v5`) and GORM (`gorm.io/gorm`); Ginkgo, Gomega and `gorm.io/driver/postgres` are test-only.
- The wire format follows Hummingbee's `/v1/events`. It changes only with a new version of the endpoint, and `README.md`'s "Wire format v1" is the one place it is written down. The fake server's rules and reason strings must match it.
- Exported names are a contract with every importer. After the first tag, changes follow semantic versioning.
- The client validates nothing the server checks: the rules live on the server, and in `hummingbeetest` for tests. The client never logs, and the token never appears in an error, a log line or a `String()`.
- Fields in structs in alphabetical order. American spelling in code (`organization`), British in prose.
- Errors wrapped with context (`failed to send 3 events: …`).
- Specs use Ginkgo v2 and Gomega. Retry specs never really sleep: they replace the clock, jitter and wait through `export_test.go`.
- Every exported name has a doc comment. Comments explain a non-obvious why; they don't narrate the code.
- Workflows: every action pinned by full commit SHA with a `# vX.Y.Z` comment, a top-level `permissions: contents: read`, `timeout-minutes` on every job and `persist-credentials: false` on every `actions/checkout`.
- Commit messages are `type(scope): subject`, with a scope.

## What the repository never names

Nothing here names a customer, a person, or who owns Hummingbee: not in code, examples, docs or commit messages. Examples use a neutral domain, an order whose status changed. The licence holder is "The Hummingbee authors".

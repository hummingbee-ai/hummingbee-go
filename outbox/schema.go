package outbox

// Table is the name of the Postgres table that holds queued and rejected
// outbox events.
const Table = "hummingbee_outbox"

// Schema holds the idempotent Postgres DDL that creates [Table] and the partial
// index the relay uses to claim waiting rows.
const Schema = `
CREATE TABLE IF NOT EXISTS hummingbee_outbox (
	id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	attempts        integer     NOT NULL DEFAULT 0,
	created_at      timestamptz NOT NULL DEFAULT now(),
	event           jsonb       NOT NULL,
	event_id        text        NOT NULL,
	last_error      text,
	lease_token     text,
	leased_until    timestamptz,
	rejected_at     timestamptz,
	rejected_field  text,
	rejected_reason text
);

CREATE INDEX IF NOT EXISTS hummingbee_outbox_pending_idx
	ON hummingbee_outbox (id)
	WHERE rejected_at IS NULL;
`

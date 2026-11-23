package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"gorm.io/gorm"

	"github.com/hummingbee-ai/hummingbee-go"
)

const insertEventSQL = "INSERT INTO " + Table + " (event, event_id) VALUES ($1, $2)"

// ErrNotInTransaction is returned by [Add] when given a [*gorm.DB] that is not
// inside an active transaction.
var ErrNotInTransaction = errors.New("outbox: gorm.DB is not in a transaction")

// ErrUnsupportedTx is returned by [Add] when given a handle that is nil or not
// one of *sql.Tx, pgx.Tx or *gorm.DB.
var ErrUnsupportedTx = errors.New("outbox: unsupported transaction type")

// Add encodes event as JSON and inserts one row into [Table] through tx,
// without opening a transaction or connection of its own.
//
// tx must be an active *sql.Tx, pgx.Tx or *gorm.DB inside a transaction. A
// *gorm.DB outside a transaction returns [ErrNotInTransaction]; nil or any
// other handle returns [ErrUnsupportedTx]. Add does not validate event fields,
// because the server is the authority.
func Add(ctx context.Context, tx any, event hummingbee.Event) error {
	exec, err := resolveTx(tx)
	if err != nil {
		return fmt.Errorf("failed to add event %s to the outbox: %w", event.ID, err)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to add event %s to the outbox: %w", event.ID, err)
	}

	if err := exec(ctx, payload, event.ID); err != nil {
		return fmt.Errorf("failed to add event %s to the outbox: %w", event.ID, err)
	}
	return nil
}

func resolveTx(tx any) (func(ctx context.Context, payload []byte, eventID string) error, error) {
	switch handle := tx.(type) {
	case *sql.Tx:
		if handle == nil {
			return nil, fmt.Errorf("%w: nil *sql.Tx", ErrUnsupportedTx)
		}
		return func(ctx context.Context, payload []byte, eventID string) error {
			_, err := handle.ExecContext(ctx, insertEventSQL, payload, eventID)
			return err
		}, nil

	case pgx.Tx:
		if isTypedNil(handle) {
			return nil, fmt.Errorf("%w: nil %T", ErrUnsupportedTx, handle)
		}
		return func(ctx context.Context, payload []byte, eventID string) error {
			_, err := handle.Exec(ctx, insertEventSQL, payload, eventID)
			return err
		}, nil

	case *gorm.DB:
		if handle == nil {
			return nil, fmt.Errorf("%w: nil *gorm.DB", ErrUnsupportedTx)
		}
		if handle.Statement == nil || handle.Statement.ConnPool == nil || isTypedNil(handle.Statement.ConnPool) {
			return nil, ErrNotInTransaction
		}
		pool := handle.Statement.ConnPool
		if _, ok := pool.(gorm.TxCommitter); !ok {
			return nil, ErrNotInTransaction
		}
		return func(ctx context.Context, payload []byte, eventID string) error {
			_, err := pool.ExecContext(ctx, insertEventSQL, payload, eventID)
			return err
		}, nil

	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedTx, tx)
	}
}

func isTypedNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	k := rv.Kind()
	if k == reflect.Pointer || k == reflect.Interface || k == reflect.Map || k == reflect.Slice || k == reflect.Func || k == reflect.Chan {
		return rv.IsNil()
	}
	return false
}

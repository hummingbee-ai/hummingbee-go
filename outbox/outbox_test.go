package outbox_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/gorm"

	"github.com/hummingbee-ai/hummingbee-go"
	"github.com/hummingbee-ai/hummingbee-go/outbox"
)

type storedRow struct {
	Event   hummingbee.Event
	EventID string
}

func sampleEvent(id string) hummingbee.Event {
	return hummingbee.Event{
		Actor: hummingbee.System("order-sync"),
		Changes: []hummingbee.Change{
			{Field: "status", From: "pending", To: "shipped"},
		},
		ID:         id,
		OccurredAt: time.Date(2026, 11, 20, 9, 30, 0, 0, time.UTC),
		Related: []hummingbee.Ref{
			{ID: "22222222-2222-4222-8222-222222222222", Kind: "customer"},
		},
		Subject: hummingbee.Ref{ID: "11111111-1111-4111-8111-111111111111", Kind: "order"},
		Type:    "order.status_changed",
	}
}

func readOutboxRows(ctx context.Context) []storedRow {
	rows, err := testPool.Query(ctx, "SELECT event, event_id FROM "+outbox.Table+" ORDER BY id")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer rows.Close()

	var result []storedRow
	for rows.Next() {
		var raw []byte
		var eventID string
		ExpectWithOffset(1, rows.Scan(&raw, &eventID)).To(Succeed())

		var ev hummingbee.Event
		ExpectWithOffset(1, json.Unmarshal(raw, &ev)).To(Succeed())
		result = append(result, storedRow{
			Event:   ev,
			EventID: eventID,
		})
	}
	ExpectWithOffset(1, rows.Err()).NotTo(HaveOccurred())
	return result
}

var _ = Describe("Add", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	})

	AfterEach(func() {
		cancel()
	})

	Context("with a *sql.Tx", func() {
		It("commits one row whose event decodes equal to the added event", func() {
			want := sampleEvent("order-sql-1")

			tx, err := testSQLDB.BeginTx(ctx, nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(outbox.Add(ctx, tx, want)).To(Succeed())
			Expect(tx.Commit()).To(Succeed())

			rows := readOutboxRows(ctx)
			Expect(rows).To(HaveLen(1))
			Expect(rows[0].EventID).To(Equal(want.ID))
			Expect(rows[0].Event).To(Equal(want))
		})

		It("writes no row when rolled back", func() {
			tx, err := testSQLDB.BeginTx(ctx, nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(outbox.Add(ctx, tx, sampleEvent("order-sql-rollback"))).To(Succeed())
			Expect(tx.Rollback()).To(Succeed())

			Expect(readOutboxRows(ctx)).To(BeEmpty())
		})
	})

	Context("with a pgx.Tx", func() {
		It("commits one row whose event decodes equal to the added event", func() {
			want := sampleEvent("order-pgx-1")

			tx, err := testPool.Begin(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(outbox.Add(ctx, tx, want)).To(Succeed())
			Expect(tx.Commit(ctx)).To(Succeed())

			rows := readOutboxRows(ctx)
			Expect(rows).To(HaveLen(1))
			Expect(rows[0].EventID).To(Equal(want.ID))
			Expect(rows[0].Event).To(Equal(want))
		})

		It("writes no row when rolled back", func() {
			tx, err := testPool.Begin(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(outbox.Add(ctx, tx, sampleEvent("order-pgx-rollback"))).To(Succeed())
			Expect(tx.Rollback(ctx)).To(Succeed())

			Expect(readOutboxRows(ctx)).To(BeEmpty())
		})
	})

	Context("with a *gorm.DB", func() {
		It("commits one row inside Transaction", func() {
			want := sampleEvent("order-gorm-1")

			err := testGormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				return outbox.Add(ctx, tx, want)
			})
			Expect(err).NotTo(HaveOccurred())

			rows := readOutboxRows(ctx)
			Expect(rows).To(HaveLen(1))
			Expect(rows[0].EventID).To(Equal(want.ID))
			Expect(rows[0].Event).To(Equal(want))
		})

		It("commits one row inside Transaction when PrepareStmt is enabled", func() {
			want := sampleEvent("order-gorm-prepared-1")
			preparedDB := testGormDB.Session(&gorm.Session{PrepareStmt: true})

			err := preparedDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				return outbox.Add(ctx, tx, want)
			})
			Expect(err).NotTo(HaveOccurred())

			rows := readOutboxRows(ctx)
			Expect(rows).To(HaveLen(1))
			Expect(rows[0].EventID).To(Equal(want.ID))
			Expect(rows[0].Event).To(Equal(want))
		})

		It("writes no row when Transaction rolls back", func() {
			rollbackErr := errors.New("abort transaction")
			err := testGormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				Expect(outbox.Add(ctx, tx, sampleEvent("order-gorm-rollback"))).To(Succeed())
				return rollbackErr
			})
			Expect(errors.Is(err, rollbackErr)).To(BeTrue())

			Expect(readOutboxRows(ctx)).To(BeEmpty())
		})

		It("returns ErrNotInTransaction outside a transaction and writes nothing", func() {
			err := outbox.Add(ctx, testGormDB, sampleEvent("order-gorm-no-tx"))
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, outbox.ErrNotInTransaction)).To(BeTrue())

			preparedDB := testGormDB.Session(&gorm.Session{PrepareStmt: true})
			err = outbox.Add(ctx, preparedDB, sampleEvent("order-gorm-prepared-no-tx"))
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, outbox.ErrNotInTransaction)).To(BeTrue())

			err = outbox.Add(ctx, &gorm.DB{}, sampleEvent("order-gorm-empty"))
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, outbox.ErrNotInTransaction)).To(BeTrue())

			Expect(readOutboxRows(ctx)).To(BeEmpty())
		})
	})

	Context("with unsupported handles", func() {
		It("returns ErrUnsupportedTx for non-transaction handles and typed nils", func() {
			var nilSQLTx *sql.Tx
			var nilGormDB *gorm.DB
			var nilPGXTx *pgxpool.Tx
			var nilInterfacePGXTx pgx.Tx

			handles := []any{
				testSQLDB,
				testPool,
				nil,
				nilSQLTx,
				nilGormDB,
				nilPGXTx,
				nilInterfacePGXTx,
				"not-a-tx",
			}

			for _, h := range handles {
				err := outbox.Add(ctx, h, sampleEvent("order-unsupported"))
				Expect(err).To(HaveOccurred())
				Expect(errors.Is(err, outbox.ErrUnsupportedTx)).To(BeTrue(), "expected ErrUnsupportedTx for %T, got %v", h, err)
			}

			Expect(readOutboxRows(ctx)).To(BeEmpty())
		})
	})

	Context("encoding and duplicates", func() {
		It("rejects an unencodable event without poisoning the transaction", func() {
			bad := sampleEvent("order-bad-json")
			bad.Changes = []hummingbee.Change{
				{Field: "broken", From: nil, To: make(chan int)},
			}
			good := sampleEvent("order-good-after-bad")

			tx, err := testPool.Begin(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = outbox.Add(ctx, tx, bad)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(bad.ID))

			Expect(outbox.Add(ctx, tx, good)).To(Succeed())
			Expect(tx.Commit(ctx)).To(Succeed())

			rows := readOutboxRows(ctx)
			Expect(rows).To(HaveLen(1))
			Expect(rows[0].EventID).To(Equal(good.ID))
			Expect(rows[0].Event).To(Equal(good))
		})

		It("commits two rows with no error when the same event id is added twice", func() {
			first := sampleEvent("order-dup-1")
			second := sampleEvent("order-dup-1")

			tx, err := testSQLDB.BeginTx(ctx, nil)
			Expect(err).NotTo(HaveOccurred())

			Expect(outbox.Add(ctx, tx, first)).To(Succeed())
			Expect(outbox.Add(ctx, tx, second)).To(Succeed())
			Expect(tx.Commit()).To(Succeed())

			rows := readOutboxRows(ctx)
			Expect(rows).To(HaveLen(2))
			Expect(rows[0].EventID).To(Equal("order-dup-1"))
			Expect(rows[1].EventID).To(Equal("order-dup-1"))
		})

		It("does not validate event fields locally", func() {
			unvalidated := hummingbee.Event{
				ID:      "order-unvalidated",
				Subject: hummingbee.Ref{ID: "not-a-uuid", Kind: "order"},
				Type:    "",
			}

			tx, err := testPool.Begin(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(outbox.Add(ctx, tx, unvalidated)).To(Succeed())
			Expect(tx.Commit(ctx)).To(Succeed())

			rows := readOutboxRows(ctx)
			Expect(rows).To(HaveLen(1))
			Expect(rows[0].EventID).To(Equal("order-unvalidated"))
			Expect(rows[0].Event).To(Equal(unvalidated))
		})
	})
})

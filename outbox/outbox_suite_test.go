package outbox_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/hummingbee-ai/hummingbee-go/outbox"
)

const (
	testDatabaseURLVar = "HUMMINGBEE_TEST_DATABASE_URL"
	missingDBMessage   = testDatabaseURLVar + " is not set, so the outbox database specs did not run; run them with make test-db"
)

var (
	testServerURL   string
	testDatabaseURL string
	testDBName      string
	testPool        *pgxpool.Pool
	testSQLDB       *sql.DB
	testGormDB      *gorm.DB
)

func TestOutbox(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "outbox")
}

var _ = BeforeSuite(func() {
	serverURL := os.Getenv(testDatabaseURLVar)
	if serverURL == "" {
		if os.Getenv("CI") != "" {
			Fail(missingDBMessage)
		}
		Skip(missingDBMessage)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testServerURL = serverURL
	dbName := newTestDatabaseName()
	dbURL, err := createTestDatabase(ctx, testServerURL, dbName)
	Expect(err).NotTo(HaveOccurred())
	testDBName = dbName
	testDatabaseURL = dbURL

	pool, err := pgxpool.New(ctx, testDatabaseURL)
	Expect(err).NotTo(HaveOccurred())
	testPool = pool

	_, err = testPool.Exec(ctx, outbox.Schema)
	Expect(err).NotTo(HaveOccurred())

	sqlDB, err := sql.Open("pgx", testDatabaseURL)
	Expect(err).NotTo(HaveOccurred())
	Expect(sqlDB.PingContext(ctx)).To(Succeed())
	testSQLDB = sqlDB

	gormDB, err := gorm.Open(postgres.New(postgres.Config{DSN: testDatabaseURL}), &gorm.Config{})
	Expect(err).NotTo(HaveOccurred())
	testGormDB = gormDB
})

var _ = BeforeEach(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := testPool.Exec(ctx, "TRUNCATE TABLE "+outbox.Table+" RESTART IDENTITY")
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	var closeErrs []error

	if testGormDB != nil {
		sqlDB, err := testGormDB.DB()
		if err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("get gorm sql.DB: %w", err))
		} else if err := sqlDB.Close(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("close gorm sql.DB: %w", err))
		}
	}
	if testSQLDB != nil {
		if err := testSQLDB.Close(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("close sql.DB: %w", err))
		}
	}
	if testPool != nil {
		testPool.Close()
	}

	if testServerURL != "" && testDBName != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := dropTestDatabase(ctx, testServerURL, testDBName); err != nil {
			closeErrs = append(closeErrs, err)
		}
	}

	Expect(errors.Join(closeErrs...)).NotTo(HaveOccurred())
})

var _ = Describe("Schema", func() {
	It("applies twice without error and creates the expected table and partial index", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := testPool.Exec(ctx, outbox.Schema)
		Expect(err).NotTo(HaveOccurred())

		rows, err := testPool.Query(ctx, `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1
			ORDER BY ordinal_position
		`, outbox.Table)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		columns, err := pgx.CollectRows(rows, pgx.RowTo[string])
		Expect(err).NotTo(HaveOccurred())
		Expect(columns).To(Equal([]string{
			"id",
			"attempts",
			"created_at",
			"event",
			"event_id",
			"last_error",
			"lease_token",
			"leased_until",
			"rejected_at",
			"rejected_field",
			"rejected_reason",
		}))

		var indexDef string
		err = testPool.QueryRow(ctx, `
			SELECT indexdef
			FROM pg_indexes
			WHERE schemaname = 'public'
			  AND tablename = $1
			  AND indexdef LIKE '%WHERE (rejected_at IS NULL)%'
		`, outbox.Table).Scan(&indexDef)
		Expect(err).NotTo(HaveOccurred())
		Expect(indexDef).To(ContainSubstring("(id)"))

		var sqlCount int
		Expect(testSQLDB.QueryRowContext(ctx, "SELECT count(*) FROM "+outbox.Table).Scan(&sqlCount)).To(Succeed())
		Expect(sqlCount).To(Equal(0))

		var gormCount int64
		Expect(testGormDB.WithContext(ctx).Table(outbox.Table).Count(&gormCount).Error).NotTo(HaveOccurred())
		Expect(gormCount).To(Equal(int64(0)))
	})

	It("allows duplicate event_id values", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		insert := "INSERT INTO " + outbox.Table + " (event, event_id) VALUES ($1, $2)"
		_, err := testPool.Exec(ctx, insert, `{"id":"evt-1"}`, "evt-1")
		Expect(err).NotTo(HaveOccurred())
		_, err = testPool.Exec(ctx, insert, `{"id":"evt-1"}`, "evt-1")
		Expect(err).NotTo(HaveOccurred())

		var count int
		Expect(testPool.QueryRow(ctx, "SELECT count(*) FROM "+outbox.Table+" WHERE event_id = $1", "evt-1").Scan(&count)).To(Succeed())
		Expect(count).To(Equal(2))
	})
})

func newTestDatabaseName() string {
	suffix := make([]byte, 6)
	_, _ = rand.Read(suffix)
	return "hummingbee_go_outbox_" + hex.EncodeToString(suffix)
}

func withAdminConn(ctx context.Context, serverURL string, fn func(*pgx.Conn) error) (err error) {
	conn, err := pgx.Connect(ctx, serverURL)
	if err != nil {
		return fmt.Errorf("connect to test server: %w", err)
	}
	defer func() { err = errors.Join(err, conn.Close(context.WithoutCancel(ctx))) }()
	return fn(conn)
}

func createTestDatabase(ctx context.Context, serverURL, name string) (string, error) {
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", testDatabaseURLVar, err)
	}
	parsed.Path = "/" + name

	err = withAdminConn(ctx, serverURL, func(conn *pgx.Conn) error {
		_, execErr := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
		return execErr
	})
	if err != nil {
		return "", fmt.Errorf("create test database %s: %w", name, err)
	}
	return parsed.String(), nil
}

func dropTestDatabase(ctx context.Context, serverURL, name string) error {
	err := withAdminConn(ctx, serverURL, func(conn *pgx.Conn) error {
		_, execErr := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		return execErr
	})
	if err != nil {
		return fmt.Errorf("drop test database %s: %w", name, err)
	}
	return nil
}

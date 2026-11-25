package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	"go.mau.fi/util/dbutil"
	log "maunium.net/go/maulogger/v2"
)

func TestKVDeletePrefixOlderThan(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:kv_prune_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	if _, err := db.KV.DeletePrefixOlderThan("", 1000); err == nil {
		t.Fatal("expected error for empty prefix")
	}

	// Seed expired, boundary, fresh, non-numeric, wildcard-trap, and unrelated keys,
	// plus >200 expired keys to exercise batched deletion.
	db.KV.Set("bb_edit:old-1", "1000")
	db.KV.Set("bb_edit:boundary", "2000")
	db.KV.Set("bb_edit:fresh", "3000")
	db.KV.Set("bb_edit:corrupt", "not-a-timestamp")
	db.KV.Set("bbXedit:wildcard-trap", "100")
	db.KV.Set("portal_service:any;-;+15551234567", "100")
	for i := 0; i < 250; i++ {
		db.KV.Set(fmt.Sprintf("bb_edit:batch-%03d", i), strconv.Itoa(500+i))
	}

	deleted, err := db.KV.DeletePrefixOlderThan("bb_edit:", 2000)
	if err != nil {
		t.Fatalf("DeletePrefixOlderThan failed: %v", err)
	}
	if deleted != 251 {
		t.Fatalf("expected 251 deleted keys (old-1 + 250 batch), got %d", deleted)
	}

	if got := db.KV.Get("bb_edit:old-1"); got != "" {
		t.Fatalf("expected bb_edit:old-1 to be deleted, got %q", got)
	}
	if got := db.KV.Get("bb_edit:batch-249"); got != "" {
		t.Fatalf("expected bb_edit:batch-249 to be deleted, got %q", got)
	}
	if got := db.KV.Get("bb_edit:boundary"); got != "2000" {
		t.Fatalf("expected boundary entry (== cutoff) to be preserved, got %q", got)
	}
	if got := db.KV.Get("bb_edit:fresh"); got != "3000" {
		t.Fatalf("expected fresh entry to be preserved, got %q", got)
	}
	if got := db.KV.Get("bb_edit:corrupt"); got != "not-a-timestamp" {
		t.Fatalf("expected non-numeric entry to be left untouched, got %q", got)
	}
	if got := db.KV.Get("bbXedit:wildcard-trap"); got != "100" {
		t.Fatalf("expected underscore wildcard trap key to remain untouched, got %q", got)
	}
	if got := db.KV.Get("portal_service:any;-;+15551234567"); got != "100" {
		t.Fatalf("expected unrelated KV key to remain untouched, got %q", got)
	}
}

type kvRecDriver struct {
	mu      sync.Mutex
	queries []string
	rows    [][2]string
}

func (d *kvRecDriver) Open(string) (driver.Conn, error) { return &kvRecConn{drv: d}, nil }

type kvRecConn struct{ drv *kvRecDriver }

func (c *kvRecConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *kvRecConn) Close() error                        { return nil }
func (c *kvRecConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *kvRecConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.drv.mu.Lock()
	c.drv.queries = append(c.drv.queries, query)
	copied := append([][2]string(nil), c.drv.rows...)
	c.drv.mu.Unlock()
	return &kvRecRows{data: copied}, nil
}
func (c *kvRecConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.drv.mu.Lock()
	c.drv.queries = append(c.drv.queries, query)
	c.drv.mu.Unlock()
	return driver.RowsAffected(len(args)), nil
}

type kvRecRows struct {
	data [][2]string
	idx  int
}

func (r *kvRecRows) Columns() []string { return []string{"key", "value"} }
func (r *kvRecRows) Close() error      { return nil }
func (r *kvRecRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.data) {
		return io.EOF
	}
	dest[0] = r.data[r.idx][0]
	dest[1] = r.data[r.idx][1]
	r.idx++
	return nil
}

var (
	kvRecOnce sync.Once
	kvRecDrv  = &kvRecDriver{}
)

func TestKVDeletePrefixOlderThanUsesPostgresPlaceholders(t *testing.T) {
	kvRecOnce.Do(func() {
		sql.Register("recording_kv_postgres", kvRecDrv)
	})

	sqlDB, err := sql.Open("recording_kv_postgres", "")
	if err != nil {
		t.Fatalf("failed to open recording sql db: %v", err)
	}
	defer sqlDB.Close()

	rawDB, err := dbutil.NewWithDB(sqlDB, "postgres")
	if err != nil {
		t.Fatalf("failed to wrap recording db as postgres: %v", err)
	}
	db := New(rawDB, log.DefaultLogger)

	kvRecDrv.mu.Lock()
	kvRecDrv.queries = nil
	kvRecDrv.rows = [][2]string{
		{"bb_edit:a", "100"},
		{"bb_edit:b", "200"},
		{"bb_edit:c", "900"},
	}
	kvRecDrv.mu.Unlock()

	deleted, err := db.KV.DeletePrefixOlderThan("bb_edit:", 500)
	if err != nil {
		t.Fatalf("DeletePrefixOlderThan failed: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", deleted)
	}

	kvRecDrv.mu.Lock()
	defer kvRecDrv.mu.Unlock()
	if len(kvRecDrv.queries) != 2 {
		t.Fatalf("expected 2 queries (SELECT + DELETE), got %d: %v", len(kvRecDrv.queries), kvRecDrv.queries)
	}
	for _, q := range kvRecDrv.queries {
		if strings.Contains(q, "?") {
			t.Fatalf("query must not use SQLite '?' placeholder: %s", q)
		}
	}
	if !strings.Contains(kvRecDrv.queries[0], "$1") {
		t.Fatalf("expected $1 placeholder in SELECT query: %s", kvRecDrv.queries[0])
	}
	if !strings.Contains(kvRecDrv.queries[1], "$1") || !strings.Contains(kvRecDrv.queries[1], "$2") {
		t.Fatalf("expected $1, $2 placeholders in DELETE query: %s", kvRecDrv.queries[1])
	}
}

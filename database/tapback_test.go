package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"sync"
	"testing"

	"go.mau.fi/util/dbutil"
	log "maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-imessage/imessage"
)

func TestTapbackSchemaAndCustomEmojiRoundTrip(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:tapback_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	portal := db.Portal.New()
	portal.GUID = "iMessage;-;+15551234567"
	portal.MXID = id.RoomID("!room:example.com")
	portal.Insert(nil)

	msg := db.Message.New()
	msg.PortalGUID = portal.GUID
	msg.GUID = "msg-1"
	msg.Part = 0
	msg.MXID = id.EventID("$evt-msg-1")
	msg.SenderGUID = "iMessage;-;+15551234567"
	msg.Timestamp = 1700000000000
	msg.Insert(nil)

	tb := db.Tapback.New()
	tb.PortalGUID = portal.GUID
	tb.GUID = "tb-guid-1"
	tb.MessageGUID = msg.GUID
	tb.MessagePart = 0
	tb.SenderGUID = ""
	tb.HandleGUID = portal.GUID
	tb.Type = imessage.TapbackCustomEmoji
	tb.Emoji = "🔥"
	tb.MXID = id.EventID("$evt-tb-1")
	tb.Insert(nil)

	got := db.Tapback.GetByMXID(tb.MXID)
	if got == nil {
		t.Fatal("expected tapback to be found by MXID")
	}
	if got.Type != imessage.TapbackCustomEmoji || got.Emoji != "🔥" || got.GUID != "tb-guid-1" {
		t.Fatalf("unexpected tapback from DB: %+v", got)
	}

	got.Emoji = "🎉"
	got.MXID = id.EventID("$evt-tb-2")
	got.Update()

	updated := db.Tapback.GetByGUID(portal.GUID, msg.GUID, 0, "")
	if updated == nil || updated.Emoji != "🎉" || updated.MXID != "$evt-tb-2" {
		t.Fatalf("unexpected updated tapback: %+v", updated)
	}
}

type recordingExecDriver struct {
	mu      sync.Mutex
	queries []string
	args    [][]driver.NamedValue
}

func (d *recordingExecDriver) Open(string) (driver.Conn, error) {
	return &recordingConn{drv: d}, nil
}

type recordingConn struct{ drv *recordingExecDriver }

func (c *recordingConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *recordingConn) Close() error                        { return nil }
func (c *recordingConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c *recordingConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.drv.mu.Lock()
	c.drv.queries = append(c.drv.queries, query)
	copied := make([]driver.NamedValue, len(args))
	copy(copied, args)
	c.drv.args = append(c.drv.args, copied)
	c.drv.mu.Unlock()
	return driver.RowsAffected(1), nil
}

var (
	registerRecOnce sync.Once
	recDriver       = &recordingExecDriver{}
)

func TestTapbackUpdateUsesPostgresPlaceholders(t *testing.T) {
	registerRecOnce.Do(func() {
		sql.Register("recording_postgres", recDriver)
	})

	sqlDB, err := sql.Open("recording_postgres", "")
	if err != nil {
		t.Fatalf("failed to open recording sql db: %v", err)
	}
	defer sqlDB.Close()

	rawDB, err := dbutil.NewWithDB(sqlDB, "postgres")
	if err != nil {
		t.Fatalf("failed to wrap recording db as postgres: %v", err)
	}
	db := New(rawDB, log.DefaultLogger)

	recDriver.mu.Lock()
	recDriver.queries = nil
	recDriver.args = nil
	recDriver.mu.Unlock()

	tb := db.Tapback.New()
	tb.PortalGUID = "iMessage;-;+15551234567"
	tb.MessageGUID = "msg-1"
	tb.MessagePart = 0
	tb.SenderGUID = ""
	tb.GUID = "tb-guid-2"
	tb.Type = imessage.TapbackCustomEmoji
	tb.Emoji = "🎉"
	tb.MXID = id.EventID("$evt-tb-2")
	tb.Update()

	recDriver.mu.Lock()
	defer recDriver.mu.Unlock()
	if len(recDriver.queries) != 1 {
		t.Fatalf("expected 1 UPDATE query, got %d", len(recDriver.queries))
	}
	q := recDriver.queries[0]
	if strings.Contains(q, "?") {
		t.Fatalf("UPDATE query must not contain SQLite '?' placeholders for PostgreSQL compatibility: %s", q)
	}
	for _, placeholder := range []string{"$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8"} {
		if !strings.Contains(q, placeholder) {
			t.Fatalf("expected placeholder %s in UPDATE query: %s", placeholder, q)
		}
	}
	if len(recDriver.args[0]) != 8 {
		t.Fatalf("expected 8 query args, got %d", len(recDriver.args[0]))
	}
}

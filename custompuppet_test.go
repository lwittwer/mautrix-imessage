package main

import (
	"testing"

	"go.mau.fi/util/dbutil"
	log "maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-imessage/config"
	"go.mau.fi/mautrix-imessage/database"
)

func TestLogoutMatrixClearsAndPersistsAcrossRestart(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:custompuppet_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	br := &IMBridge{
		DB: db,
		Config: &config.Config{
			Bridge: config.BridgeConfig{
				User:              "@alice:example.com",
				LoginSharedSecret: "super-secret",
			},
		},
	}
	br.Log = log.DefaultLogger

	dbUser := db.User.New()
	dbUser.MXID = "@alice:example.com"
	dbUser.AccessToken = "active-token"
	dbUser.Insert()

	user := br.NewUser(dbUser)
	user.DoublePuppetIntent = &appservice.IntentAPI{}

	// Non-empty mismatching MXID must still be rejected.
	if err := user.SwitchCustomMXID("other-token", "@bob:example.com"); err == nil {
		t.Fatal("expected mismatching mxid error")
	}

	// commands.CommandLogoutMatrix invokes SwitchCustomMXID("", "").
	if err := user.SwitchCustomMXID("", ""); err != nil {
		t.Fatalf("expected logout-matrix SwitchCustomMXID(\"\", \"\") to succeed, got: %v", err)
	}
	if user.AccessToken != "" || user.DoublePuppetIntent != nil {
		t.Fatalf("expected in-memory double puppet to be cleared, got token=%q intent=%v", user.AccessToken, user.DoublePuppetIntent)
	}

	reloadedDBUser := db.User.GetByMXID("@alice:example.com")
	if reloadedDBUser == nil || reloadedDBUser.AccessToken != "" {
		t.Fatalf("expected cleared access token to be persisted, got %+v", reloadedDBUser)
	}
	if got := db.KV.Get(database.KVDoublePuppetLoggedOut); got != "true" {
		t.Fatalf("expected KVDoublePuppetLoggedOut='true', got %q", got)
	}

	// Simulate bridge restart with LoginSharedSecret configured: initDoublePuppet must NOT auto-login again.
	restartedUser := br.NewUser(reloadedDBUser)
	restartedUser.initDoublePuppet()
	if restartedUser.DoublePuppetIntent != nil || restartedUser.AccessToken != "" {
		t.Fatalf("expected manual logout to persist across restart even with shared secret, got token=%q intent=%v", restartedUser.AccessToken, restartedUser.DoublePuppetIntent)
	}
}

func TestParseCreateGroupArgs(t *testing.T) {
	addrs, msg := parseCreateGroupArgs("+15551111111 alice@example.com -- Hello   everyone!")
	if len(addrs) != 2 || addrs[0] != "+15551111111" || addrs[1] != "alice@example.com" || msg != "Hello   everyone!" {
		t.Fatalf("unexpected parse result: addrs=%v msg=%q", addrs, msg)
	}

	addrs2, msg2 := parseCreateGroupArgs("+15551111111 +15552222222")
	if len(addrs2) != 2 || msg2 != "" {
		t.Fatalf("unexpected parse result without message: addrs=%v msg=%q", addrs2, msg2)
	}

	addrs3, msg3 := parseCreateGroupArgs("+15551111111 +15552222222 --")
	if len(addrs3) != 2 || msg3 != "" {
		t.Fatalf("unexpected parse result with trailing --: addrs=%v msg=%q", addrs3, msg3)
	}
}

var _ = id.UserID("")

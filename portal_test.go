package main

import (
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	log "maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-imessage/config"
	"go.mau.fi/mautrix-imessage/database"
	"go.mau.fi/mautrix-imessage/imessage"
	"go.mau.fi/mautrix-imessage/imessage/bluebubbles"
)

func TestConvertEditedMultipartCaptionTargetsStoredCaptionOnly(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:portal_edit_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	zlog := zerolog.Nop()
	br := &IMBridge{
		DB: db,
		Config: &config.Config{
			Bridge: config.BridgeConfig{
				CaptionInMessage: false,
			},
		},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	bbAPI, err := bluebubbles.NewBlueBubblesConnector(br)
	if err != nil {
		t.Fatalf("failed to create bluebubbles connector: %v", err)
	}
	br.IM = bbAPI

	dbPortal := db.Portal.New()
	dbPortal.GUID = "iMessage;-;+15551234567"
	dbPortal.MXID = id.RoomID("!room:example.com")
	dbPortal.Insert(nil)

	// Part 0 = attachment, Part 1 = caption text
	attRow := db.Message.New()
	attRow.PortalGUID = dbPortal.GUID
	attRow.GUID = "multi-msg-1"
	attRow.Part = 0
	attRow.MXID = id.EventID("$att-event")
	attRow.SenderGUID = dbPortal.GUID
	attRow.Timestamp = 1700000000000
	attRow.Insert(nil)

	captionRow := db.Message.New()
	captionRow.PortalGUID = dbPortal.GUID
	captionRow.GUID = "multi-msg-1"
	captionRow.Part = 1
	captionRow.MXID = id.EventID("$caption-event")
	captionRow.SenderGUID = dbPortal.GUID
	captionRow.Timestamp = 1700000000000
	captionRow.Insert(nil)

	portal := &Portal{
		Portal: dbPortal,
		bridge: br,
		log:    log.DefaultLogger,
	}

	editedMsg := &imessage.Message{
		GUID:     "multi-msg-1",
		ChatGUID: dbPortal.GUID,
		Text:     "edited caption",
		IsEdited: true,
		Attachments: []*imessage.Attachment{
			{GUID: "att-1", FileName: "photo.jpg", DownloadError: "should not even be read on caption edit"},
		},
	}

	lastRow := db.Message.GetLastByGUID(dbPortal.GUID, editedMsg.GUID)
	parts := portal.convertEditediMessage(editedMsg, lastRow, nil)
	if len(parts) != 1 {
		t.Fatalf("expected exactly 1 edit event (no duplicate media), got %d", len(parts))
	}
	content := parts[0].Content
	if content.RelatesTo == nil || content.RelatesTo.Type != event.RelReplace || content.RelatesTo.EventID != "$caption-event" {
		t.Fatalf("expected m.replace targeting $caption-event, got %+v", content.RelatesTo)
	}
	if content.NewContent == nil || content.NewContent.Body != "edited caption" {
		t.Fatalf("expected new content 'edited caption', got %+v", content.NewContent)
	}
}

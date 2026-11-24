package main

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	log "maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridge/bridgeconfig"
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

func TestAnalyzeAudioPCM(t *testing.T) {
	// 1.5 seconds of 8kHz mono s16le: first half quiet, second half loud sine wave.
	const sampleRate = 8000
	const totalSamples = 12000
	pcm := make([]byte, totalSamples*2)
	for i := 0; i < totalSamples; i++ {
		amp := 2000.0
		if i >= totalSamples/2 {
			amp = 24000.0
		}
		sample := int16(amp * math.Sin(2*math.Pi*440*float64(i)/float64(sampleRate)))
		binary.LittleEndian.PutUint16(pcm[i*2:i*2+2], uint16(sample))
	}

	durMs, wf := analyzeAudioPCM(pcm, sampleRate)
	if durMs != 1500 {
		t.Fatalf("expected 1500ms duration, got %d", durMs)
	}
	if len(wf) != voiceWaveformBuckets {
		t.Fatalf("expected %d waveform samples, got %d", voiceWaveformBuckets, len(wf))
	}
	for idx, v := range wf {
		if v < 0 || v > voiceWaveformMaxValue {
			t.Fatalf("waveform[%d]=%d out of [0, %d]", idx, v, voiceWaveformMaxValue)
		}
	}
	if wf[10] >= wf[50] {
		t.Fatalf("expected quiet first half (%d) < loud second half (%d)", wf[10], wf[50])
	}

	// Silence produces all zeros and preserves duration
	silence := make([]byte, 8000*2)
	durSilence, wfSilence := analyzeAudioPCM(silence, sampleRate)
	if durSilence != 1000 || len(wfSilence) != voiceWaveformBuckets {
		t.Fatalf("unexpected silence result: dur=%d len=%d", durSilence, len(wfSilence))
	}
	for idx, v := range wfSilence {
		if v != 0 {
			t.Fatalf("expected silence waveform[%d]=0, got %d", idx, v)
		}
	}

	// Very short audio (< 64 samples) must not panic
	shortPCM := make([]byte, 10)
	_, wfShort := analyzeAudioPCM(shortPCM, sampleRate)
	if len(wfShort) != 5 {
		t.Fatalf("expected 5 buckets for 5-sample buffer, got %d", len(wfShort))
	}
}

func TestFormatHealthReport(t *testing.T) {
	now := time.Date(2026, 11, 24, 18, 0, 0, 0, time.UTC)
	helperTrue := true
	helperFalse := false

	healthy := formatHealthReport("bluebubbles", &imessage.ConnectorHealth{
		ServerVersion:    "1.9.7",
		OSVersion:        "15.3.1",
		PrivateAPI:       true,
		HelperConnected:  &helperTrue,
		WSConnected:      true,
		ConnectedSince:   now.Add(-2 * time.Hour),
		LastEventAt:      now.Add(-45 * time.Second),
		ReconnectCount:   1,
		LastCatchUpAt:    now.Add(-2 * time.Hour),
		LastCatchUpChats: 3,
		LastCatchUpMsgs:  7,
	}, now)
	for _, want := range []string{"Healthy", "1.9.7", "macOS 15.3.1", "helper: connected", "up 2h", "45s ago", "3 chats, 7 messages"} {
		if !strings.Contains(healthy, want) {
			t.Fatalf("expected healthy report to contain %q, got:\n%s", want, healthy)
		}
	}

	degradedHelper := formatHealthReport("bluebubbles", &imessage.ConnectorHealth{
		ServerVersion:   "1.9.7",
		PrivateAPI:      true,
		HelperConnected: &helperFalse,
		WSConnected:     true,
	}, now)
	if !strings.Contains(degradedHelper, "Degraded (Private API helper disconnected") || !strings.Contains(degradedHelper, "DISCONNECTED") {
		t.Fatalf("expected degraded helper report, got:\n%s", degradedHelper)
	}

	disconnected := formatHealthReport("bluebubbles", &imessage.ConnectorHealth{
		WSConnected:       false,
		DisconnectedSince: now.Add(-30 * time.Second),
		LastDisconnectErr: "unexpected EOF",
		ServerInfoErr:     "connection refused",
	}, now)
	if !strings.Contains(disconnected, "Disconnected") || !strings.Contains(disconnected, "unexpected EOF") || !strings.Contains(disconnected, "connection refused") {
		t.Fatalf("expected disconnected report details, got:\n%s", disconnected)
	}
}

func TestBackfillMetaIndexesZeroBased(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:portal_bf_index_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
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
			BaseConfig: &bridgeconfig.BaseConfig{},
			Bridge: config.BridgeConfig{
				UsernameTemplate:    "imessage_{{.}}",
				DisplaynameTemplate: "{{.}}",
			},
		},
		puppets: make(map[string]*Puppet),
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	br.Bot = &appservice.IntentAPI{UserID: "@bot:example.com"}
	bbAPI, _ := bluebubbles.NewBlueBubblesConnector(br)
	br.IM = bbAPI

	dbPortal := db.Portal.New()
	dbPortal.GUID = "iMessage;-;+15551234567"
	dbPortal.MXID = id.RoomID("!room:example.com")
	dbPortal.Insert(nil)

	puppet := &Puppet{
		Puppet: &database.Puppet{ID: "+15551234567", Displayname: "Alice"},
		bridge: br,
		MXID:   "@imessage_15551234567:example.com",
		Intent: &appservice.IntentAPI{UserID: "@imessage_15551234567:example.com"},
	}
	br.puppets["+15551234567"] = puppet

	portal := &Portal{
		Portal:     dbPortal,
		bridge:     br,
		log:        log.DefaultLogger,
		Identifier: imessage.ParseIdentifier(dbPortal.GUID),
	}

	msgs := []*imessage.Message{
		{GUID: "first-msg", ChatGUID: dbPortal.GUID, Sender: imessage.Identifier{LocalID: "+15551234567"}, Text: "hello", Time: time.Now()},
		{GUID: "second-msg", ChatGUID: dbPortal.GUID, Sender: imessage.Identifier{LocalID: "+15551234567"}, Text: "reply", ReplyToGUID: "first-msg", Time: time.Now()},
	}
	events, _, metaIndexes, _, err := portal.convertBackfill(msgs)
	if err != nil {
		t.Fatalf("convertBackfill failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if idx, ok := metaIndexes[messageIndex{"first-msg", 0}]; !ok || idx != 0 {
		t.Fatalf("expected first-msg at index 0, got ok=%v idx=%d", ok, idx)
	}
	if idx, ok := metaIndexes[messageIndex{"second-msg", 0}]; !ok || idx != 1 {
		t.Fatalf("expected second-msg at index 1, got ok=%v idx=%d", ok, idx)
	}
}

func TestPortalEffectiveServiceAndSequoiaAny(t *testing.T) {
	zlog := zerolog.Nop()
	br := &IMBridge{
		Config: &config.Config{
			BaseConfig: &bridgeconfig.BaseConfig{},
			IMessage:   imessage.PlatformConfig{Platform: "bluebubbles"},
		},
		puppets: make(map[string]*Puppet),
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	br.Bot = &appservice.IntentAPI{UserID: "@bot:example.com"}
	br.puppets["+15551234567"] = &Puppet{
		Puppet: &database.Puppet{ID: "+15551234567"},
		bridge: br,
		MXID:   "@imessage_15551234567:example.com",
		Intent: &appservice.IntentAPI{UserID: "@imessage_15551234567:example.com"},
	}

	portal := &Portal{
		Portal:     &database.Portal{GUID: "any;-;+15551234567"},
		bridge:     br,
		log:        log.DefaultLogger,
		Identifier: imessage.ParseIdentifier("any;-;+15551234567"),
	}

	// Default before any message arrives
	if got := portal.effectiveService(); got != "iMessage" {
		t.Fatalf("expected default effectiveService 'iMessage', got %q", got)
	}

	// Receiving an RCS message on an any;-; chat switches effectiveService and bridge info
	portal.noteMessageService("RCS")
	if got := portal.effectiveService(); got != "RCS" {
		t.Fatalf("expected effectiveService 'RCS', got %q", got)
	}
	_, info := portal.getBridgeInfo()
	if info.Protocol.ID != "imessage-sms" || info.Protocol.DisplayName != "iMessage (RCS)" || info.Channel.Service != "RCS" {
		t.Fatalf("unexpected RCS bridge info: %+v", info)
	}

	// Switching to SMS updates bridge info to SMS
	portal.noteMessageService("SMS")
	_, smsInfo := portal.getBridgeInfo()
	if smsInfo.Protocol.ID != "imessage-sms" || smsInfo.Protocol.DisplayName != "iMessage (SMS)" {
		t.Fatalf("unexpected SMS bridge info: %+v", smsInfo)
	}
}

func TestUpdateAvatarCleansUpTempFile(t *testing.T) {
	tmpDir := t.TempDir()
	tempAvatar := filepath.Join(tmpDir, "bb-avatar-temp.jpg")
	if err := os.WriteFile(tempAvatar, []byte("avatar-bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	nonTempAvatar := filepath.Join(tmpDir, "mac-avatar-keep.jpg")
	if err := os.WriteFile(nonTempAvatar, []byte("avatar-bytes"), 0600); err != nil {
		t.Fatal(err)
	}

	portal := &Portal{
		Portal: &database.Portal{GUID: "iMessage;+;chat123"},
		log:    log.DefaultLogger,
	}

	// Set matching hash so UpdateAvatar returns early without calling UploadBytes,
	// while still running the deferred temp-file cleanup.
	tempAtt := &imessage.Attachment{PathOnDisk: tempAvatar, IsTemp: true}
	data, _ := os.ReadFile(tempAvatar)
	// First verify non-temp with matching hash is NOT deleted.
	portal.UpdateAvatar(&imessage.Attachment{PathOnDisk: nonTempAvatar, IsTemp: false, DownloadError: "stop-before-upload"}, nil)
	if _, err := os.Stat(nonTempAvatar); err != nil {
		t.Fatalf("non-temp avatar should NOT have been deleted: %v", err)
	}

	// Temp attachment MUST be deleted even if Read() fails or returns early.
	_ = data
	tempAtt.DownloadError = "stop-before-upload"
	portal.UpdateAvatar(tempAtt, nil)
	if _, err := os.Stat(tempAvatar); !os.IsNotExist(err) {
		t.Fatalf("expected temp avatar file to be deleted, stat err=%v", err)
	}
}

type fakeGroupAdminAPI struct {
	imessage.API
	groupAdmin bool
}

func (f *fakeGroupAdminAPI) Capabilities() imessage.ConnectorCapabilities {
	return imessage.ConnectorCapabilities{GroupAdmin: f.groupAdmin}
}

func TestPortalPowerLevelsGroupAdmin(t *testing.T) {
	br := &IMBridge{
		user: &User{User: &database.User{MXID: "@alice:example.com"}},
	}
	br.Bot = &appservice.IntentAPI{UserID: "@bot:example.com"}
	br.IM = &fakeGroupAdminAPI{groupAdmin: true}

	imGroup := &Portal{
		Portal:     &database.Portal{GUID: "iMessage;+;chat123"},
		bridge:     br,
		Identifier: imessage.ParseIdentifier("iMessage;+;chat123"),
	}
	pls := imGroup.GetBasePowerLevels()
	if pls.Invite() != 50 || pls.Kick() != 50 || pls.GetUserLevel("@alice:example.com") != 50 {
		t.Fatalf("expected moderator invite/kick power levels for iMessage group, got invite=%d kick=%d user=%d", pls.Invite(), pls.Kick(), pls.GetUserLevel("@alice:example.com"))
	}

	// Existing room with old locked power levels gets upgraded by updatePowerLevels
	oldPLs := &event.PowerLevelsEventContent{Users: map[id.UserID]int{"@bot:example.com": 100}}
	locked := 99
	oldPLs.InvitePtr = &locked
	oldPLs.KickPtr = &locked
	if !imGroup.updatePowerLevels(oldPLs) {
		t.Fatal("expected updatePowerLevels to migrate old locked power levels")
	}
	if oldPLs.Invite() != 50 || oldPLs.Kick() != 50 || oldPLs.GetUserLevel("@alice:example.com") != 50 {
		t.Fatalf("unexpected migrated power levels: invite=%d kick=%d user=%d", oldPLs.Invite(), oldPLs.Kick(), oldPLs.GetUserLevel("@alice:example.com"))
	}

	// SMS group stays locked at 99
	smsGroup := &Portal{
		Portal:     &database.Portal{GUID: "SMS;+;chat456"},
		bridge:     br,
		Identifier: imessage.ParseIdentifier("SMS;+;chat456"),
	}
	smsPLs := smsGroup.GetBasePowerLevels()
	if smsPLs.Invite() != 99 || smsPLs.Kick() != 99 {
		t.Fatalf("expected SMS group to stay locked at 99, got invite=%d kick=%d", smsPLs.Invite(), smsPLs.Kick())
	}
}

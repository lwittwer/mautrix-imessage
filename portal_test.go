package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	log "maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix"
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

	// Sequoia any;+; group stays locked until its service is confirmed as iMessage
	anyGroup := &Portal{
		Portal:     &database.Portal{GUID: "any;+;chat789"},
		bridge:     br,
		log:        log.DefaultLogger,
		Identifier: imessage.ParseIdentifier("any;+;chat789"),
	}
	if anyGroup.canManageGroup() {
		t.Fatal("expected any;+; group with unknown service not to enable group admin")
	}
	if pls := anyGroup.GetBasePowerLevels(); pls.Invite() != 99 || pls.Kick() != 99 {
		t.Fatalf("expected unknown-service any;+; group locked at 99, got invite=%d kick=%d", pls.Invite(), pls.Kick())
	}
	anyGroup.noteMessageService("SMS")
	if anyGroup.canManageGroup() {
		t.Fatal("expected any;+; group with SMS service not to enable group admin")
	}
	anyGroup.noteMessageService("iMessage")
	if !anyGroup.canManageGroup() {
		t.Fatal("expected any;+; group with confirmed iMessage service to enable group admin")
	}
	if pls := anyGroup.GetBasePowerLevels(); pls.Invite() != 50 || pls.Kick() != 50 {
		t.Fatalf("expected confirmed iMessage any;+; group to allow invite/kick at 50, got invite=%d kick=%d", pls.Invite(), pls.Kick())
	}
}

type fakeTestIMAPI struct {
	imessage.API
	resolvedGUID      string
	backfillMessages  []*imessage.Message
	lastSendFileMime  string
	lastSendFileVoice bool
	sendFileCalled    bool
	lastSendMsgChatID string
	lastSendMsgText   string
}

func (f *fakeTestIMAPI) Capabilities() imessage.ConnectorCapabilities {
	return imessage.ConnectorCapabilities{MessageSendResponses: true}
}

func (f *fakeTestIMAPI) ResolveIdentifier(identifier string) (string, error) {
	return f.resolvedGUID, nil
}

func (f *fakeTestIMAPI) PrepareDM(guid string) error { return nil }

func (f *fakeTestIMAPI) GetMessagesSinceDate(chatID string, minDate time.Time, backfillID string) ([]*imessage.Message, error) {
	return f.backfillMessages, nil
}

func (f *fakeTestIMAPI) SendBackfillResult(chatID, backfillID string, success bool, idMap map[string][]id.EventID) {
}

func (f *fakeTestIMAPI) SendMessageBridgeResult(chatID, messageID string, eventID id.EventID, success bool) {
}

func (f *fakeTestIMAPI) SendMessage(chatID, text string, replyTo string, replyToPart int, richLink *imessage.RichLink, metadata imessage.MessageMetadata) (*imessage.SendResponse, error) {
	f.lastSendMsgChatID = chatID
	f.lastSendMsgText = text
	return &imessage.SendResponse{GUID: "out-msg-1", Service: "iMessage", ChatGUID: chatID, Time: time.Now()}, nil
}

func (f *fakeTestIMAPI) SendFile(chatID, text, filename string, pathOnDisk string, replyTo string, replyToPart int, mimeType string, voiceMemo bool, metadata imessage.MessageMetadata) (*imessage.SendResponse, error) {
	f.sendFileCalled = true
	f.lastSendFileMime = mimeType
	f.lastSendFileVoice = voiceMemo
	return &imessage.SendResponse{GUID: "sent-file-1", Service: "SMS", ChatGUID: chatID, Time: time.Now()}, nil
}

func (f *fakeTestIMAPI) SendFileCleanup(sendFileDir string) {
	_ = os.RemoveAll(sendFileDir)
}

func TestStartChatReusesExistingRoomWhenResolveReturnsAny(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:startchat_any_reuse_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	var sentMatrixRoom id.RoomID
	mxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/send/m.room.message/") {
			// Path: /_matrix/client/v3/rooms/<roomID>/send/m.room.message/<txn>
			parts := strings.Split(r.URL.Path, "/")
			for i, p := range parts {
				if p == "rooms" && i+1 < len(parts) {
					sentMatrixRoom = id.RoomID(parts[i+1])
					break
				}
			}
			_, _ = w.Write([]byte(`{"event_id":"$in-evt-1"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer mxSrv.Close()

	makeTestIntent := func(mxid id.UserID, serverURL string) *appservice.IntentAPI {
		as := appservice.Create()
		as.HomeserverDomain = "example.com"
		as.Registration = &appservice.Registration{AppToken: "token"}
		client, err := mautrix.NewClient(serverURL, mxid, "token")
		if err != nil {
			t.Fatal(err)
		}
		intent := as.NewIntentAPI(mxid.Localpart())
		intent.Client = client
		intent.UserID = mxid
		intent.IsCustomPuppet = true
		as.StateStore.MarkRegistered(mxid)
		as.StateStore.SetMembership("!original-room:example.com", mxid, event.MembershipJoin)
		return intent
	}

	puppetIntent := makeTestIntent("@imessage_15551234567:example.com", mxSrv.URL)
	botIntent := makeTestIntent("@bot:example.com", mxSrv.URL)

	fakeIM := &fakeTestIMAPI{resolvedGUID: "any;-;+15551234567"}
	newBridge := func() *IMBridge {
		zlog := zerolog.Nop()
		b := &IMBridge{
			DB:            db,
			Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
			portalsByGUID: make(map[string]*Portal),
			portalsByMXID: make(map[id.RoomID]*Portal),
			puppets:       make(map[string]*Puppet),
			user:          &User{User: &database.User{MXID: "@me:example.com"}},
			IM:            fakeIM,
		}
		b.Log = log.DefaultLogger
		b.ZLog = &zlog
		b.Bot = botIntent
		b.puppets["+15551234567"] = &Puppet{
			Puppet: &database.Puppet{ID: "+15551234567", Displayname: "Alice"},
			bridge: b,
			MXID:   "@imessage_15551234567:example.com",
			Intent: puppetIntent,
		}
		return b
	}

	br := newBridge()

	// Pre-existing iMessage;-; portal with a Matrix room (pre-Sequoia migration state)
	dbPortal := db.Portal.New()
	dbPortal.GUID = "iMessage;-;+15551234567"
	dbPortal.MXID = id.RoomID("!original-room:example.com")
	dbPortal.Insert(nil)

	mx := &WebsocketCommandHandler{bridge: br, log: log.DefaultLogger}
	resp, err := mx.StartChat(StartDMRequest{
		Identifier:    "+15551234567",
		ActuallyStart: true,
	})
	if err != nil {
		t.Fatalf("StartChat failed: %v", err)
	}
	if resp.RoomID != "!original-room:example.com" || resp.GUID != "any;-;+15551234567" || resp.JustCreated {
		t.Fatalf("expected StartChat to reuse original room and bind to any;-;+15551234567, got %+v", resp)
	}

	// Simulate restart: reload bridge from SQLite and verify BOTH inbound any;-; message
	// and outbound Matrix reply use the original Matrix room and current BlueBubbles chat.
	brReloaded := newBridge()
	inboundPortal := brReloaded.GetPortalByGUID("any;-;+15551234567")
	if inboundPortal.MXID != "!original-room:example.com" {
		t.Fatalf("expected persisted any;-; portal to resolve to original room, got %q", inboundPortal.MXID)
	}
	if legacyAlias := brReloaded.GetPortalByGUID("iMessage;-;+15551234567"); legacyAlias.MXID != "!original-room:example.com" {
		t.Fatalf("expected legacy iMessage;-; alias to also resolve to original room, got %q", legacyAlias.MXID)
	}

	// Inbound message on any;-; lands in the original Matrix room
	inboundPortal.HandleiMessage(&imessage.Message{
		GUID:     "in-any-1",
		ChatGUID: "any;-;+15551234567",
		Service:  "iMessage",
		Sender:   imessage.Identifier{LocalID: "+15551234567", Service: "iMessage"},
		Text:     "hello after sequoia upgrade",
		Time:     time.Now(),
	})
	if sentMatrixRoom != "!original-room:example.com" {
		t.Fatalf("expected inbound message to be sent to !original-room:example.com, got %q", sentMatrixRoom)
	}

	// Outbound Matrix reply in the original room sends to the current BlueBubbles any;-; chat
	outboundPortal := brReloaded.GetPortalByMXID("!original-room:example.com")
	if outboundPortal == nil {
		t.Fatal("expected GetPortalByMXID to return original room portal")
	}
	outboundPortal.HandleMatrixMessage(&event.Event{
		ID:        "$out-evt-1",
		Sender:    "@me:example.com",
		Timestamp: time.Now().UnixMilli(),
		Type:      event.EventMessage,
		Content: event.Content{
			Parsed: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    "reply from matrix",
			},
		},
	})
	if fakeIM.lastSendMsgChatID != "any;-;+15551234567" || fakeIM.lastSendMsgText != "reply from matrix" {
		t.Fatalf("expected outbound reply to target any;-;+15551234567, got chat=%q text=%q", fakeIM.lastSendMsgChatID, fakeIM.lastSendMsgText)
	}
}

func TestAnyPortalPersistsServiceAcrossRestartForOutboundVoice(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:any_service_restart_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	zlog := zerolog.Nop()
	// First run: receive an SMS message on an any;-; portal so its service is persisted.
	br1 := &IMBridge{
		DB:            db,
		Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		IM:            &fakeTestIMAPI{},
	}
	br1.Log = log.DefaultLogger
	br1.ZLog = &zlog

	dbPortal := db.Portal.New()
	dbPortal.GUID = "any;-;+15557654321"
	dbPortal.LastSeenHandle = "any;-;+15557654321"
	dbPortal.Insert(nil)

	p1 := br1.NewPortal(dbPortal)
	p1.noteMessageService("SMS")

	// Simulate restart: brand-new IMBridge loading the portal fresh from SQLite.
	fakeIM := &fakeTestIMAPI{}
	mediaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Minimal OggS header so mimetype.Detect sees audio/ogg
		_, _ = w.Write([]byte("OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x13OpusHead\x01\x01\x00\x00\x80\xbb\x00\x00\x00\x00\x00"))
	}))
	defer mediaSrv.Close()

	mxClient, err := mautrix.NewClient(mediaSrv.URL, "@imessage_15557654321:example.com", "token")
	if err != nil {
		t.Fatal(err)
	}
	br2 := &IMBridge{
		DB:            db,
		Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		puppets:       make(map[string]*Puppet),
		IM:            fakeIM,
	}
	br2.Log = log.DefaultLogger
	br2.ZLog = &zlog
	br2.puppets["+15557654321"] = &Puppet{
		Puppet: &database.Puppet{ID: "+15557654321"},
		bridge: br2,
		MXID:   "@imessage_15557654321:example.com",
		Intent: &appservice.IntentAPI{Client: mxClient, UserID: "@imessage_15557654321:example.com"},
	}

	reloaded := br2.GetPortalByGUID("any;-;+15557654321")
	if got := reloaded.effectiveService(); got != "SMS" {
		t.Fatalf("expected reloaded any;-; portal to recover persisted service 'SMS', got %q", got)
	}

	// Send an MSC3245 voice message BEFORE any incoming message after restart:
	// must NOT attempt CAF conversion (which would also fail without ffmpeg / send audio/x-caf).
	evt := &event.Event{
		ID: "$voice-out-1",
		Content: event.Content{
			Raw: map[string]any{
				"org.matrix.msc3245.voice": map[string]any{},
			},
		},
	}
	mxc := id.ContentURI{Homeserver: "example.com", FileID: "voice1"}
	if _, err := reloaded.handleMatrixMediaDirect(mxc, nil, "voice.ogg", "", evt, "", 0, nil); err != nil {
		t.Fatalf("handleMatrixMediaDirect failed on SMS any;-; portal after restart: %v", err)
	}
	if !fakeIM.sendFileCalled || fakeIM.lastSendFileVoice || fakeIM.lastSendFileMime == "audio/x-caf" {
		t.Fatalf("expected SMS voice send to skip CAF conversion, called=%v voiceMemo=%v mime=%q", fakeIM.sendFileCalled, fakeIM.lastSendFileVoice, fakeIM.lastSendFileMime)
	}
}

func TestForwardBackfillAndHandleiMessageCleanUpTempAttachments(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:temp_att_cleanup_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	tmpDir := t.TempDir()
	makeFile := func(name string) string {
		p := filepath.Join(tmpDir, name)
		if err := os.WriteFile(p, []byte("payload"), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	bfTemp := makeFile("bf-dup-temp.bin")
	bfKeep := makeFile("bf-dup-keep.bin")
	echoTemp := makeFile("echo-dup-temp.bin")
	echoKeep := makeFile("echo-dup-keep.bin")
	timeoutTemp := makeFile("timeout-dup-temp.bin")

	fakeIM := &fakeTestIMAPI{
		backfillMessages: []*imessage.Message{
			{
				GUID:     "dup-msg-1",
				ChatGUID: "iMessage;-;+15551234567",
				Text:     "already bridged",
				Time:     time.Now(),
				Attachments: []*imessage.Attachment{
					{GUID: "a1", PathOnDisk: bfTemp, IsTemp: true},
					{GUID: "a2", PathOnDisk: bfKeep, IsTemp: false},
				},
			},
		},
	}

	zlog := zerolog.Nop()
	br := &IMBridge{
		DB:     db,
		Config: &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		IM:     fakeIM,
		user:   &User{User: &database.User{MXID: "@me:example.com"}},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog

	dbPortal := db.Portal.New()
	dbPortal.GUID = "iMessage;-;+15551234567"
	dbPortal.MXID = id.RoomID("!room:example.com")
	dbPortal.Insert(nil)

	existing := db.Message.New()
	existing.PortalGUID = dbPortal.GUID
	existing.GUID = "dup-msg-1"
	existing.Part = 0
	existing.MXID = "$existing-evt"
	existing.SenderGUID = dbPortal.GUID
	existing.Timestamp = time.Now().Add(-time.Minute).UnixMilli()
	existing.Insert(nil)

	portal := &Portal{
		Portal:       dbPortal,
		bridge:       br,
		log:          log.DefaultLogger,
		Identifier:   imessage.ParseIdentifier(dbPortal.GUID),
		messageDedup: map[string]SentMessage{"timeout.jpg": {EventID: "$timeout-evt", Timestamp: time.Now()}},
	}

	// 1. forwardBackfill skips duplicate message and MUST clean up IsTemp attachment while keeping non-temp.
	portal.forwardBackfill()
	if _, err := os.Stat(bfTemp); !os.IsNotExist(err) {
		t.Fatalf("expected forwardBackfill duplicate temp attachment to be deleted, stat err=%v", err)
	}
	if _, err := os.Stat(bfKeep); err != nil {
		t.Fatalf("expected non-temp attachment in forwardBackfill to remain untouched: %v", err)
	}

	// 2. HandleiMessage duplicate echo (e.g. own outbound media echoed back by BlueBubbles) cleans up IsTemp.
	portal.HandleiMessage(&imessage.Message{
		GUID:     "dup-msg-1",
		ChatGUID: dbPortal.GUID,
		IsFromMe: true,
		Time:     time.Now(),
		Attachments: []*imessage.Attachment{
			{GUID: "a3", PathOnDisk: echoTemp, IsTemp: true},
			{GUID: "a4", PathOnDisk: echoKeep, IsTemp: false},
		},
	})
	if _, err := os.Stat(echoTemp); !os.IsNotExist(err) {
		t.Fatalf("expected HandleiMessage duplicate echo temp attachment to be deleted, stat err=%v", err)
	}
	if _, err := os.Stat(echoKeep); err != nil {
		t.Fatalf("expected non-temp attachment in HandleiMessage to remain untouched: %v", err)
	}

	// 3. HandleiMessage timeout-dedup echo (getIntentForMessage returns nil) cleans up IsTemp.
	portal.HandleiMessage(&imessage.Message{
		GUID:     "timeout-echo-guid",
		ChatGUID: dbPortal.GUID,
		IsFromMe: true,
		Time:     time.Now(),
		Attachments: []*imessage.Attachment{
			{GUID: "a5", FileName: "timeout.jpg", PathOnDisk: timeoutTemp, IsTemp: true},
		},
	})
	if _, err := os.Stat(timeoutTemp); !os.IsNotExist(err) {
		t.Fatalf("expected HandleiMessage timeout-dedup temp attachment to be deleted, stat err=%v", err)
	}

	// 4. Expired timeout-dedup entry must NOT match a later message with the same text.
	portal.messageDedup = map[string]SentMessage{
		"hello again": {
			EventID:   "$old-timeout-evt",
			Timestamp: time.Now().Add(-10 * time.Minute),
			ExpiresAt: time.Now().Add(-1 * time.Minute),
		},
	}
	if portal.isDuplicate(db.Message.New(), &imessage.Message{GUID: "later-msg", Text: "hello again", Time: time.Now()}) {
		t.Fatal("expected expired timeout dedup entry not to match later message")
	}
}

func TestNoteMessageServiceUpdatesGroupPowerLevelsImmediately(t *testing.T) {
	var setPLCalled bool
	var gotInvite, gotKick int
	mxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/state/m.room.power_levels"):
			_, _ = w.Write([]byte(`{"invite":99,"kick":99,"users":{"@bot:example.com":100}}`))
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/state/m.room.power_levels"):
			setPLCalled = true
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if v, ok := body["invite"].(float64); ok {
				gotInvite = int(v)
			}
			if v, ok := body["kick"].(float64); ok {
				gotKick = int(v)
			}
			_, _ = w.Write([]byte(`{"event_id":"$pl-evt"}`))
		default:
			_, _ = w.Write([]byte(`{"event_id":"$state-evt"}`))
		}
	}))
	defer mxSrv.Close()

	as := appservice.Create()
	as.HomeserverDomain = "example.com"
	as.Registration = &appservice.Registration{AppToken: "token"}
	as.StateStore.MarkRegistered("@bot:example.com")
	as.StateStore.SetMembership("!grp:example.com", "@bot:example.com", event.MembershipJoin)
	botClient, err := mautrix.NewClient(mxSrv.URL, "@bot:example.com", "token")
	if err != nil {
		t.Fatal(err)
	}
	botIntent := as.NewIntentAPI("bot")
	botIntent.Client = botClient
	botIntent.UserID = "@bot:example.com"
	botIntent.IsCustomPuppet = true

	zlog := zerolog.Nop()
	br := &IMBridge{
		Config: &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		user:   &User{User: &database.User{MXID: "@alice:example.com"}},
		IM:     &fakeGroupAdminAPI{groupAdmin: true},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	br.Bot = botIntent

	portal := &Portal{
		Portal:        &database.Portal{GUID: "any;+;chat555", MXID: "!grp:example.com"},
		bridge:        br,
		log:           log.DefaultLogger,
		zlog:          zlog,
		Identifier:    imessage.ParseIdentifier("any;+;chat555"),
		serviceLoaded: true,
	}

	portal.noteMessageService("iMessage")
	if !setPLCalled || gotInvite != 50 || gotKick != 50 {
		t.Fatalf("expected power levels to update immediately on iMessage confirmation, called=%v invite=%d kick=%d", setPLCalled, gotInvite, gotKick)
	}
}

func TestOverlappingDMMigrationsPreserveRoomMapping(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:overlap_dm_migration_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
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
		DB:            db,
		Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		puppets:       make(map[string]*Puppet),
		IM:            &fakeTestIMAPI{resolvedGUID: "any;-;+15559990000"},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	br.puppets["+15559990000"] = &Puppet{
		Puppet: &database.Puppet{ID: "+15559990000"},
		bridge: br,
		MXID:   "@imessage_15559990000:example.com",
		Intent: &appservice.IntentAPI{UserID: "@imessage_15559990000:example.com"},
	}

	dbPortal := db.Portal.New()
	dbPortal.GUID = "iMessage;-;+15559990000"
	dbPortal.MXID = id.RoomID("!keep-this-room:example.com")
	dbPortal.Insert(nil)

	// Run overlapping concurrent bindExistingDMPortal and duplicate reIDInto calls.
	const workers = 8
	start := make(chan struct{})
	done := make(chan *Portal, workers)
	for i := 0; i < workers; i++ {
		go func() {
			<-start
			done <- br.bindExistingDMPortal("any;-;+15559990000", "iMessage")
		}()
	}
	close(start)

	for i := 0; i < workers; i++ {
		p := <-done
		if p == nil || p.MXID != "!keep-this-room:example.com" || p.GUID != "any;-;+15559990000" {
			t.Fatalf("overlapping migration returned unexpected portal: %+v", p)
		}
	}

	// Direct duplicate reIDInto on the already-migrated portal must also be a safe no-op.
	migrated := br.GetPortalByGUID("any;-;+15559990000")
	if !migrated.reIDInto("any;-;+15559990000", nil, true, false) {
		t.Fatal("duplicate reIDInto to same GUID should return true without deleting portal")
	}

	// Verify SQLite still holds the original room mapping under both GUIDs.
	if row := db.Portal.GetByGUID("any;-;+15559990000"); row == nil || row.MXID != "!keep-this-room:example.com" {
		t.Fatalf("expected DB row for any;-;+15559990000 with original room, got %+v", row)
	}
	if row := db.Portal.GetByGUID("iMessage;-;+15559990000"); row == nil || row.MXID != "!keep-this-room:example.com" {
		t.Fatalf("expected DB alias for iMessage;-;+15559990000 with original room, got %+v", row)
	}
}

func TestReIDIntoAliasingScopedToSequoiaAny(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:reid_scope_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	newBridge := func() *IMBridge {
		zlog := zerolog.Nop()
		b := &IMBridge{
			DB:            db,
			Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
			portalsByGUID: make(map[string]*Portal),
			portalsByMXID: make(map[id.RoomID]*Portal),
			IM:            &fakeTestIMAPI{},
		}
		b.Log = log.DefaultLogger
		b.ZLog = &zlog
		return b
	}

	// 1. Non-Sequoia private-chat re-ID (e.g. legacy mac/iOS ReIDPortal or DisableSMSPortals toggle)
	//    must NOT keep the old GUID aliased in portalsByGUID or merged_chat.
	br1 := newBridge()
	legacyRow := db.Portal.New()
	legacyRow.GUID = "iMessage;-;+15550001111"
	legacyRow.Insert(nil)

	legacyPortal := br1.GetPortalByGUID("iMessage;-;+15550001111")
	defer legacyPortal.stopLoop()
	if !br1.ReIDPortal("iMessage;-;+15550001111", "SMS;-;+15550001111", false) {
		t.Fatal("expected legacy ReIDPortal to succeed")
	}
	if _, stillAliased := br1.portalsByGUID["iMessage;-;+15550001111"]; stillAliased {
		t.Fatal("expected old GUID not to remain aliased in portalsByGUID for non-Sequoia re-ID")
	}
	if br1.portalsByGUID["SMS;-;+15550001111"] != legacyPortal {
		t.Fatal("expected new GUID to map to portal in portalsByGUID")
	}
	// Reload from SQLite: new GUID resolves, old GUID does NOT resolve.
	if row := db.Portal.GetByGUID("SMS;-;+15550001111"); row == nil || row.GUID != "SMS;-;+15550001111" {
		t.Fatalf("expected DB lookup for new GUID SMS;-;+15550001111 to succeed, got %+v", row)
	}
	if row := db.Portal.GetByGUID("iMessage;-;+15550001111"); row != nil {
		t.Fatalf("expected old GUID iMessage;-;+15550001111 not to remain aliased in merged_chat, got %+v", row)
	}

	// 2. Sequoia "any;-;" migration MUST preserve the old GUID alias in both memory and merged_chat.
	seqRow := db.Portal.New()
	seqRow.GUID = "iMessage;-;+15550002222"
	seqRow.Insert(nil)

	seqPortal := br1.GetPortalByGUID("iMessage;-;+15550002222")
	defer seqPortal.stopLoop()
	if !seqPortal.reIDInto("any;-;+15550002222", nil, true, false) {
		t.Fatal("expected Sequoia any;-; reIDInto to succeed")
	}
	if br1.portalsByGUID["iMessage;-;+15550002222"] != seqPortal || br1.portalsByGUID["any;-;+15550002222"] != seqPortal {
		t.Fatal("expected Sequoia any;-; migration to keep both old and new GUIDs in portalsByGUID")
	}
	if row := db.Portal.GetByGUID("any;-;+15550002222"); row == nil || row.GUID != "any;-;+15550002222" {
		t.Fatalf("expected DB lookup for any;-;+15550002222 to succeed, got %+v", row)
	}
	if row := db.Portal.GetByGUID("iMessage;-;+15550002222"); row == nil || row.GUID != "any;-;+15550002222" {
		t.Fatalf("expected DB alias for iMessage;-;+15550002222 -> any;-;+15550002222, got %+v", row)
	}
}

func TestDeletedAndReplacedPortalsStopMessageLoop(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:portal_stop_loop_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
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
		DB:            db,
		Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		user:          &User{User: &database.User{MXID: "@me:example.com"}},
		IM:            &fakeTestIMAPI{},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog

	// 1. Direct Delete() stops handleMessageLoop cleanly and is idempotent.
	p1 := br.GetPortalByGUID("iMessage;-;+15553000001")
	p1.Delete()
	select {
	case <-p1.loopDone:
	case <-time.After(time.Second):
		t.Fatal("deleted portal handleMessageLoop did not exit cleanly")
	}
	// Second Delete() must not panic (sync.Once protects close(portal.stop)).
	p1.Delete()
	// Sending an event after stop must not block even when MatrixMessages buffer is full.
	for i := 0; i < cap(p1.MatrixMessages)+5; i++ {
		p1.ReceiveMatrixEvent(nil, &event.Event{ID: id.EventID("$evt")})
	}

	// 2. ReIDPortal replacing an empty target portal stops the replaced target's loop,
	//    while the surviving portal's loop continues running.
	mxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"event_id":"$state-evt"}`))
	}))
	defer mxSrv.Close()

	as := appservice.Create()
	as.HomeserverDomain = "example.com"
	as.Registration = &appservice.Registration{AppToken: "token"}
	as.StateStore.MarkRegistered("@imessage_15553000002:example.com")
	as.StateStore.SetMembership("!room-src:example.com", "@imessage_15553000002:example.com", event.MembershipJoin)
	mxClient, err := mautrix.NewClient(mxSrv.URL, "@imessage_15553000002:example.com", "token")
	if err != nil {
		t.Fatal(err)
	}
	puppetIntent := as.NewIntentAPI("imessage_15553000002")
	puppetIntent.Client = mxClient
	puppetIntent.UserID = "@imessage_15553000002:example.com"
	puppetIntent.IsCustomPuppet = true
	br.Bot = &appservice.IntentAPI{UserID: "@bot:example.com"}
	br.puppets = map[string]*Puppet{
		"+15553000002": {
			Puppet: &database.Puppet{ID: "+15553000002"},
			bridge: br,
			MXID:   "@imessage_15553000002:example.com",
			Intent: puppetIntent,
		},
	}

	sourceDB := db.Portal.New()
	sourceDB.GUID = "iMessage;-;+15553000002"
	sourceDB.MXID = "!room-src:example.com"
	sourceDB.Insert(nil)

	emptyTargetDB := db.Portal.New()
	emptyTargetDB.GUID = "SMS;-;+15553000002"
	emptyTargetDB.Insert(nil)

	sourcePortal := br.GetPortalByGUID("iMessage;-;+15553000002")
	defer sourcePortal.stopLoop()
	emptyTargetPortal := br.GetPortalByGUID("SMS;-;+15553000002")

	if !br.ReIDPortal("iMessage;-;+15553000002", "SMS;-;+15553000002", false) {
		t.Fatal("expected ReIDPortal replacing empty target portal to succeed")
	}
	select {
	case <-emptyTargetPortal.loopDone:
	case <-time.After(time.Second):
		t.Fatal("replaced empty target portal handleMessageLoop did not exit cleanly")
	}
	select {
	case <-sourcePortal.loopDone:
		t.Fatal("surviving re-ID'd portal handleMessageLoop should still be running")
	default:
	}

	// 3. ReIDPortal where old portal has no room and target portal already exists:
	//    old portal is deleted and its message loop MUST stop, while target stays alive.
	oldNoRoom := br.GetPortalByGUID("iMessage;-;+15553000003")
	existingTarget := br.GetPortalByGUID("SMS;-;+15553000003")
	defer existingTarget.stopLoop()

	if br.ReIDPortal("iMessage;-;+15553000003", "SMS;-;+15553000003", false) {
		t.Fatal("expected ReIDPortal to return false when target already exists and old portal is nuked")
	}
	select {
	case <-oldNoRoom.loopDone:
	case <-time.After(time.Second):
		t.Fatal("nuked old portal handleMessageLoop did not exit cleanly")
	}
	select {
	case <-existingTarget.loopDone:
		t.Fatal("surviving target portal handleMessageLoop should still be running")
	default:
	}

	// 4. Verify secondary GUID deletion removes key from portalsByGUID (doesn't leave nil entry).
	pSec := br.GetPortalByGUID("any;-;+15553000004")
	pSec.addSecondaryGUIDs([]string{"iMessage;-;+15553000004"})
	br.portalsLock.Lock()
	br.portalsByGUID["iMessage;-;+15553000004"] = pSec
	br.portalsLock.Unlock()
	pSec.Delete()
	select {
	case <-pSec.loopDone:
	case <-time.After(time.Second):
		t.Fatal("deleted portal with secondary GUID did not stop its loop")
	}
	if _, exists := br.portalsByGUID["iMessage;-;+15553000004"]; exists {
		t.Fatal("expected deleted secondary GUID to be removed from portalsByGUID instead of set to nil")
	}
}

type recordingBridgeResultIMAPI struct {
	fakeTestIMAPI
	mu      sync.Mutex
	results []struct {
		chatID    string
		messageID string
		success   bool
	}
}

func (r *recordingBridgeResultIMAPI) SendMessageBridgeResult(chatID, messageID string, eventID id.EventID, success bool) {
	r.mu.Lock()
	r.results = append(r.results, struct {
		chatID    string
		messageID string
		success   bool
	}{chatID: chatID, messageID: messageID, success: success})
	r.mu.Unlock()
}

func pausePortalLoop(t *testing.T, portal *Portal) func() {
	t.Helper()
	portal.backfillLock.Lock()
	portal.backfillWait.Add(1)
	select {
	case portal.backfillStart <- struct{}{}:
	case <-time.After(2 * time.Second):
		portal.backfillWait.Done()
		portal.backfillLock.Unlock()
		t.Fatal("timed out waiting for handleMessageLoop to acknowledge pause")
	}
	return func() {
		portal.backfillWait.Done()
		portal.backfillLock.Unlock()
	}
}

func TestEnqueueSynchronizedWithShutdownNoStrandedMessages(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:enqueue_shutdown_sync_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	recIM := &recordingBridgeResultIMAPI{}
	zlog := zerolog.Nop()
	br := &IMBridge{
		DB:            db,
		Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		IM:            recIM,
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog

	portal := br.GetPortalByGUID("iMessage;-;+15554000001")
	// Pause the message loop deterministically so queued messages remain buffered until shutdown drains them.
	unpause := pausePortalLoop(t, portal)

	tmpDir := t.TempDir()
	const totalSenders = 40
	attPaths := make([]string, totalSenders)
	for i := 0; i < totalSenders; i++ {
		p := filepath.Join(tmpDir, fmt.Sprintf("att-%d.bin", i))
		if err := os.WriteFile(p, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		attPaths[i] = p
	}

	// Race senders holding the old portal across stopLoop() and after loopDone closes.
	var accepted, rejected atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < totalSenders; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			msg := &imessage.Message{
				GUID:        fmt.Sprintf("msg-%d", i),
				ChatGUID:    portal.GUID,
				Attachments: []*imessage.Attachment{{GUID: fmt.Sprintf("a-%d", i), PathOnDisk: attPaths[i], IsTemp: true}},
			}
			ok, stopped := portal.enqueueMessage(msg, time.Second)
			if ok {
				accepted.Add(1)
			} else {
				if !stopped {
					t.Errorf("expected stopped=true on rejection for msg-%d", i)
				}
				rejected.Add(1)
				msg.CleanupTempAttachments()
			}
		}()
	}

	close(start)
	portal.stopLoop()
	unpause()
	<-portal.loopDone
	wg.Wait()

	// After loopDone closes, a sender still holding the old portal MUST be rejected cleanly.
	latePath := filepath.Join(tmpDir, "late.bin")
	if err := os.WriteFile(latePath, []byte("late"), 0600); err != nil {
		t.Fatal(err)
	}
	lateMsg := &imessage.Message{
		GUID:        "msg-late",
		ChatGUID:    portal.GUID,
		Attachments: []*imessage.Attachment{{GUID: "a-late", PathOnDisk: latePath, IsTemp: true}},
	}
	if ok, stopped := portal.enqueueMessage(lateMsg, time.Second); ok || !stopped {
		t.Fatalf("expected late enqueue on stopped portal to return (false, true), got (%v, %v)", ok, stopped)
	}

	// Zero stranded messages may remain in portal.Messages.
	if rem := len(portal.Messages); rem != 0 {
		t.Fatalf("expected 0 stranded messages in portal.Messages after loopDone, got %d", rem)
	}

	// Every accepted message must have been accounted for by the final drain (temp file deleted + failure result).
	recIM.mu.Lock()
	gotResults := len(recIM.results)
	for _, res := range recIM.results {
		if res.success {
			t.Fatalf("expected drained message result to report success=false, got %+v", res)
		}
	}
	recIM.mu.Unlock()
	if int32(gotResults) != accepted.Load() {
		t.Fatalf("expected %d failed bridge results for accepted messages, got %d (rejected=%d)", accepted.Load(), gotResults, rejected.Load())
	}
	for i, p := range attPaths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("temp attachment %d (%s) was leaked, stat err=%v", i, p, err)
		}
	}
}

func TestDrainedEditClearsBlueBubblesInflightState(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:drained_edit_bb_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	wsSendCh := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": 200,
				"data":   map[string]any{"private_api": true, "server_version": "1.9.7"},
			})
		case "/api/v1/contact":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "data": []any{}})
		case "/socket.io/":
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer ws.Close()
			_, _, _ = ws.ReadMessage()
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"s1","pingInterval":25000,"pingTimeout":20000}`))
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`40`))
			for frame := range wsSendCh {
				if err := ws.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	defer close(wsSendCh)

	zlog := zerolog.Nop()
	br := &IMBridge{
		DB: db,
		Config: &config.Config{
			BaseConfig: &bridgeconfig.BaseConfig{},
			IMessage: imessage.PlatformConfig{
				Platform:            "bluebubbles",
				BlueBubblesURL:      srv.URL,
				BlueBubblesPassword: "pw",
			},
		},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		user:          &User{User: &database.User{MXID: "@me:example.com"}},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	br.AS = appservice.Create()

	bbAPI, err := bluebubbles.NewBlueBubblesConnector(br)
	if err != nil {
		t.Fatalf("NewBlueBubblesConnector failed: %v", err)
	}
	br.IM = bbAPI

	ready := make(chan struct{})
	if err := bbAPI.Start(func() { close(ready) }); err != nil {
		t.Fatalf("bbAPI.Start failed: %v", err)
	}
	defer bbAPI.Stop()
	<-ready

	portal := br.GetPortalByGUID("iMessage;-;+15554000002")
	// Pause handleMessageLoop deterministically so the queued edit is discarded by the drain on stop.
	unpause := pausePortalLoop(t, portal)

	baseMs := time.Now().Add(-time.Minute).UnixMilli()
	editFrame := fmt.Sprintf(`42["updated-message",{"guid":"edit-drain-1","isFromMe":true,"text":"edited text","dateCreated":%d,"dateEdited":%d,"chats":[{"guid":"iMessage;-;+15554000002"}]}]`, baseMs, baseMs+5000)
	wsSendCh <- editFrame

	var firstEdit *imessage.Message
	select {
	case firstEdit = <-bbAPI.MessageChan():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial edit from BlueBubbles")
	}

	if ok, _ := portal.enqueueMessage(firstEdit, time.Second); !ok {
		t.Fatal("failed to enqueue edit onto portal")
	}

	// Stop the portal and release backfillWait so the drain discards firstEdit and
	// reports SendMessageBridgeResult(..., false) back to BlueBubbles.
	portal.Delete()
	unpause()
	select {
	case <-portal.loopDone:
	case <-time.After(time.Second):
		t.Fatal("portal handleMessageLoop did not exit after Delete")
	}

	// Retry the exact same edit timestamp: BlueBubbles must have cleared inflightEdits
	// and MUST emit the edit again.
	wsSendCh <- editFrame
	select {
	case retried := <-bbAPI.MessageChan():
		if retried.GUID != "edit-drain-1" || !retried.IsEdited || retried.Text != "edited text" {
			t.Fatalf("unexpected retried edit: %+v", retried)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected BlueBubbles to accept retried edit timestamp after drained portal reported failure")
	}
}

func TestReIDIntoFailurePreservesSurvivingPortalLookup(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:reid_fail_preserve_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
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
		DB:            db,
		Config:        &config.Config{BaseConfig: &bridgeconfig.BaseConfig{}},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		IM:            &fakeTestIMAPI{},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog

	portal := br.GetPortalByGUID("iMessage;-;+15555000001")
	defer portal.stopLoop()

	// Force UPDATE portal SET guid=... to fail via a SQLite trigger.
	if _, err := rawDB.Exec(`CREATE TRIGGER block_portal_reid BEFORE UPDATE OF guid ON portal BEGIN SELECT RAISE(ABORT, 'simulated reid failure'); END;`); err != nil {
		t.Fatalf("failed to create abort trigger: %v", err)
	}

	if br.ReIDPortal("iMessage;-;+15555000001", "SMS;-;+15555000001", false) {
		t.Fatal("expected ReIDPortal to return false when DB update fails")
	}

	if portal.GUID != "iMessage;-;+15555000001" {
		t.Fatalf("expected surviving portal GUID to stay unchanged, got %q", portal.GUID)
	}
	if br.portalsByGUID["iMessage;-;+15555000001"] != portal {
		t.Fatal("expected surviving portal to remain cached under old GUID")
	}
	if _, hasNew := br.portalsByGUID["SMS;-;+15555000001"]; hasNew {
		t.Fatal("expected failed target GUID not to be cached in portalsByGUID")
	}
	// Fresh DB lookup (simulating restart) MUST still resolve the surviving portal via merged_chat!
	if row := db.Portal.GetByGUID("iMessage;-;+15555000001"); row == nil || row.GUID != "iMessage;-;+15555000001" {
		t.Fatalf("expected fresh DB lookup for surviving portal to succeed after failed ReID, got %+v", row)
	}

	// Also verify atomic rollback when replacing an empty target portal fails on DB UPDATE.
	srcRow := db.Portal.New()
	srcRow.GUID = "iMessage;-;+15555000002"
	srcRow.MXID = "!src-room:example.com"
	srcRow.Insert(nil)
	srcPortal := br.GetPortalByGUID("iMessage;-;+15555000002")
	defer srcPortal.stopLoop()

	emptyTarget := br.GetPortalByGUID("SMS;-;+15555000002")
	defer emptyTarget.stopLoop()

	if br.ReIDPortal("iMessage;-;+15555000002", "SMS;-;+15555000002", false) {
		t.Fatal("expected ReIDPortal replacing empty target to return false when DB update fails")
	}
	if row := db.Portal.GetByGUID("SMS;-;+15555000002"); row == nil {
		t.Fatal("expected empty target portal row to survive via rollback when DB ReID fails")
	}
	select {
	case <-emptyTarget.loopDone:
		t.Fatal("expected empty target portal loop to remain running when DB ReID fails")
	default:
	}
}

func TestMergeTwoRoomFallbackEvictsRetiredMXIDAndForwardsQueuedMessages(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:merge_two_room_fallback_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	var mu sync.Mutex
	sentRooms := make([]id.RoomID, 0)
	sentCh := make(chan id.RoomID, 4)
	mxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/send/m.room.message/") {
			parts := strings.Split(r.URL.Path, "/")
			for i, p := range parts {
				if p == "rooms" && i+1 < len(parts) {
					roomID := id.RoomID(parts[i+1])
					mu.Lock()
					sentRooms = append(sentRooms, roomID)
					mu.Unlock()
					sentCh <- roomID
					break
				}
			}
			_, _ = w.Write([]byte(`{"event_id":"$merged-evt-1"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer mxSrv.Close()

	makeIntent := func(mxid id.UserID, rooms ...id.RoomID) *appservice.IntentAPI {
		as := appservice.Create()
		as.HomeserverDomain = "example.com"
		as.Registration = &appservice.Registration{AppToken: "token"}
		client, err := mautrix.NewClient(mxSrv.URL, mxid, "token")
		if err != nil {
			t.Fatal(err)
		}
		intent := as.NewIntentAPI(mxid.Localpart())
		intent.Client = client
		intent.UserID = mxid
		intent.IsCustomPuppet = true
		as.StateStore.MarkRegistered(mxid)
		for _, room := range rooms {
			as.StateStore.SetMembership(room, mxid, event.MembershipJoin)
		}
		return intent
	}

	survivorRoom := id.RoomID("!survivor:example.com")
	retiredRoom := id.RoomID("!retired:example.com")

	zlog := zerolog.Nop()
	br := &IMBridge{
		DB: db,
		Config: &config.Config{
			BaseConfig: &bridgeconfig.BaseConfig{
				Homeserver: bridgeconfig.HomeserverConfig{Software: bridgeconfig.SoftwareStandard},
			},
		},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		puppets:       make(map[string]*Puppet),
		user:          &User{User: &database.User{MXID: "@me:example.com"}},
		IM:            &fakeTestIMAPI{},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	br.SpecVersions = mautrix.RespVersions{}
	br.Bot = makeIntent("@bot:example.com", survivorRoom, retiredRoom)
	br.puppets["+15556000001"] = &Puppet{
		Puppet: &database.Puppet{ID: "+15556000001", Displayname: "Alice"},
		bridge: br,
		MXID:   "@imessage_15556000001:example.com",
		Intent: makeIntent("@imessage_15556000001:example.com", survivorRoom, retiredRoom),
	}
	br.puppets["alice@example.com"] = &Puppet{
		Puppet: &database.Puppet{ID: "alice@example.com", Displayname: "Alice"},
		bridge: br,
		MXID:   "@imessage_alice:example.com",
		Intent: makeIntent("@imessage_alice:example.com", survivorRoom, retiredRoom),
	}

	survRow := db.Portal.New()
	survRow.GUID = "iMessage;-;+15556000001"
	survRow.MXID = survivorRoom
	survRow.Insert(nil)

	retRow := db.Portal.New()
	retRow.GUID = "iMessage;-;alice@example.com"
	retRow.MXID = retiredRoom
	retRow.Insert(nil)

	survivor := br.GetPortalByGUID(survRow.GUID)
	defer survivor.stopLoop()
	retired := br.GetPortalByGUID(retRow.GUID)

	// Buffer a message on retired while its loop is deterministically paused so it is guaranteed
	// to be buffered at merge time even under GOMAXPROCS=1.
	unpauseRetired := pausePortalLoop(t, retired)
	queuedMsg := &imessage.Message{
		GUID:     "queued-during-merge-1",
		ChatGUID: retired.GUID,
		Sender:   imessage.Identifier{LocalID: "+15556000001", Service: "iMessage"},
		Service:  "iMessage",
		Text:     "arrived during merge",
		Time:     time.Now(),
	}
	if ok, _ := retired.enqueueMessage(queuedMsg, time.Second); !ok {
		t.Fatal("failed to queue message on retired portal")
	}

	survivor.Merge([]*Portal{retired})
	unpauseRetired()

	select {
	case <-retired.loopDone:
	case <-time.After(time.Second):
		t.Fatal("retired portal handleMessageLoop did not stop after Merge")
	}

	// Survivor remains reachable; retired room MXID lookup MUST be empty.
	if got := br.GetPortalByMXID(survivorRoom); got != survivor {
		t.Fatalf("expected survivor room lookup to return survivor portal, got %+v", got)
	}
	if got := br.GetPortalByMXID(retiredRoom); got != nil {
		t.Fatalf("expected retired room lookup to return nil after fallback merge, got %+v", got)
	}
	if got := br.GetPortalByGUID(retRow.GUID); got != survivor {
		t.Fatalf("expected retired GUID lookup to resolve to survivor portal, got %+v", got)
	}

	// The buffered message from retired MUST be forwarded to and sent into survivorRoom.
	select {
	case gotRoom := <-sentCh:
		if gotRoom != survivorRoom {
			t.Fatalf("expected queued message to be delivered to survivor room %s, got %s", survivorRoom, gotRoom)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for merged portal to deliver message buffered on retired portal")
	}
}

func TestReIDIntoNukedOldPortalForwardsQueuedMessages(t *testing.T) {
	rawDB, err := dbutil.NewWithDialect("file:reid_nuke_forward_test?mode=memory&cache=shared&_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	defer rawDB.Close()

	db := database.New(rawDB, log.DefaultLogger)
	if err := db.Upgrade(); err != nil {
		t.Fatalf("database upgrade failed: %v", err)
	}

	oldRoom := id.RoomID("!reid-old:example.com")
	newRoom := id.RoomID("!reid-new:example.com")
	sentRooms := make(chan id.RoomID, 4)
	mxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/send/m.room.message/") {
			parts := strings.Split(r.URL.Path, "/")
			for i, p := range parts {
				if p == "rooms" && i+1 < len(parts) {
					sentRooms <- id.RoomID(parts[i+1])
					break
				}
			}
			_, _ = w.Write([]byte(`{"event_id":"$reid-fwd-evt-1"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer mxSrv.Close()

	makeIntent := func(mxid id.UserID) *appservice.IntentAPI {
		as := appservice.Create()
		as.HomeserverDomain = "example.com"
		as.Registration = &appservice.Registration{AppToken: "token"}
		client, err := mautrix.NewClient(mxSrv.URL, mxid, "token")
		if err != nil {
			t.Fatal(err)
		}
		intent := as.NewIntentAPI(mxid.Localpart())
		intent.Client = client
		intent.UserID = mxid
		intent.IsCustomPuppet = true
		as.StateStore.MarkRegistered(mxid)
		as.StateStore.SetMembership(oldRoom, mxid, event.MembershipJoin)
		as.StateStore.SetMembership(newRoom, mxid, event.MembershipJoin)
		return intent
	}

	zlog := zerolog.Nop()
	br := &IMBridge{
		DB: db,
		Config: &config.Config{
			BaseConfig: &bridgeconfig.BaseConfig{
				Homeserver: bridgeconfig.HomeserverConfig{Software: bridgeconfig.SoftwareStandard},
			},
		},
		portalsByGUID: make(map[string]*Portal),
		portalsByMXID: make(map[id.RoomID]*Portal),
		puppets:       make(map[string]*Puppet),
		user:          &User{User: &database.User{MXID: "@me:example.com"}},
		IM:            &fakeTestIMAPI{},
	}
	br.Log = log.DefaultLogger
	br.ZLog = &zlog
	br.SpecVersions = mautrix.RespVersions{}
	br.Bot = makeIntent("@bot:example.com")
	br.puppets["+15558000001"] = &Puppet{
		Puppet: &database.Puppet{ID: "+15558000001", Displayname: "Alice"},
		bridge: br,
		MXID:   "@imessage_15558000001:example.com",
		Intent: makeIntent("@imessage_15558000001:example.com"),
	}
	br.puppets["a@example.com"] = &Puppet{
		Puppet: &database.Puppet{ID: "a@example.com", Displayname: "Alice"},
		bridge: br,
		MXID:   "@imessage_a_example.com:example.com",
		Intent: makeIntent("@imessage_a_example.com:example.com"),
	}

	oldRow := db.Portal.New()
	oldRow.GUID = "iMessage;-;a@example.com"
	oldRow.MXID = oldRoom
	oldRow.Insert(nil)

	newRow := db.Portal.New()
	newRow.GUID = "iMessage;-;+15558000001"
	newRow.MXID = newRoom
	newRow.Insert(nil)

	oldPortal := br.GetPortalByGUID(oldRow.GUID)
	newPortal := br.GetPortalByGUID(newRow.GUID)
	defer newPortal.stopLoop()

	unpauseOld := pausePortalLoop(t, oldPortal)
	if ok, _ := oldPortal.enqueueMessage(&imessage.Message{
		GUID:     "reid-queued-1",
		ChatGUID: oldPortal.GUID,
		Sender:   imessage.Identifier{LocalID: "+15558000001", Service: "iMessage"},
		Service:  "iMessage",
		Text:     "queued before reid nuke",
		Time:     time.Now(),
	}, time.Second); !ok {
		t.Fatal("failed to queue message on oldPortal")
	}

	if br.ReIDPortal(oldRow.GUID, newRow.GUID, false) {
		t.Fatal("expected ReIDPortal to return false when nuking old portal in favor of existing target")
	}
	unpauseOld()
	select {
	case <-oldPortal.loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("oldPortal did not stop after being nuked in ReIDPortal")
	}

	select {
	case gotRoom := <-sentRooms:
		if gotRoom != newRoom {
			t.Fatalf("expected queued message to be forwarded to %s, got %s", newRoom, gotRoom)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queued message to be forwarded to existing target portal")
	}
}

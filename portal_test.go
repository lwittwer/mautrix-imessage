package main

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

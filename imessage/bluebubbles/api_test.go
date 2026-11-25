package bluebubbles

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	log "maunium.net/go/maulogger/v2"

	"go.mau.fi/mautrix-imessage/imessage"
	"go.mau.fi/mautrix-imessage/ipc"
)

type fakeBridge struct {
	cfg              *imessage.PlatformConfig
	zlog             zerolog.Logger
	mu               sync.Mutex
	statuses         []imessage.BridgeStatus
	messagesSince    map[string][]string
	kv               map[string]string
	captionInMessage bool
}

func newFakeBridge(url, password string) *fakeBridge {
	return &fakeBridge{
		cfg: &imessage.PlatformConfig{
			Platform:            "bluebubbles",
			BlueBubblesURL:      url,
			BlueBubblesPassword: password,
		},
		zlog: zerolog.Nop(),
	}
}

func (f *fakeBridge) GetIPC() *ipc.Processor                       { return nil }
func (f *fakeBridge) GetLog() log.Logger                           { return log.DefaultLogger }
func (f *fakeBridge) GetZLog() *zerolog.Logger                     { return &f.zlog }
func (f *fakeBridge) GetConnectorConfig() *imessage.PlatformConfig { return f.cfg }
func (f *fakeBridge) PingServer() (time.Time, time.Time, time.Time) {
	now := time.Now()
	return now, now, now
}
func (f *fakeBridge) SendBridgeStatus(state imessage.BridgeStatus) {
	f.mu.Lock()
	f.statuses = append(f.statuses, state)
	f.mu.Unlock()
}
func (f *fakeBridge) ReIDPortal(oldGUID, newGUID string, mergeExisting bool) bool { return false }
func (f *fakeBridge) GetMessagesSince(chatGUID string, since time.Time) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.messagesSince == nil {
		return nil
	}
	return append([]string(nil), f.messagesSince[chatGUID]...)
}
func (f *fakeBridge) SetPushKey(req *imessage.PushKeyRequest) {}
func (f *fakeBridge) GetKV(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.kv == nil {
		return ""
	}
	return f.kv[key]
}
func (f *fakeBridge) SetKV(key, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.kv == nil {
		f.kv = make(map[string]string)
	}
	f.kv[key] = value
}
func (f *fakeBridge) DeleteKV(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.kv, key)
}
func (f *fakeBridge) DeleteKVPrefixOlderThan(prefix string, cutoff int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var deleted int64
	for k, v := range f.kv {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		ts, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		if ts < cutoff {
			delete(f.kv, k)
			deleted++
		}
	}
	return deleted, nil
}
func (f *fakeBridge) IsCaptionInMessage() bool {
	return f.captionInMessage
}

func (f *fakeBridge) getStatuses() []imessage.BridgeStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]imessage.BridgeStatus, len(f.statuses))
	copy(out, f.statuses)
	return out
}

func newTestConnector(t *testing.T, serverURL string) (*blueBubbles, *fakeBridge) {
	t.Helper()
	fb := newFakeBridge(serverURL, "secret-pw")
	api, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatalf("NewBlueBubblesConnector failed: %v", err)
	}
	return api.(*blueBubbles), fb
}

var upstreamAllowedReactions = map[string]bool{
	"love":       true,
	"like":       true,
	"dislike":    true,
	"laugh":      true,
	"emphasize":  true,
	"question":   true,
	"-love":      true,
	"-like":      true,
	"-dislike":   true,
	"-laugh":     true,
	"-emphasize": true,
	"-question":  true,
}

func TestContactConversionAndMatching(t *testing.T) {
	bb, _ := newTestConnector(t, "http://localhost:1234")

	// Nil avatar should NOT panic
	c, err := bb.convertBBContactToiMessageContact(&Contact{
		FirstName:   "Alice",
		LastName:    "Smith",
		DisplayName: "Alice Smith",
		Avatar:      nil,
		ID:          float64(42),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.UserGUID != "42" || len(c.Avatar) != 0 {
		t.Fatalf("unexpected contact: %+v", c)
	}

	// Valid base64 avatar
	rawImg := []byte("fake-png-bytes")
	encoded := base64.StdEncoding.EncodeToString(rawImg)
	c2, err := bb.convertBBContactToiMessageContact(&Contact{
		FirstName: "Bob",
		Avatar:    &encoded,
		ID:        "bob-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(c2.Avatar) != string(rawImg) {
		t.Fatalf("expected decoded avatar, got %q", string(c2.Avatar))
	}

	// Matching: case-insensitive email, phone suffix, db preference, and no false match on empty numeric
	bb.contacts = []Contact{
		{
			FirstName:    "API",
			LastName:     "User",
			SourceType:   "api",
			PhoneNumbers: []PhoneNumber{{Address: "(555) 123-4567"}},
			Emails:       []Email{{Address: "Alice@Example.com"}},
		},
		{
			FirstName:    "DB",
			LastName:     "User",
			SourceType:   "db",
			PhoneNumbers: []PhoneNumber{{Address: "+1 555-123-4567"}},
		},
		{
			FirstName:    "ShortCode",
			LastName:     "Target",
			SourceType:   "db",
			PhoneNumbers: []PhoneNumber{{Address: "+1 (555) 126-2966"}},
		},
	}

	matched := bb.matchHandleToContact("+15551234567")
	if matched == nil || matched.FirstName != "DB" {
		t.Fatalf("expected DB contact to win over API contact, got %+v", matched)
	}

	// 10-digit national number without country code should still match +1-prefixed contact.
	matchedLocal := bb.matchHandleToContact("5551234567")
	if matchedLocal == nil || matchedLocal.FirstName != "DB" {
		t.Fatalf("expected 10-digit local number to match +1 contact, got %+v", matchedLocal)
	}

	// SMS short code (e.g. 262966) must NOT falsely match a contact whose number ends in 262966.
	if shortCodeMatch := bb.matchHandleToContact("262966"); shortCodeMatch != nil {
		t.Fatalf("expected short-code sender '262966' not to match any contact, got %+v", shortCodeMatch)
	}

	matchedEmail := bb.matchHandleToContact("alice@example.com")
	if matchedEmail == nil || matchedEmail.FirstName != "API" {
		t.Fatalf("expected case-insensitive email match, got %+v", matchedEmail)
	}

	// Non-numeric non-email address should not falsely match phones
	if bogus := bb.matchHandleToContact("not-a-phone"); bogus != nil {
		t.Fatalf("expected nil match for non-numeric non-email, got %+v", bogus)
	}

	// SearchContactList trims whitespace
	found, err := bb.SearchContactList("  alice@example.com  ")
	if err != nil || len(found) != 1 {
		t.Fatalf("expected 1 search result, got %d (err=%v)", len(found), err)
	}
}

func TestConvertBBMessageEdgeCases(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/attachment/att-ok":
			_ = json.NewEncoder(w).Encode(AttachmentResponse{
				Status: 200,
				Data:   Attachment{GUID: "att-ok", TransferName: "photo.jpg", MimeType: "image/jpeg"},
			})
		case "/api/v1/attachment/att-ok/download":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("jpegdata"))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"status":404,"message":"not found"}`))
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)

	// 1. Empty Chats returns error instead of panicking
	if _, err := bb.convertBBMessageToiMessage(Message{GUID: "msg-no-chat"}); err == nil {
		t.Fatal("expected error when message has no chats")
	}

	// 2. Shadowed attachment error does not panic and does not leave nil in Attachments;
	//    ThreadOriginatorPart parses part index; ThreadID uses Chats[0].GroupID (not ThreadOriginatorGUID).
	msg, err := bb.convertBBMessageToiMessage(Message{
		GUID:                 "msg-1",
		DateCreated:          1700000000000,
		Text:                 "hello",
		Chats:                []Chat{{GUID: "iMessage;+;chat123", GroupID: "group-uuid-1"}},
		Handle:               Handle{Address: "+15551234567", Service: "iMessage"},
		ThreadOriginatorGUID: "reply-target-guid",
		ThreadOriginatorPart: "2:0:17",
		Attachments: []Attachment{
			{GUID: "att-missing"},
			{GUID: "att-ok"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msg.Attachments) != 2 {
		t.Fatalf("expected 2 attachments preserving source part indices, got %d (%+v)", len(msg.Attachments), msg.Attachments)
	}
	if msg.Attachments[0] == nil || msg.Attachments[0].GUID != "att-missing" || msg.Attachments[0].DownloadError == "" {
		t.Fatalf("expected failed placeholder at index 0, got %+v", msg.Attachments[0])
	}
	if _, err := msg.Attachments[0].Read(); err == nil {
		t.Fatal("expected Read() on failed attachment placeholder to return error")
	}
	if msg.Attachments[1] == nil || msg.Attachments[1].FileName != "photo.jpg" {
		t.Fatalf("expected surviving attachment preserved at index 1, got %+v", msg.Attachments[1])
	}
	defer os.Remove(msg.Attachments[1].PathOnDisk)

	if msg.ReplyToGUID != "reply-target-guid" || msg.ReplyToPart != 2 {
		t.Fatalf("expected reply part 2, got guid=%q part=%d", msg.ReplyToGUID, msg.ReplyToPart)
	}
	if msg.ThreadID != "group-uuid-1" {
		t.Fatalf("expected ThreadID to be chat GroupID 'group-uuid-1', got %q", msg.ThreadID)
	}

	// 3. Inbound custom emoji tapback add (2006) and remove (3006)
	var customMsg Message
	if err := json.Unmarshal([]byte(`{
		"guid": "tb-1",
		"dateCreated": 1700000001000,
		"chats": [{"guid": "iMessage;-;+15551234567"}],
		"handle": {"address": "+15551234567", "service": "iMessage"},
		"associatedMessageGuid": "p:0/msg-1",
		"associatedMessageType": 2006,
		"associatedMessageEmoji": "🔥"
	}`), &customMsg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	convertedTB, err := bb.convertBBMessageToiMessage(customMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if convertedTB.Tapback == nil || convertedTB.Tapback.Type != imessage.TapbackCustomEmoji || convertedTB.Tapback.Emoji() != "🔥" || convertedTB.Tapback.Remove {
		t.Fatalf("unexpected custom tapback: %+v", convertedTB.Tapback)
	}

	var customRemoveMsg Message
	if err := json.Unmarshal([]byte(`{
		"guid": "tb-2",
		"dateCreated": 1700000001500,
		"chats": [{"guid": "iMessage;-;+15551234567"}],
		"handle": {"address": "+15551234567", "service": "iMessage"},
		"associatedMessageGuid": "p:0/msg-1",
		"associatedMessageType": 3006,
		"associatedMessageEmoji": "🔥"
	}`), &customRemoveMsg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	convertedRemove, err := bb.convertBBMessageToiMessage(customRemoveMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if convertedRemove.Tapback == nil || convertedRemove.Tapback.Type != imessage.TapbackCustomEmoji || !convertedRemove.Tapback.Remove || convertedRemove.Tapback.Emoji() != "🔥" {
		t.Fatalf("unexpected custom tapback removal: %+v", convertedRemove.Tapback)
	}

	// Non-tapback associatedMessageType (e.g. sticker 1000) should NOT set Tapback
	nonTB, err := bb.convertBBMessageToiMessage(Message{
		GUID:                  "sticker-1",
		DateCreated:           1700000002000,
		Chats:                 []Chat{{GUID: "iMessage;-;+15551234567"}},
		AssociatedMessageGUID: "p:0/msg-1",
		AssociatedMessageType: "1000",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nonTB.Tapback != nil {
		t.Fatalf("expected nil tapback for non-tapback associatedMessageType, got %+v", nonTB.Tapback)
	}
}

func TestSendFileCreateGroupAndTapbacksUpstreamContract(t *testing.T) {
	var (
		mu                  sync.Mutex
		gotFilename         string
		gotSubject          string
		gotHasSubject       bool
		textMessageCalls    int
		gotCreateGroupReq   CreateChatRequest
		gotReactionRequests []SendReactionRequest
		allowCustomEmoji    atomic.Bool
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/message/attachment":
			if err := r.ParseMultipartForm(10 << 20); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_, fh, err := r.FormFile("attachment")
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			gotFilename = fh.Filename
			_, gotHasSubject = r.MultipartForm.Value["subject"]
			gotSubject = r.FormValue("subject")
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(SendTextResponse{
				Status: 200,
				Data:   Message{GUID: "att-msg-1", DateCreated: 1700000000000},
			})
		case "/api/v1/message/text":
			mu.Lock()
			textMessageCalls++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(SendTextResponse{
				Status: 200,
				Data:   Message{GUID: "txt-msg-1", DateCreated: 1700000000000},
			})
		case "/api/v1/chat/new":
			var req CreateChatRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			// Enforce upstream BlueBubbles Private API requirement: non-empty initial message.
			if req.Method == "private-api" && strings.TrimSpace(req.Message) == "" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"status":400,"message":"A message is required when using the Private API"}`))
				return
			}
			mu.Lock()
			gotCreateGroupReq = req
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(CreateChatResponse{
				Status: 200,
				Data:   Chat{GUID: "iMessage;+;chat999", GroupID: "grp-999"},
			})
		case "/api/v1/message/react":
			var req SendReactionRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			// Enforce upstream BlueBubbles validator unless custom emoji server support is simulated.
			if !allowCustomEmoji.Load() && !upstreamAllowedReactions[req.Reaction] {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"status":400,"message":"Reaction must be one of the 6 valid tapbacks"}`))
				return
			}
			mu.Lock()
			gotReactionRequests = append(gotReactionRequests, req)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(SendReactionResponse{
				Status: 200,
				Data:   Message{GUID: "react-guid-1", DateCreated: 1700000000000},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)

	tmpFile := filepath.Join(t.TempDir(), "vacation.png")
	if err := os.WriteFile(tmpFile, []byte("png-bytes"), 0600); err != nil {
		t.Fatal(err)
	}

	// Without Private API and empty caption: should preserve real filename and NOT call SendMessage
	bb.usingPrivateAPI.Store(false)
	resp, err := bb.SendFile("iMessage;-;+15551234567", "", "vacation.png", tmpFile, "", 0, "image/png", false, nil)
	if err != nil {
		t.Fatalf("SendFile failed: %v", err)
	}
	if resp.ChatGUID != "iMessage;-;+15551234567" || resp.Service != "iMessage" {
		t.Fatalf("unexpected SendResponse: %+v", resp)
	}
	if gotFilename != "vacation.png" {
		t.Fatalf("expected filename vacation.png, got %q", gotFilename)
	}
	if gotHasSubject {
		t.Fatal("did not expect subject field without Private API")
	}
	if textMessageCalls != 0 {
		t.Fatalf("expected 0 extra SendMessage calls for empty caption, got %d", textMessageCalls)
	}

	// With Private API and caption: should send subject
	bb.usingPrivateAPI.Store(true)
	_, err = bb.SendFile("iMessage;-;+15551234567", "look at this", "vacation.png", tmpFile, "", 0, "image/png", false, nil)
	if err != nil {
		t.Fatalf("SendFile with private API failed: %v", err)
	}
	if !gotHasSubject || gotSubject != "look at this" {
		t.Fatalf("expected subject 'look at this', got has=%v val=%q", gotHasSubject, gotSubject)
	}

	// CreateGroup without initial message must return ErrUnsupported
	if _, err := bb.CreateGroup([]string{"iMessage;-;+15551111111", "iMessage;-;+15552222222"}); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported for CreateGroup without initial message, got %v", err)
	}

	// CreateGroupWithMessage requires Private API
	bb.usingPrivateAPI.Store(false)
	if _, err := bb.CreateGroupWithMessage([]string{"iMessage;-;+15551111111", "iMessage;-;+15552222222"}, "Hi all"); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported without private API, got %v", err)
	}

	// CreateGroupWithMessage rejects blank message before hitting server
	bb.usingPrivateAPI.Store(true)
	if _, err := bb.CreateGroupWithMessage([]string{"iMessage;-;+15551111111", "iMessage;-;+15552222222"}, "   "); err == nil {
		t.Fatal("expected error when initial message is empty")
	}

	// CreateGroupWithMessage with non-empty message passes upstream validator
	grp, err := bb.CreateGroupWithMessage([]string{"iMessage;-;+15551111111", "iMessage;-;alice@example.com"}, "Hello group!")
	if err != nil {
		t.Fatalf("CreateGroupWithMessage failed: %v", err)
	}
	if grp.GUID != "iMessage;+;chat999" || grp.ThreadID != "grp-999" {
		t.Fatalf("unexpected CreateGroupResponse: %+v", grp)
	}
	if len(gotCreateGroupReq.Addresses) != 2 || gotCreateGroupReq.Addresses[0] != "+15551111111" || gotCreateGroupReq.Addresses[1] != "alice@example.com" || gotCreateGroupReq.Service != "iMessage" || gotCreateGroupReq.Message != "Hello group!" {
		t.Fatalf("unexpected CreateChatRequest: %+v", gotCreateGroupReq)
	}

	// Outbound custom emoji tapbacks are gated off by default against upstream BlueBubbles
	if bb.Capabilities().SendCustomEmojiTapbacks {
		t.Fatal("expected SendCustomEmojiTapbacks capability to be false by default")
	}
	if _, err := bb.SendCustomEmojiTapback("iMessage;-;+15551234567", "msg-1", 0, "🔥", false); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported for outbound custom emoji by default, got %v", err)
	}

	// Classic tapbacks add and remove pass upstream validator
	if _, err := bb.SendTapback("iMessage;-;+15551234567", "msg-1", 0, imessage.TapbackLove, false); err != nil {
		t.Fatalf("SendTapback add failed: %v", err)
	}
	if _, err := bb.SendTapback("iMessage;-;+15551234567", "msg-1", 0, imessage.TapbackLove, true); err != nil {
		t.Fatalf("SendTapback remove failed: %v", err)
	}

	// When server explicitly supports custom emoji reactions, gated outbound path works for add & remove
	allowCustomEmoji.Store(true)
	bb.supportsCustomEmojiTapbacks.Store(true)
	if !bb.Capabilities().SendCustomEmojiTapbacks {
		t.Fatal("expected SendCustomEmojiTapbacks capability to be true when enabled")
	}
	if _, err := bb.SendCustomEmojiTapback("iMessage;-;+15551234567", "msg-1", 0, "🔥", false); err != nil {
		t.Fatalf("SendCustomEmojiTapback add failed: %v", err)
	}
	if _, err := bb.SendCustomEmojiTapback("iMessage;-;+15551234567", "msg-1", 0, "🔥", true); err != nil {
		t.Fatalf("SendCustomEmojiTapback remove failed: %v", err)
	}
	if len(gotReactionRequests) != 4 ||
		gotReactionRequests[0].Reaction != "love" ||
		gotReactionRequests[1].Reaction != "-love" ||
		gotReactionRequests[2].Reaction != "🔥" ||
		gotReactionRequests[3].Reaction != "-🔥" {
		t.Fatalf("unexpected reaction requests: %+v", gotReactionRequests)
	}
}

func TestHTTPStatusErrorsAndBasePathURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/bb/api/v1/") {
			http.Error(w, "bad prefix: "+r.URL.Path, http.StatusNotFound)
			return
		}
		if r.URL.Path == "/bb/api/v1/server/info" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal server error"))
			return
		}
		_ = json.NewEncoder(w).Encode(ContactResponse{Status: 200, Data: []Contact{}})
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL+"/bb")
	if err := bb.RefreshContactList(); err != nil {
		t.Fatalf("expected base path '/bb' to be preserved, got error: %v", err)
	}
	var info ServerInfoResponse
	if err := bb.apiGet("/api/v1/server/info", nil, &info); err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("expected HTTP 500 error, got %v", err)
	}
}

func TestWebSocketHandshakeEventsAndReconnect(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var connCount atomic.Int32
	var gotPong atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{Status: 200, Data: ServerInfo{PrivateAPI: true}})
		case "/api/v1/contact":
			_ = json.NewEncoder(w).Encode(ContactResponse{Status: 200, Data: []Contact{}})
		case "/api/v1/chat/iMessage;-;+15551234567":
			_ = json.NewEncoder(w).Encode(ChatResponse{
				Status: 200,
				Data: &Chat{
					GUID:        "iMessage;-;+15551234567",
					LastMessage: &Message{GUID: "last-msg-1"},
				},
			})
		case "/socket.io/":
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer ws.Close()

			n := connCount.Add(1)
			// Expect initial "40" from client
			_, msg, err := ws.ReadMessage()
			if err != nil || string(msg) != "40" {
				return
			}

			// Send Engine.IO handshake and Socket.IO namespace acceptance ("40")
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"abc","pingInterval":25000,"pingTimeout":20000}`))
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`40{"sid":"ns-abc"}`))

			// Send ping and expect pong "3"
			_ = ws.WriteMessage(websocket.TextMessage, []byte("2"))
			_, pong, err := ws.ReadMessage()
			if err == nil && string(pong) == "3" {
				gotPong.Store(true)
			}

			if n == 1 {
				// Short/malformed 42 frames should not panic
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`42[]`))
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`42["new-message"]`))
				// Chat read status changed (read: false ignored, read: true produces IsFromMe: true)
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`42["chat-read-status-changed",{"chatGuid":"iMessage;-;+15551234567","read":false}]`))
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`42["chat-read-status-changed",{"chatGuid":"iMessage;-;+15551234567","read":true}]`))
				// Message send error -> status channel
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`42["message-send-error",{"guid":"err-msg-1","error":22,"chats":[{"guid":"iMessage;-;+15551234567"}]}]`))
				// Updated message edit dedup: first edit goes to messageChan, second with same dateEdited + dateDelivered goes to messageStatusChan
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`42["updated-message",{"guid":"ed-1","text":"edited","dateCreated":1700000000000,"dateEdited":1700000005000,"chats":[{"guid":"iMessage;-;+15551234567"}]}]`))
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`42["updated-message",{"guid":"ed-1","text":"edited","dateCreated":1700000000000,"dateEdited":1700000005000,"dateDelivered":1700000006000,"chats":[{"guid":"iMessage;-;+15551234567"}]}]`))

				// Give client a moment to process, then force disconnect to test reconnect
				time.Sleep(50 * time.Millisecond)
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`41`))
				return
			}

			// Second connection stays open until closed by Stop()
			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, fb := newTestConnector(t, srv.URL)
	bb.reconnectBaseDelay = 20 * time.Millisecond
	bb.reconnectMaxDelay = 50 * time.Millisecond
	bb.contactRefreshInterval = time.Hour

	ready := make(chan struct{})
	if err := bb.Start(func() { close(ready) }); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer bb.Stop()

	<-ready

	// Verify read receipt from chat-read-status-changed
	select {
	case rr := <-bb.ReadReceiptChan():
		if !rr.IsFromMe || rr.ReadUpTo != "last-msg-1" || rr.ChatGUID != "iMessage;-;+15551234567" {
			t.Fatalf("unexpected read receipt: %+v", rr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for read receipt")
	}

	// Verify edit message received once
	select {
	case edited := <-bb.MessageChan():
		if edited.GUID != "ed-1" || !edited.IsEdited {
			t.Fatalf("unexpected edited message: %+v", edited)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for edited message")
	}

	// Verify send error + delivered status on ed-1
	gotFailed := false
	gotDelivered := false
	deadline := time.After(2 * time.Second)
	for !gotFailed || !gotDelivered {
		select {
		case st := <-bb.MessageStatusChan():
			if st.GUID == "err-msg-1" && st.Status == "failed" {
				gotFailed = true
			}
			if st.GUID == "ed-1" && st.Status == "delivered" {
				gotDelivered = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message statuses (gotFailed=%v, gotDelivered=%v)", gotFailed, gotDelivered)
		}
	}

	// Verify reconnect happened after "41"
	reconnectDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(reconnectDeadline) {
		if connCount.Load() >= 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if connCount.Load() < 2 {
		t.Fatalf("expected websocket to reconnect, connCount=%d", connCount.Load())
	}
	if !gotPong.Load() {
		t.Fatal("expected websocket to respond to ping with pong")
	}

	// Verify bridge status transitions were reported
	statuses := fb.getStatuses()
	if len(statuses) < 3 {
		t.Fatalf("expected CONNECTED -> TRANSIENT_DISCONNECT -> CONNECTED statuses, got %+v", statuses)
	}
}

func TestWebSocketNamespaceRejectionAndTimeout(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	// Case 1: Server rejects namespace with "44{...}" -> Start fails and never reports CONNECTED
	rejectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{Status: 200, Data: ServerInfo{PrivateAPI: true}})
		case "/api/v1/contact":
			_ = json.NewEncoder(w).Encode(ContactResponse{Status: 200, Data: []Contact{}})
		case "/socket.io/":
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer ws.Close()
			_, _, _ = ws.ReadMessage()
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"abc","pingInterval":25000,"pingTimeout":20000}`))
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`44{"message":"Invalid password"}`))
		}
	}))
	defer rejectSrv.Close()

	bbReject, fbReject := newTestConnector(t, rejectSrv.URL)
	bbReject.wsHandshakeTimeout = 500 * time.Millisecond
	if err := bbReject.Start(func() { t.Error("readyCallback should not be called when namespace is rejected") }); err == nil || !strings.Contains(err.Error(), "namespace rejected") {
		t.Fatalf("expected namespace rejected error, got %v", err)
	}
	for _, st := range fbReject.getStatuses() {
		if st.StateEvent == "CONNECTED" {
			t.Fatalf("should not have emitted CONNECTED on namespace rejection: %+v", fbReject.getStatuses())
		}
	}

	// Case 2: Server sends Engine.IO open & pings but never sends "40" -> times out and never reports CONNECTED
	silentSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{Status: 200, Data: ServerInfo{PrivateAPI: true}})
		case "/api/v1/contact":
			_ = json.NewEncoder(w).Encode(ContactResponse{Status: 200, Data: []Contact{}})
		case "/socket.io/":
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer ws.Close()
			_, _, _ = ws.ReadMessage()
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"abc","pingInterval":25000,"pingTimeout":20000}`))
			time.Sleep(400 * time.Millisecond)
		}
	}))
	defer silentSrv.Close()

	bbSilent, fbSilent := newTestConnector(t, silentSrv.URL)
	bbSilent.wsHandshakeTimeout = 100 * time.Millisecond
	if err := bbSilent.Start(func() { t.Error("readyCallback should not be called on handshake timeout") }); err == nil {
		t.Fatal("expected timeout error when server never sends namespace ack")
	}
	for _, st := range fbSilent.getStatuses() {
		if st.StateEvent == "CONNECTED" {
			t.Fatalf("should not have emitted CONNECTED on handshake timeout: %+v", fbSilent.getStatuses())
		}
	}
}

func TestReconnectPrivateAPIRaceAndTransientError(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var serverInfoCalls atomic.Int32
	var wsConns atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			call := serverInfoCalls.Add(1)
			if call == 2 {
				// Simulate transient server-info failure on first reconnect; Private API should stay true.
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("temporary failure"))
				return
			}
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{Status: 200, Data: ServerInfo{PrivateAPI: true}})
		case "/api/v1/contact":
			_ = json.NewEncoder(w).Encode(ContactResponse{Status: 200, Data: []Contact{}})
		case "/api/v1/message/text":
			_ = json.NewEncoder(w).Encode(SendTextResponse{
				Status: 200,
				Data:   Message{GUID: "m-1", DateCreated: 1700000000000},
			})
		case "/socket.io/":
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer ws.Close()
			_, _, _ = ws.ReadMessage()
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"abc","pingInterval":25000,"pingTimeout":20000}`))
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`40`))
			n := wsConns.Add(1)
			if n == 1 {
				// Trigger reconnect via "44" namespace error mid-session
				time.Sleep(20 * time.Millisecond)
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`44{"message":"session expired"}`))
				return
			}
			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					return
				}
			}
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	bb.reconnectBaseDelay = 15 * time.Millisecond
	bb.reconnectMaxDelay = 30 * time.Millisecond

	ready := make(chan struct{})
	if err := bb.Start(func() { close(ready) }); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer bb.Stop()
	<-ready

	// Concurrently read Capabilities and SendMessage while reconnect runs (verified under -race).
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			_ = bb.Capabilities()
			time.Sleep(2 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_, _ = bb.SendMessage("iMessage;-;+15551234567", "ping", "", 0, nil, nil)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wsConns.Load() >= 2 && serverInfoCalls.Load() >= 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	wg.Wait()

	if wsConns.Load() < 2 {
		t.Fatalf("expected reconnect after mid-session 44 error, got %d connections", wsConns.Load())
	}
	// Transient HTTP 500 on reconnect's server/info check must NOT clobber confirmed Private API = true.
	if !bb.Capabilities().SendTapbacks {
		t.Fatal("expected Private API capability to remain true after transient server/info error on reconnect")
	}
}

func TestDroppedEditNotSuppressedWhenBufferFull(t *testing.T) {
	bb, _ := newTestConnector(t, "http://localhost:1234")
	// Fill messageChan to capacity so the next enqueue drops.
	for i := 0; i < cap(bb.messageChan); i++ {
		bb.messageChan <- &imessage.Message{GUID: "filler"}
	}

	editPayload := json.RawMessage(`{"guid":"ed-drop","text":"updated text","dateCreated":1700000000000,"dateEdited":1700000005000,"chats":[{"guid":"iMessage;-;+15551234567"}]}`)

	// First attempt drops because messageChan is full; must NOT mark edit timestamp as processed.
	if err := bb.handleMessageUpdated(editPayload); err != nil {
		t.Fatalf("handleMessageUpdated failed: %v", err)
	}

	// Drain the buffer.
	for i := 0; i < cap(bb.messageChan); i++ {
		<-bb.messageChan
	}

	// Repeat of the exact same edit update must now succeed and enqueue.
	if err := bb.handleMessageUpdated(editPayload); err != nil {
		t.Fatalf("second handleMessageUpdated failed: %v", err)
	}
	select {
	case msg := <-bb.messageChan:
		if msg.GUID != "ed-drop" || msg.Text != "updated text" {
			t.Fatalf("unexpected enqueued edit: %+v", msg)
		}
	default:
		t.Fatal("expected repeated edit after buffer drain to be enqueued, but it was suppressed")
	}

	// A 3rd identical edit update after successful enqueue SHOULD now be deduplicated.
	if err := bb.handleMessageUpdated(editPayload); err != nil {
		t.Fatalf("third handleMessageUpdated failed: %v", err)
	}
	select {
	case msg := <-bb.messageChan:
		t.Fatalf("expected duplicate edit after enqueue to be suppressed, got %+v", msg)
	default:
	}
}

func TestEditMessageSuppressesOwnEcho(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/message/msg-edit-1/edit" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(EditMessageResponse{
			Status: 200,
			Data: Message{
				GUID:        "msg-edit-1",
				DateCreated: 1700000000000,
				DateEdited:  1700000010000,
				IsFromMe:    true,
				Text:        "fixed typo",
				Chats:       []Chat{{GUID: "iMessage;-;+15551234567"}},
			},
		})
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	bb.usingPrivateAPI.Store(true)

	if _, err := bb.EditMessage("iMessage;-;+15551234567", "msg-edit-1", "fixed typo", 0); err != nil {
		t.Fatalf("EditMessage failed: %v", err)
	}

	// BlueBubbles echoes back an updated-message event with the same dateEdited: must NOT emit to messageChan.
	sameEcho := json.RawMessage(`{"guid":"msg-edit-1","isFromMe":true,"text":"fixed typo","dateCreated":1700000000000,"dateEdited":1700000010000,"chats":[{"guid":"iMessage;-;+15551234567"}]}`)
	if err := bb.handleMessageUpdated(sameEcho); err != nil {
		t.Fatalf("handleMessageUpdated echo failed: %v", err)
	}
	select {
	case msg := <-bb.messageChan:
		t.Fatalf("expected own Matrix edit echo to be suppressed, got %+v", msg)
	default:
	}

	// A subsequent edit from another Apple device with a newer dateEdited SHOULD still be bridged.
	newerEdit := json.RawMessage(`{"guid":"msg-edit-1","isFromMe":true,"text":"edited again on phone","dateCreated":1700000000000,"dateEdited":1700000020000,"chats":[{"guid":"iMessage;-;+15551234567"}]}`)
	if err := bb.handleMessageUpdated(newerEdit); err != nil {
		t.Fatalf("handleMessageUpdated newer edit failed: %v", err)
	}
	select {
	case msg := <-bb.messageChan:
		if msg.Text != "edited again on phone" || !msg.IsEdited {
			t.Fatalf("unexpected newer edit message: %+v", msg)
		}
	default:
		t.Fatal("expected newer edit from another device to reach messageChan")
	}
}

func TestEditMessageSuppressesEchoWithoutDateEditedAndBeforeHTTPResponse(t *testing.T) {
	var bb *blueBubbles
	var triggerEarlyEcho atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/message/msg-early/edit":
			if triggerEarlyEcho.Load() {
				// Simulate WebSocket updated-message arriving BEFORE the HTTP response returns.
				earlyEcho := json.RawMessage(`{"guid":"msg-early","isFromMe":true,"text":"early edit","dateCreated":1700000000000,"dateEdited":1700000030000,"chats":[{"guid":"iMessage;-;+15551234567"}]}`)
				if err := bb.handleMessageUpdated(earlyEcho); err != nil {
					t.Errorf("early handleMessageUpdated failed: %v", err)
				}
			}
			_ = json.NewEncoder(w).Encode(EditMessageResponse{
				Status: 200,
				Data: Message{
					GUID:        "msg-early",
					DateCreated: 1700000000000,
					DateEdited:  1700000030000,
					IsFromMe:    true,
					Text:        "early edit",
					Chats:       []Chat{{GUID: "iMessage;-;+15551234567"}},
				},
			})
		case "/api/v1/message/msg-no-ts/edit":
			// Simulate older server omitting dateEdited in HTTP response.
			_ = json.NewEncoder(w).Encode(EditMessageResponse{
				Status: 200,
				Data: Message{
					GUID:        "msg-no-ts",
					DateCreated: 1700000000000,
					DateEdited:  0,
					IsFromMe:    true,
					Text:        "old text",
					Chats:       []Chat{{GUID: "iMessage;-;+15551234567"}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, _ = newTestConnector(t, srv.URL)
	bb.usingPrivateAPI.Store(true)
	triggerEarlyEcho.Store(true)

	// Case 1: Echo arrives before HTTP response completes.
	if _, err := bb.EditMessage("iMessage;-;+15551234567", "msg-early", "early edit", 0); err != nil {
		t.Fatalf("EditMessage early failed: %v", err)
	}
	select {
	case msg := <-bb.messageChan:
		t.Fatalf("expected early websocket echo before HTTP response to be suppressed, got %+v", msg)
	default:
	}

	// Case 2: HTTP response omits dateEdited, echo arrives after with non-zero dateEdited.
	if _, err := bb.EditMessage("iMessage;-;+15551234567", "msg-no-ts", "no ts edit", 0); err != nil {
		t.Fatalf("EditMessage no-ts failed: %v", err)
	}
	noTsEcho := json.RawMessage(`{"guid":"msg-no-ts","isFromMe":true,"text":"no ts edit","dateCreated":1700000000000,"dateEdited":1700000040000,"chats":[{"guid":"iMessage;-;+15551234567"}]}`)
	if err := bb.handleMessageUpdated(noTsEcho); err != nil {
		t.Fatalf("handleMessageUpdated no-ts echo failed: %v", err)
	}
	select {
	case msg := <-bb.messageChan:
		t.Fatalf("expected echo after response omitting dateEdited to be suppressed, got %+v", msg)
	default:
	}
}

func TestCredentialSanitizationInErrors(t *testing.T) {
	// Start and immediately close a local server to guarantee connection-refused *url.Error.
	closedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closedSrv.URL
	closedSrv.Close()

	secret := "s3cr3t+pw&123"
	escapedSecret := url.QueryEscape(secret)
	fb := newFakeBridge(closedURL, secret)
	api, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatalf("NewBlueBubblesConnector failed: %v", err)
	}
	bb := api.(*blueBubbles)
	bb.usingPrivateAPI.Store(true)

	tmpFile := filepath.Join(t.TempDir(), "test.txt")
	if err := os.WriteFile(tmpFile, []byte("hello"), 0600); err != nil {
		t.Fatal(err)
	}

	assertNoSecret := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected error against closed server, got nil", name)
		}
		msg := err.Error()
		if strings.Contains(msg, secret) || strings.Contains(msg, escapedSecret) {
			t.Fatalf("%s: error leaked BlueBubbles password: %s", name, msg)
		}
	}

	assertNoSecret("RefreshContactList", bb.RefreshContactList())
	_, err = bb.SendMessage("iMessage;-;+15551234567", "hi", "", 0, nil, nil)
	assertNoSecret("SendMessage", err)
	_, err = bb.SendFile("iMessage;-;+15551234567", "", "test.txt", tmpFile, "", 0, "text/plain", false, nil)
	assertNoSecret("SendFile", err)
	_, err = bb.downloadAttachment("att-1")
	assertNoSecret("downloadAttachment", err)
	_, err = bb.connectToWebSocket()
	assertNoSecret("connectToWebSocket", err)
}

func TestResolveIdentifierFirstContact404AndErrorPropagation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/handle/+15551112222":
			_ = json.NewEncoder(w).Encode(ResolveIdentifierResponse{
				Status: 200,
				Data:   Handle{Address: "+15551112222", Service: "SMS"},
			})
		case "/api/v1/handle/+15559998888":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"status":404,"message":"Handle does not exist!"}`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"status":500,"message":"db locked"}`))
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)

	// Known handle -> returns server-reported service
	got, err := bb.ResolveIdentifier("+1 (555) 111-2222")
	if err != nil || got != "SMS;-;+15551112222" {
		t.Fatalf("expected SMS;-;+15551112222, got %q (err=%v)", got, err)
	}

	// Unknown first-contact handle (HTTP 404) -> falls back to iMessage
	gotNew, err := bb.ResolveIdentifier("+15559998888")
	if err != nil || gotNew != "iMessage;-;+15559998888" {
		t.Fatalf("expected 404 fallback iMessage;-;+15559998888, got %q (err=%v)", gotNew, err)
	}

	// Non-404 error (HTTP 500) -> propagated
	if _, err := bb.ResolveIdentifier("+15550000000"); err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("expected HTTP 500 error to propagate, got %v", err)
	}
}

func TestSendTimeoutUsesSendClientAndPreservesUncertainty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/message/text" {
			time.Sleep(80 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(SendTextResponse{
				Status: 200,
				Data:   Message{GUID: "slow-msg-1", DateCreated: 1700000000000},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	secret := "timeout-secret-pw"
	fb := newFakeBridge(srv.URL, secret)
	api, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatalf("NewBlueBubblesConnector failed: %v", err)
	}
	bb := api.(*blueBubbles)

	// SendMessage uses sendHTTPClient (300ms), NOT httpClient (30ms), so 80ms send succeeds.
	bb.httpClient = &http.Client{Timeout: 30 * time.Millisecond}
	bb.sendHTTPClient = &http.Client{Timeout: 300 * time.Millisecond}
	if resp, err := bb.SendMessage("iMessage;-;+15551234567", "slow send", "", 0, nil, nil); err != nil || resp.GUID != "slow-msg-1" {
		t.Fatalf("expected SendMessage to use sendHTTPClient timeout and succeed, got resp=%+v err=%v", resp, err)
	}

	// When sendHTTPClient itself times out, error wraps imessage.ErrSendTimeout and redacts password.
	bb.sendHTTPClient = &http.Client{Timeout: 30 * time.Millisecond}
	_, err = bb.SendMessage("iMessage;-;+15551234567", "too slow", "", 0, nil, nil)
	if !errors.Is(err, imessage.ErrSendTimeout) {
		t.Fatalf("expected ErrSendTimeout, got %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("timeout error leaked password: %s", err.Error())
	}
}

func TestUpdatedMessageStatusDoesNotDownloadAttachments(t *testing.T) {
	var attachmentRequests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/attachment/") {
			attachmentRequests.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	deliveredWithAttachment := json.RawMessage(`{
		"guid": "msg-att-delivered",
		"isFromMe": true,
		"dateCreated": 1700000000000,
		"dateDelivered": 1700000005000,
		"chats": [{"guid": "iMessage;-;+15551234567"}],
		"attachments": [{"guid": "att-1", "transferName": "pic.jpg", "mimeType": "image/jpeg"}]
	}`)
	if err := bb.handleMessageUpdated(deliveredWithAttachment); err != nil {
		t.Fatalf("handleMessageUpdated failed: %v", err)
	}
	if attachmentRequests.Load() != 0 {
		t.Fatalf("expected 0 attachment downloads for delivery status update, got %d", attachmentRequests.Load())
	}
	select {
	case st := <-bb.MessageStatusChan():
		if st.GUID != "msg-att-delivered" || st.Status != "delivered" {
			t.Fatalf("unexpected delivery status: %+v", st)
		}
	default:
		t.Fatal("expected delivered status event on MessageStatusChan")
	}
}

func TestHealthReportingAndHelperDisconnected(t *testing.T) {
	var mode atomic.Int32 // 0 = healthy, 1 = helper disconnected, 2 = server 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/server/info" {
			http.NotFound(w, r)
			return
		}
		switch mode.Load() {
		case 0:
			helperTrue := true
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{
				Status: 200,
				Data: ServerInfo{
					ServerVersion:   "1.9.7",
					OSVersion:       "15.3.1",
					ProxyService:    "Cloudflare",
					PrivateAPI:      true,
					HelperConnected: &helperTrue,
				},
			})
		case 1:
			helperFalse := false
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{
				Status: 200,
				Data: ServerInfo{
					ServerVersion:   "1.9.7",
					OSVersion:       "15.3.1",
					PrivateAPI:      true,
					HelperConnected: &helperFalse,
				},
			})
		default:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("server unreachable"))
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	bb.wsConnected.Store(true)
	bb.connectedSince.Store(time.Now().Add(-5 * time.Minute).UnixNano())
	bb.lastEventAt.Store(time.Now().Add(-10 * time.Second).UnixNano())
	bb.reconnectCount.Store(2)

	h := bb.GetHealth()
	if h.ServerVersion != "1.9.7" || h.OSVersion != "15.3.1" || h.ProxyService != "Cloudflare" {
		t.Fatalf("unexpected server metadata: %+v", h)
	}
	if !h.PrivateAPI || h.HelperConnected == nil || !*h.HelperConnected {
		t.Fatalf("expected PrivateAPI=true and HelperConnected=true, got %+v", h)
	}
	if !h.WSConnected || h.ReconnectCount != 2 || h.LastEventAt.IsZero() || h.ServerInfoErr != "" {
		t.Fatalf("unexpected websocket/health state: %+v", h)
	}

	// Simulate Private API helper detached from Messages.app
	mode.Store(1)
	h2 := bb.GetHealth()
	if !h2.PrivateAPI || h2.HelperConnected == nil || *h2.HelperConnected {
		t.Fatalf("expected PrivateAPI=true and HelperConnected=false, got %+v", h2)
	}

	// Simulate transient server/info outage: ServerInfoErr populated, cached info preserved
	mode.Store(2)
	h3 := bb.GetHealth()
	if h3.ServerInfoErr == "" || h3.ServerVersion != "1.9.7" {
		t.Fatalf("expected ServerInfoErr with cached version preserved, got %+v", h3)
	}
}

func TestReconnectCatchUpSync(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	var connCount atomic.Int32
	var seenAttachmentDownloads atomic.Int32
	var missedAttachmentDownloads atomic.Int32

	nowMs := time.Now().UnixMilli()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			helperTrue := true
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{
				Status: 200,
				Data:   ServerInfo{PrivateAPI: true, HelperConnected: &helperTrue, ServerVersion: "1.9.7"},
			})
		case "/api/v1/contact":
			_ = json.NewEncoder(w).Encode(ContactResponse{Status: 200, Data: []Contact{}})
		case "/api/v1/chat/query":
			_ = json.NewEncoder(w).Encode(ChatQueryResponse{
				Status: 200,
				Data: []Chat{
					{GUID: "iMessage;-;+15551111111", LastMessage: &Message{GUID: "msg-missed-2", DateCreated: nowMs}},
					{GUID: "any;-;+15552222222", LastMessage: &Message{GUID: "msg-newchat-1", DateCreated: nowMs}},
				},
				Metadata: PageMetadata{Total: 2, Limit: 1000, Offset: 0, Count: 2},
			})
		case "/api/v1/chat/iMessage;-;+15551111111/message":
			// Returned in DESC order (newest first), as BlueBubbles does.
			_ = json.NewEncoder(w).Encode(MessageQueryResponse{
				Status: 200,
				Data: []Message{
					{
						GUID:        "msg-missed-2",
						DateCreated: nowMs - 1000,
						Text:        "missed while offline",
						Handle:      Handle{Address: "+15551111111", Service: "iMessage"},
						Attachments: []Attachment{{GUID: "att-missed", TransferName: "doc.txt", MimeType: "text/plain"}},
					},
					{
						GUID:        "msg-seen-edited",
						DateCreated: nowMs - 2000,
						DateEdited:  nowMs - 500,
						Text:        "edited while offline",
						Handle:      Handle{Address: "+15551111111", Service: "iMessage"},
					},
					{
						GUID:        "msg-already-seen",
						DateCreated: nowMs - 3000,
						Text:        "already bridged",
						Handle:      Handle{Address: "+15551111111", Service: "iMessage"},
						Attachments: []Attachment{{GUID: "att-seen", TransferName: "old.jpg", MimeType: "image/jpeg"}},
					},
				},
				Metadata: PageMetadata{Total: 3, Limit: 100, Offset: 0, Count: 3},
			})
		case "/api/v1/chat/any;-;+15552222222/message":
			_ = json.NewEncoder(w).Encode(MessageQueryResponse{
				Status: 200,
				Data: []Message{
					{
						GUID:        "msg-newchat-1",
						DateCreated: nowMs - 500,
						Text:        "hello from rcs/any chat",
						Handle:      Handle{Address: "+15552222222", Service: "RCS"},
					},
				},
				Metadata: PageMetadata{Total: 1, Limit: 100, Offset: 0, Count: 1},
			})
		case "/api/v1/attachment/att-seen":
			seenAttachmentDownloads.Add(1)
			http.NotFound(w, r)
		case "/api/v1/attachment/att-missed":
			missedAttachmentDownloads.Add(1)
			_ = json.NewEncoder(w).Encode(AttachmentResponse{
				Status: 200,
				Data:   Attachment{GUID: "att-missed", TransferName: "doc.txt", MimeType: "text/plain"},
			})
		case "/api/v1/attachment/att-missed/download":
			_, _ = w.Write([]byte("missed-bytes"))
		case "/socket.io/":
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer ws.Close()
			_, _, _ = ws.ReadMessage()
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"abc","pingInterval":25000,"pingTimeout":20000}`))
			_ = ws.WriteMessage(websocket.TextMessage, []byte(`40`))
			if connCount.Add(1) == 1 {
				time.Sleep(20 * time.Millisecond)
				_ = ws.WriteMessage(websocket.TextMessage, []byte(`41`))
				return
			}
			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, fb := newTestConnector(t, srv.URL)
	fb.messagesSince = map[string][]string{
		"iMessage;-;+15551111111": {"msg-already-seen", "msg-seen-edited"},
	}
	bb.reconnectBaseDelay = 15 * time.Millisecond
	bb.reconnectMaxDelay = 30 * time.Millisecond
	bb.catchUpOverlap = 5 * time.Minute

	ready := make(chan struct{})
	if err := bb.Start(func() { close(ready) }); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer bb.Stop()
	<-ready

	got := make(map[string]*imessage.Message)
	var order []string
	deadline := time.After(3 * time.Second)
	for len(got) < 3 {
		select {
		case msg := <-bb.MessageChan():
			got[msg.GUID] = msg
			order = append(order, msg.GUID)
			for _, att := range msg.Attachments {
				if att != nil && att.PathOnDisk != "" {
					defer os.Remove(att.PathOnDisk)
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for catch-up messages, got=%v", order)
		}
	}

	if _, sawDup := got["msg-already-seen"]; sawDup {
		t.Fatal("already-seen message should have been skipped during catch-up")
	}
	if seenAttachmentDownloads.Load() != 0 {
		t.Fatalf("expected 0 attachment downloads for already-seen message, got %d", seenAttachmentDownloads.Load())
	}
	if missedAttachmentDownloads.Load() != 1 {
		t.Fatalf("expected 1 attachment download for missed message, got %d", missedAttachmentDownloads.Load())
	}

	missed := got["msg-missed-2"]
	if missed == nil || missed.ChatGUID != "iMessage;-;+15551111111" || len(missed.Attachments) != 1 || !missed.Attachments[0].IsTemp {
		t.Fatalf("unexpected missed message: %+v", missed)
	}
	edited := got["msg-seen-edited"]
	if edited == nil || !edited.IsEdited || edited.Text != "edited while offline" {
		t.Fatalf("expected missed edit to be replayed, got %+v", edited)
	}
	newChatMsg := got["msg-newchat-1"]
	if newChatMsg == nil || newChatMsg.ChatGUID != "any;-;+15552222222" || newChatMsg.Service != "RCS" {
		t.Fatalf("unexpected new-chat catch-up message: %+v", newChatMsg)
	}

	// Verify chronological order within the first chat (msg-seen-edited before msg-missed-2)
	if order[0] != "msg-seen-edited" || order[1] != "msg-missed-2" {
		t.Fatalf("expected chronological order [msg-seen-edited, msg-missed-2, ...], got %v", order)
	}

	// Wait briefly for health stats update
	waitDeadline := time.Now().Add(time.Second)
	for time.Now().Before(waitDeadline) {
		h := bb.GetHealth()
		if !h.LastCatchUpAt.IsZero() && h.LastCatchUpChats == 2 && h.LastCatchUpMsgs == 3 && h.LastCatchUpErr == "" {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("expected catch-up health telemetry to record 2 chats / 3 msgs, got %+v", bb.GetHealth())
}

func TestBackfillQueriesIncludeChatAndFallback(t *testing.T) {
	var withParam string
	var pageCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/chat/iMessage;-;+15551234567/message" {
			withParam = r.URL.Query().Get("with")
			call := pageCalls.Add(1)
			if call == 1 {
				// Intentionally omit `chats` on returned messages to verify chatID fallback,
				// and return Total > Count with Limit=0 to verify pagination loop guard.
				_ = json.NewEncoder(w).Encode(MessageQueryResponse{
					Status: 200,
					Data: []Message{
						{GUID: "m-2", DateCreated: 1700000002000, Text: "second", Handle: Handle{Address: "+15551234567", Service: "iMessage"}},
						{GUID: "m-1", DateCreated: 1700000001000, Text: "first", Handle: Handle{Address: "+15551234567", Service: "iMessage"}},
					},
					Metadata: PageMetadata{Total: 10, Limit: 0, Offset: 0, Count: 2},
				})
				return
			}
			t.Errorf("unexpected extra pagination call #%d when Limit=0", call)
			http.Error(w, "infinite pagination", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	msgs, err := bb.GetMessagesWithLimit("iMessage;-;+15551234567", 50, "bf-1")
	if err != nil {
		t.Fatalf("GetMessagesWithLimit failed: %v", err)
	}
	if !strings.Contains(","+withParam+",", ",chat,") {
		t.Fatalf("expected 'chat' in with query param, got %q", withParam)
	}
	if len(msgs) != 2 || msgs[0].GUID != "m-1" || msgs[1].GUID != "m-2" || msgs[0].ChatGUID != "iMessage;-;+15551234567" {
		t.Fatalf("unexpected backfilled messages: %+v", msgs)
	}

	// Verify pagination guard on GetMessagesSinceDate when server returns Limit=0
	pageCalls.Store(0)
	msgs2, err := bb.GetMessagesSinceDate("iMessage;-;+15551234567", time.UnixMilli(1700000000000), "bf-2")
	if err != nil || len(msgs2) != 2 {
		t.Fatalf("GetMessagesSinceDate failed: msgs=%d err=%v", len(msgs2), err)
	}
}

func TestSequoiaAnyChatResolution(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/handle/+15551112222":
			_ = json.NewEncoder(w).Encode(ResolveIdentifierResponse{
				Status: 200,
				Data:   Handle{Address: "+15551112222", Service: "iMessage"},
			})
		case "/api/v1/chat/any;-;+15551112222":
			_ = json.NewEncoder(w).Encode(ChatResponse{
				Status: 200,
				Data:   &Chat{GUID: "any;-;+15551112222"},
			})
		case "/api/v1/handle/+15553334444":
			_ = json.NewEncoder(w).Encode(ResolveIdentifierResponse{
				Status: 200,
				Data:   Handle{Address: "+15553334444", Service: "RCS"},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"status":404,"message":"not found"}`))
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)

	// Existing Sequoia any;-; chat is reused
	gotAny, err := bb.ResolveIdentifier("+15551112222")
	if err != nil || gotAny != "any;-;+15551112222" {
		t.Fatalf("expected any;-;+15551112222, got %q (err=%v)", gotAny, err)
	}

	// No any;-; chat -> falls back to handle's reported RCS service
	gotRCS, err := bb.ResolveIdentifier("+15553334444")
	if err != nil || gotRCS != "RCS;-;+15553334444" {
		t.Fatalf("expected RCS;-;+15553334444, got %q (err=%v)", gotRCS, err)
	}
}

func TestGroupAdminAPIAndRenameEchoSuppression(t *testing.T) {
	var (
		mu             sync.Mutex
		gotRename      UpdateChatRequest
		gotAdded       ChatParticipantRequest
		gotRemoved     ChatParticipantRequest
		gotIconName    string
		gotIconDeleted bool
		gotLeft        bool
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/chat/iMessage;+;chat123":
			mu.Lock()
			_ = json.NewDecoder(r.Body).Decode(&gotRename)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(GenericResponse{Status: 200, Message: "ok"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/chat/iMessage;+;chat123/participant/add":
			mu.Lock()
			_ = json.NewDecoder(r.Body).Decode(&gotAdded)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(GenericResponse{Status: 200, Message: "ok"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/chat/iMessage;+;chat123/participant/remove":
			mu.Lock()
			_ = json.NewDecoder(r.Body).Decode(&gotRemoved)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(GenericResponse{Status: 200, Message: "ok"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/chat/iMessage;+;chat123/icon":
			_ = r.ParseMultipartForm(10 << 20)
			_, fh, err := r.FormFile("icon")
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			gotIconName = fh.Filename
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(GenericResponse{Status: 200, Message: "ok"})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/chat/iMessage;+;chat123/icon":
			mu.Lock()
			gotIconDeleted = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(GenericResponse{Status: 200, Message: "ok"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/chat/iMessage;+;chat123/leave":
			mu.Lock()
			gotLeft = true
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(GenericResponse{Status: 200, Message: "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)

	// Without Private API -> ErrUnsupported
	bb.usingPrivateAPI.Store(false)
	if bb.Capabilities().GroupAdmin {
		t.Fatal("expected GroupAdmin capability to be false without Private API")
	}
	if err := bb.SetGroupName("iMessage;+;chat123", "New Name"); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported without Private API, got %v", err)
	}

	bb.usingPrivateAPI.Store(true)
	if !bb.Capabilities().GroupAdmin {
		t.Fatal("expected GroupAdmin capability to be true with Private API")
	}

	// Rejects DMs and SMS groups
	if err := bb.SetGroupName("iMessage;-;+15551234567", "DM"); err == nil {
		t.Fatal("expected error renaming a DM")
	}
	if err := bb.SetGroupName("SMS;+;chat123", "SMS Group"); err == nil {
		t.Fatal("expected error renaming an SMS group")
	}

	// Rename + echo suppression
	if err := bb.SetGroupName("iMessage;+;chat123", "Weekend Trip"); err != nil {
		t.Fatalf("SetGroupName failed: %v", err)
	}
	if gotRename.DisplayName != "Weekend Trip" {
		t.Fatalf("unexpected rename payload: %+v", gotRename)
	}
	renameEcho := json.RawMessage(`{"guid":"rn-1","itemType":2,"groupTitle":"Weekend Trip","dateCreated":1700000000000,"chats":[{"guid":"iMessage;+;chat123"}]}`)
	if err := bb.handleNewMessage(renameEcho); err != nil {
		t.Fatalf("handleNewMessage rename echo failed: %v", err)
	}
	select {
	case msg := <-bb.MessageChan():
		t.Fatalf("expected Matrix-initiated group rename echo to be suppressed, got %+v", msg)
	default:
	}

	// Another rename from iMessage should still come through
	remoteRename := json.RawMessage(`{"guid":"rn-2","itemType":2,"groupTitle":"Beach Trip","dateCreated":1700000001000,"chats":[{"guid":"iMessage;+;chat123"}]}`)
	if err := bb.handleNewMessage(remoteRename); err != nil {
		t.Fatalf("handleNewMessage remote rename failed: %v", err)
	}
	select {
	case msg := <-bb.MessageChan():
		if msg.ItemType != imessage.ItemTypeName || msg.NewGroupName != "Beach Trip" {
			t.Fatalf("unexpected remote rename message: %+v", msg)
		}
	default:
		t.Fatal("expected remote group rename to reach MessageChan")
	}

	// Add / Remove participant (including address normalization)
	if err := bb.AddGroupParticipant("iMessage;+;chat123", "(555) 222-3333"); err != nil {
		t.Fatalf("AddGroupParticipant failed: %v", err)
	}
	if gotAdded.Address != "+5552223333" {
		t.Fatalf("unexpected added address: %+v", gotAdded)
	}
	if err := bb.RemoveGroupParticipant("iMessage;+;chat123", "iMessage;-;alice@example.com"); err != nil {
		t.Fatalf("RemoveGroupParticipant failed: %v", err)
	}
	if gotRemoved.Address != "alice@example.com" {
		t.Fatalf("unexpected removed address: %+v", gotRemoved)
	}

	// Set / Remove group avatar and LeaveGroup
	if err := bb.SetGroupAvatar("iMessage;+;chat123", "group.png", []byte("png-data")); err != nil {
		t.Fatalf("SetGroupAvatar failed: %v", err)
	}
	if gotIconName != "group.png" {
		t.Fatalf("expected group.png icon upload, got %q", gotIconName)
	}
	if err := bb.RemoveGroupAvatar("iMessage;+;chat123"); err != nil {
		t.Fatalf("RemoveGroupAvatar failed: %v", err)
	}
	if !gotIconDeleted {
		t.Fatal("expected group icon DELETE call")
	}
	if err := bb.LeaveGroup("iMessage;+;chat123"); err != nil {
		t.Fatalf("LeaveGroup failed: %v", err)
	}
	if !gotLeft {
		t.Fatal("expected group leave POST call")
	}
}

func TestReconnectCatchUpRecoversPreWindowRetraction(t *testing.T) {
	now := time.Now()
	since := now.Add(-1 * time.Minute)
	twoHoursAgoMs := now.Add(-2 * time.Hour).UnixMilli()
	tenMinsAgoMs := now.Add(-10 * time.Minute).UnixMilli()
	retractedDuringDisconnectMs := now.Add(-15 * time.Second).UnixMilli()

	var attachmentCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/chat/query":
			_ = json.NewEncoder(w).Encode(ChatQueryResponse{
				Status: 200,
				Data: []Chat{
					{
						// Quiet chat whose last message was created 2 hours ago, then retracted during disconnect.
						GUID: "iMessage;-;+15551000001",
						LastMessage: &Message{
							GUID:          "old-retracted-last",
							DateCreated:   twoHoursAgoMs,
							DateRetracted: retractedDuringDisconnectMs,
							Handle:        Handle{Address: "+15551000001", Service: "iMessage"},
							Attachments:   []Attachment{{GUID: "att-old", TransferName: "pic.jpg", MimeType: "image/jpeg"}},
						},
					},
					{
						// Chat with a message created 10m ago (pre-window, within 20m lookback) retracted during disconnect.
						GUID: "iMessage;-;+15551000002",
						LastMessage: &Message{
							GUID:        "recent-normal",
							DateCreated: tenMinsAgoMs + 5000,
							Text:        "still here",
							Handle:      Handle{Address: "+15551000002", Service: "iMessage"},
						},
					},
				},
				Metadata: PageMetadata{Total: 2, Limit: 1000, Offset: 0, Count: 2},
			})
		case "/api/v1/chat/iMessage;-;+15551000001/message":
			// Query uses 20m lookback, so the 2-hour-old message is not returned here;
			// catch-up must still discover it via chat.LastMessage.
			_ = json.NewEncoder(w).Encode(MessageQueryResponse{
				Status:   200,
				Data:     []Message{},
				Metadata: PageMetadata{Total: 0, Limit: 100, Offset: 0, Count: 0},
			})
		case "/api/v1/chat/iMessage;-;+15551000002/message":
			_ = json.NewEncoder(w).Encode(MessageQueryResponse{
				Status: 200,
				Data: []Message{
					{
						GUID:        "recent-normal",
						DateCreated: tenMinsAgoMs + 5000,
						Text:        "still here",
						Handle:      Handle{Address: "+15551000002", Service: "iMessage"},
					},
					{
						GUID:          "prewindow-retracted-earlier",
						DateCreated:   tenMinsAgoMs,
						DateRetracted: retractedDuringDisconnectMs,
						Handle:        Handle{Address: "+15551000002", Service: "iMessage"},
					},
				},
				Metadata: PageMetadata{Total: 2, Limit: 100, Offset: 0, Count: 2},
			})
		default:
			if strings.HasPrefix(r.URL.Path, "/api/v1/attachment/") {
				attachmentCalls.Add(1)
			}
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	bb.runReconnectCatchUp(since)

	got := make(map[string]*imessage.Message)
	for {
		select {
		case msg := <-bb.MessageChan():
			got[msg.GUID] = msg
		default:
			goto done
		}
	}
done:
	if len(got) != 2 {
		t.Fatalf("expected exactly 2 pre-window retractions (and no untouched old messages), got %d: %+v", len(got), got)
	}
	if m1 := got["old-retracted-last"]; m1 == nil || !m1.IsRetracted || m1.ChatGUID != "iMessage;-;+15551000001" {
		t.Fatalf("expected 2h-old quiet-chat retraction to be recovered, got %+v", m1)
	}
	if m2 := got["prewindow-retracted-earlier"]; m2 == nil || !m2.IsRetracted || m2.ChatGUID != "iMessage;-;+15551000002" {
		t.Fatalf("expected 10m-old pre-window retraction to be recovered, got %+v", m2)
	}
	if attachmentCalls.Load() != 0 {
		t.Fatalf("expected 0 attachment downloads for retracted messages, got %d", attachmentCalls.Load())
	}
}

func TestCreateGroupPreservesSMSServiceThroughAnyChat(t *testing.T) {
	var (
		mu        sync.Mutex
		gotCreate []CreateChatRequest
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/handle/+15551111111":
			_ = json.NewEncoder(w).Encode(ResolveIdentifierResponse{
				Status: 200,
				Data:   Handle{Address: "+15551111111", Service: "SMS"},
			})
		case "/api/v1/chat/any;-;+15551111111":
			_ = json.NewEncoder(w).Encode(ChatResponse{
				Status: 200,
				Data:   &Chat{GUID: "any;-;+15551111111"},
			})
		case "/api/v1/handle/+15552222222":
			_ = json.NewEncoder(w).Encode(ResolveIdentifierResponse{
				Status: 200,
				Data:   Handle{Address: "+15552222222", Service: "iMessage"},
			})
		case "/api/v1/handle/+15553333333":
			_ = json.NewEncoder(w).Encode(ResolveIdentifierResponse{
				Status: 200,
				Data:   Handle{Address: "+15553333333", Service: "SMS"},
			})
		case "/api/v1/chat/new":
			var req CreateChatRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			gotCreate = append(gotCreate, req)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(CreateChatResponse{
				Status: 200,
				Data:   Chat{GUID: "SMS;+;chat777", GroupID: "grp-777"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	bb.usingPrivateAPI.Store(true)

	// ResolveIdentifier returns any;-;+15551111111 for the SMS participant because the any chat exists.
	g1, err := bb.ResolveIdentifier("+15551111111")
	if err != nil || g1 != "any;-;+15551111111" {
		t.Fatalf("expected any;-;+15551111111, got %q (err=%v)", g1, err)
	}
	g2, err := bb.ResolveIdentifier("+15552222222")
	if err != nil || g2 != "iMessage;-;+15552222222" {
		t.Fatalf("expected iMessage;-;+15552222222, got %q (err=%v)", g2, err)
	}

	// CreateGroupWithMessage must still submit service="SMS" using the preserved resolved service.
	if _, err := bb.CreateGroupWithMessage([]string{g1, g2}, "Hello mixed group"); err != nil {
		t.Fatalf("CreateGroupWithMessage failed: %v", err)
	}
	if len(gotCreate) != 1 || gotCreate[0].Service != "SMS" {
		t.Fatalf("expected outbound CreateChatRequest.Service='SMS', got %+v", gotCreate)
	}

	// Also verify direct any;-; participant (without prior ResolveIdentifier) probes handle service.
	if _, err := bb.CreateGroupWithMessage([]string{"any;-;+15553333333", g2}, "Hello direct any"); err != nil {
		t.Fatalf("CreateGroupWithMessage direct any failed: %v", err)
	}
	if len(gotCreate) != 2 || gotCreate[1].Service != "SMS" {
		t.Fatalf("expected direct any;-; SMS participant to yield service='SMS', got %+v", gotCreate)
	}
}

func TestEditedMessagePersistsAcrossRestartAndPreservesReceipts(t *testing.T) {
	var attachmentCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/attachment/") {
			attachmentCalls.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	// Shared bridge with persistent KV store (caption_in_message=false -> caption-only edits skip media download)
	fb := newFakeBridge(srv.URL, "secret-pw")
	api1, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatal(err)
	}
	bb1 := api1.(*blueBubbles)

	initialEdit := json.RawMessage(`{
		"guid": "msg-persist-1",
		"isFromMe": true,
		"text": "edited once",
		"dateCreated": 1700000000000,
		"dateEdited": 1700000005000,
		"chats": [{"guid": "iMessage;-;+15551234567"}],
		"attachments": [{"guid": "att-1", "transferName": "pic.jpg", "mimeType": "image/jpeg"}]
	}`)
	if err := bb1.handleMessageUpdated(initialEdit); err != nil {
		t.Fatalf("bb1.handleMessageUpdated failed: %v", err)
	}
	select {
	case msg := <-bb1.MessageChan():
		if msg.GUID != "msg-persist-1" || !msg.IsEdited {
			t.Fatalf("unexpected initial edit: %+v", msg)
		}
	default:
		t.Fatal("expected initial edit on bb1.MessageChan")
	}
	// Caption-only edit (caption_in_message=false) should not download attachments
	if attachmentCalls.Load() != 0 {
		t.Fatalf("expected 0 attachment downloads on caption-only edit, got %d", attachmentCalls.Load())
	}

	// Simulate crash/restart BETWEEN enqueue and Matrix delivery (SendMessageBridgeResult never called on bb1):
	// edit MUST NOT be persisted yet and MUST remain retryable after restart.
	if gotKV := fb.GetKV(kvEditedPrefix + "msg-persist-1"); gotKV != "" {
		t.Fatalf("expected edit timestamp not to be persisted before Matrix confirms delivery, got %q", gotKV)
	}
	api2, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatal(err)
	}
	bb2 := api2.(*blueBubbles)
	if err := bb2.handleMessageUpdated(initialEdit); err != nil {
		t.Fatalf("bb2.handleMessageUpdated retry after crash failed: %v", err)
	}
	select {
	case retry := <-bb2.MessageChan():
		if retry.GUID != "msg-persist-1" || retry.Text != "edited once" {
			t.Fatalf("unexpected retried edit after crash: %+v", retry)
		}
	default:
		t.Fatal("expected unconfirmed edit to be retried after restart")
	}

	// Failed Matrix send must clear inflight state so a subsequent retry still works even without restart.
	bb2.SendMessageBridgeResult("iMessage;-;+15551234567", "msg-persist-1", "", false)
	if err := bb2.handleMessageUpdated(initialEdit); err != nil {
		t.Fatalf("bb2.handleMessageUpdated retry after failure failed: %v", err)
	}
	select {
	case <-bb2.MessageChan():
	default:
		t.Fatal("expected failed edit to remain retryable")
	}

	// Once Matrix confirms delivery, the edit timestamp is persisted.
	bb2.SendMessageBridgeResult("iMessage;-;+15551234567", "msg-persist-1", "$evt1", true)
	if gotKV := fb.GetKV(kvEditedPrefix + "msg-persist-1"); gotKV != "1700000005000" {
		t.Fatalf("expected persisted edit timestamp 1700000005000, got %q", gotKV)
	}

	// Simulate bridge restart AFTER Matrix delivery: brand new blueBubbles instance with empty in-memory map.
	api3, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatal(err)
	}
	bb3 := api3.(*blueBubbles)

	// Same dateEdited arrives after restart with dateRead and an attachment:
	// must NOT re-emit duplicate edit, must NOT download attachments, and MUST emit read receipt.
	postRestartRead := json.RawMessage(`{
		"guid": "msg-persist-1",
		"isFromMe": true,
		"text": "edited once",
		"dateCreated": 1700000000000,
		"dateEdited": 1700000005000,
		"dateRead": 1700000010000,
		"chats": [{"guid": "iMessage;-;+15551234567"}],
		"attachments": [{"guid": "att-1", "transferName": "pic.jpg", "mimeType": "image/jpeg"}]
	}`)
	if err := bb3.handleMessageUpdated(postRestartRead); err != nil {
		t.Fatalf("bb3.handleMessageUpdated failed: %v", err)
	}
	select {
	case dup := <-bb3.MessageChan():
		t.Fatalf("expected persisted edit to suppress duplicate after restart, got %+v", dup)
	default:
	}
	if attachmentCalls.Load() != 0 {
		t.Fatalf("expected 0 attachment downloads on suppressed edit after restart, got %d", attachmentCalls.Load())
	}
	select {
	case rr := <-bb3.ReadReceiptChan():
		if rr.ReadUpTo != "msg-persist-1" {
			t.Fatalf("unexpected read receipt: %+v", rr)
		}
	default:
		t.Fatal("expected read receipt to still be forwarded after restart")
	}

	// A genuinely newer edit that ALSO carries dateDelivered must forward BOTH the edit AND delivered status.
	newerEditWithDelivered := json.RawMessage(`{
		"guid": "msg-persist-1",
		"isFromMe": true,
		"text": "edited twice",
		"dateCreated": 1700000000000,
		"dateEdited": 1700000020000,
		"dateDelivered": 1700000021000,
		"chats": [{"guid": "iMessage;-;+15551234567"}]
	}`)
	if err := bb3.handleMessageUpdated(newerEditWithDelivered); err != nil {
		t.Fatalf("bb3.handleMessageUpdated newer edit failed: %v", err)
	}
	select {
	case edited := <-bb3.MessageChan():
		if edited.Text != "edited twice" {
			t.Fatalf("unexpected newer edit: %+v", edited)
		}
	default:
		t.Fatal("expected newer edit to reach MessageChan")
	}
	select {
	case st := <-bb3.MessageStatusChan():
		if st.GUID != "msg-persist-1" || st.Status != "delivered" {
			t.Fatalf("unexpected delivery status: %+v", st)
		}
	default:
		t.Fatal("expected delivered status to also be forwarded alongside new edit")
	}
	bb3.SendMessageBridgeResult("iMessage;-;+15551234567", "msg-persist-1", "$evt2", true)

	// Confirming a retraction cleans up the persisted bb_edit:<guid> KV row.
	retractUpdate := json.RawMessage(`{
		"guid": "msg-persist-1",
		"isFromMe": true,
		"dateCreated": 1700000000000,
		"dateEdited": 1700000020000,
		"dateRetracted": 1700000030000,
		"chats": [{"guid": "iMessage;-;+15551234567"}]
	}`)
	if err := bb3.handleMessageUpdated(retractUpdate); err != nil {
		t.Fatalf("bb3.handleMessageUpdated retract failed: %v", err)
	}
	<-bb3.MessageChan()
	bb3.SendMessageBridgeResult("iMessage;-;+15551234567", "msg-persist-1", "$evt1", true)
	if gotKV := fb.GetKV(kvEditedPrefix + "msg-persist-1"); gotKV != "" {
		t.Fatalf("expected persisted edit KV row to be deleted on retraction, got %q", gotKV)
	}
}

func TestHealthAndAnyChatLookupDoNotBlockOnRequestLock(t *testing.T) {
	helperTrue := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{
				Status: 200,
				Data:   ServerInfo{ServerVersion: "1.9.7", PrivateAPI: true, HelperConnected: &helperTrue},
			})
		case "/api/v1/chat/any;-;+15551112222":
			_ = json.NewEncoder(w).Encode(ChatResponse{
				Status: 200,
				Data:   &Chat{GUID: "any;-;+15551112222"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)

	// Hold bbRequestLock to simulate an in-flight long media upload.
	bb.bbRequestLock.Lock()
	defer bb.bbRequestLock.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h := bb.GetHealth()
		if h.ServerVersion != "1.9.7" || h.ServerInfoErr != "" {
			t.Errorf("unexpected health while request lock held: %+v", h)
		}
		if got := bb.lookupExistingAnyChat("+15551112222"); got != "any;-;+15551112222" {
			t.Errorf("unexpected lookupExistingAnyChat while request lock held: %q", got)
		}
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("GetHealth or lookupExistingAnyChat blocked on bbRequestLock")
	}
}

func TestQueryChatsPaginationGuardAndFullPreLookbackFetch(t *testing.T) {
	now := time.Now()
	since := now.Add(-1 * time.Minute)
	twoHoursAgoMs := now.Add(-2 * time.Hour).UnixMilli()
	editedNowMs := now.Add(-10 * time.Second).UnixMilli()

	var chatQueryCalls atomic.Int32
	var fullMsgFetched atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/chat/query":
			if chatQueryCalls.Add(1) > 1 {
				t.Error("queryChats should not paginate infinitely when Limit=0")
				http.Error(w, "loop", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(ChatQueryResponse{
				Status: 200,
				Data: []Chat{
					{
						GUID: "iMessage;-;+15554445555",
						// Deliberately omit Handle on LastMessage to confirm full message lookup runs.
						LastMessage: &Message{
							GUID:        "old-edited-last",
							DateCreated: twoHoursAgoMs,
							DateEdited:  editedNowMs,
							Text:        "partial",
						},
					},
				},
				Metadata: PageMetadata{Total: 50, Limit: 0, Offset: 0, Count: 1},
			})
		case "/api/v1/chat/iMessage;-;+15554445555/message":
			_ = json.NewEncoder(w).Encode(MessageQueryResponse{
				Status:   200,
				Data:     []Message{},
				Metadata: PageMetadata{Total: 0, Limit: 100, Offset: 0, Count: 0},
			})
		case "/api/v1/message/old-edited-last":
			fullMsgFetched.Store(true)
			_ = json.NewEncoder(w).Encode(MessageResponse{
				Status: 200,
				Data: Message{
					GUID:        "old-edited-last",
					DateCreated: twoHoursAgoMs,
					DateEdited:  editedNowMs,
					Text:        "full edited text",
					Handle:      Handle{Address: "+15554445555", Service: "iMessage"},
					Chats:       []Chat{{GUID: "iMessage;-;+15554445555"}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	bb.runReconnectCatchUp(since)

	if !fullMsgFetched.Load() {
		t.Fatal("expected catch-up to fetch full message details for pre-lookback edited LastMessage")
	}
	select {
	case msg := <-bb.MessageChan():
		if msg.GUID != "old-edited-last" || msg.Text != "full edited text" || msg.Sender.LocalID != "+15554445555" {
			t.Fatalf("unexpected hydrated pre-lookback edited message: %+v", msg)
		}
	default:
		t.Fatal("expected hydrated pre-lookback edit on MessageChan")
	}
}

func TestAckingEditADoesNotPersistQueuedEditB(t *testing.T) {
	fb := newFakeBridge("http://localhost:1234", "secret-pw")
	api1, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatal(err)
	}
	bb1 := api1.(*blueBubbles)

	editA := json.RawMessage(`{
		"guid": "msg-ab",
		"isFromMe": true,
		"text": "edit A",
		"dateCreated": 1700000000000,
		"dateEdited": 1700000010000,
		"chats": [{"guid": "iMessage;-;+15551234567"}]
	}`)
	editB := json.RawMessage(`{
		"guid": "msg-ab",
		"isFromMe": true,
		"text": "edit B",
		"dateCreated": 1700000000000,
		"dateEdited": 1700000020000,
		"chats": [{"guid": "iMessage;-;+15551234567"}]
	}`)

	// Enqueue both edit A and edit B before either is acknowledged by Matrix.
	if err := bb1.handleMessageUpdated(editA); err != nil {
		t.Fatalf("handleMessageUpdated A failed: %v", err)
	}
	if err := bb1.handleMessageUpdated(editB); err != nil {
		t.Fatalf("handleMessageUpdated B failed: %v", err)
	}
	msgA := <-bb1.MessageChan()
	msgB := <-bb1.MessageChan()
	if msgA.Text != "edit A" || msgB.Text != "edit B" {
		t.Fatalf("unexpected queued edits: A=%+v B=%+v", msgA, msgB)
	}

	// Acknowledge ONLY edit A. Edit B is still unacknowledged when the bridge restarts.
	bb1.SendMessageBridgeResult("iMessage;-;+15551234567", "msg-ab", "$evtA", true)
	if gotKV := fb.GetKV(kvEditedPrefix + "msg-ab"); gotKV != "1700000010000" {
		t.Fatalf("expected only edit A timestamp (1700000010000) persisted, got %q", gotKV)
	}

	// Simulate restart: edit A must be suppressed, while edit B MUST remain retryable.
	api2, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatal(err)
	}
	bb2 := api2.(*blueBubbles)

	if err := bb2.handleMessageUpdated(editA); err != nil {
		t.Fatalf("bb2 handleMessageUpdated A failed: %v", err)
	}
	select {
	case dup := <-bb2.MessageChan():
		t.Fatalf("expected already-acked edit A to be suppressed after restart, got %+v", dup)
	default:
	}

	if err := bb2.handleMessageUpdated(editB); err != nil {
		t.Fatalf("bb2 handleMessageUpdated B failed: %v", err)
	}
	select {
	case retried := <-bb2.MessageChan():
		if retried.Text != "edit B" {
			t.Fatalf("unexpected retried edit B: %+v", retried)
		}
	default:
		t.Fatal("expected unacknowledged edit B to remain retryable after restart")
	}
}

func TestCustomEmojiTapbacksConfigAndServerVersionHelper(t *testing.T) {
	var gotReactions []SendReactionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/server/info":
			_ = json.NewEncoder(w).Encode(ServerInfoResponse{
				Status: 200,
				Data:   ServerInfo{ServerVersion: "1.9.7", PrivateAPI: true},
			})
		case "/api/v1/message/react":
			var req SendReactionRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			gotReactions = append(gotReactions, req)
			_ = json.NewEncoder(w).Encode(SendReactionResponse{
				Status: 200,
				Data:   Message{GUID: "react-1", DateCreated: 1700000000000},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	// Default config: gated off even after applyServerInfo with Private API enabled.
	bbDefault, _ := newTestConnector(t, srv.URL)
	bbDefault.refreshPrivateAPI()
	if bbDefault.Capabilities().SendCustomEmojiTapbacks {
		t.Fatal("expected SendCustomEmojiTapbacks to stay false by default after applyServerInfo")
	}

	// Explicit config opt-in: enabled when Private API is true, disabled when Private API is false.
	fb := newFakeBridge(srv.URL, "secret-pw")
	fb.cfg.BlueBubblesCustomEmojiReactions = true
	apiOptIn, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatalf("NewBlueBubblesConnector failed: %v", err)
	}
	bbOptIn := apiOptIn.(*blueBubbles)
	if bbOptIn.Capabilities().SendCustomEmojiTapbacks {
		t.Fatal("expected SendCustomEmojiTapbacks to require Private API even when opted in")
	}
	bbOptIn.refreshPrivateAPI()
	if !bbOptIn.Capabilities().SendCustomEmojiTapbacks {
		t.Fatal("expected SendCustomEmojiTapbacks to be true when opted in and Private API is active")
	}
	if _, err := bbOptIn.SendCustomEmojiTapback("iMessage;-;+15551234567", "msg-1", 0, "🎉", false); err != nil {
		t.Fatalf("SendCustomEmojiTapback failed: %v", err)
	}
	if len(gotReactions) != 1 || gotReactions[0].Reaction != "🎉" {
		t.Fatalf("unexpected custom emoji reaction request: %+v", gotReactions)
	}

	// Verify semver comparison helper behavior for future server_version auto-detection.
	if serverVersionAtLeast("1.9.7", "") {
		t.Fatal("empty minVersion must always return false")
	}
	if !serverVersionAtLeast("v1.10.0-beta.1", "1.10.0") {
		t.Fatal("expected v1.10.0-beta.1 >= 1.10.0")
	}
	if !serverVersionAtLeast("2.0", "1.10.5") {
		t.Fatal("expected 2.0 >= 1.10.5")
	}
	if serverVersionAtLeast("1.9.9", "1.10.0") {
		t.Fatal("expected 1.9.9 < 1.10.0")
	}
	if serverVersionAtLeast("invalid", "1.0.0") {
		t.Fatal("expected invalid version to return false")
	}
}

func TestReconnectCatchUpEarlyStopBoundsChatPagination(t *testing.T) {
	now := time.Now()
	since := now.Add(-2 * time.Minute)
	editLookback := since.Add(-defaultEditRetractLookback)

	var pagesRequested []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/chat/query" {
			http.NotFound(w, r)
			return
		}
		var req ChatQueryRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		pagesRequested = append(pagesRequested, req.Offset)

		switch req.Offset {
		case 0:
			// Page 1: active recent chat + quiet chat 1h ago (both within 24h scan window -> fetch page 2)
			_ = json.NewEncoder(w).Encode(ChatQueryResponse{
				Status: 200,
				Data: []Chat{
					{
						GUID:        "iMessage;-;+15550000001",
						LastMessage: &Message{GUID: "m-recent", DateCreated: now.Add(-30 * time.Second).UnixMilli()},
					},
					{
						GUID:        "iMessage;-;+15550000002",
						LastMessage: &Message{GUID: "m-quiet-untouched", DateCreated: now.Add(-1 * time.Hour).UnixMilli()},
					},
				},
				Metadata: PageMetadata{Total: 6, Limit: 2, Offset: 0, Count: 2},
			})
		case 2:
			// Page 2: quiet chat created 2h ago whose LastMessage was retracted during disconnect,
			// followed by an old chat created 48h ago (past 24h scan window -> early stop!).
			_ = json.NewEncoder(w).Encode(ChatQueryResponse{
				Status: 200,
				Data: []Chat{
					{
						GUID: "iMessage;-;+15550000003",
						LastMessage: &Message{
							GUID:          "m-2h-retracted",
							DateCreated:   now.Add(-2 * time.Hour).UnixMilli(),
							DateRetracted: now.Add(-10 * time.Second).UnixMilli(),
						},
					},
					{
						GUID:        "iMessage;-;+15550000004",
						LastMessage: &Message{GUID: "m-48h-old", DateCreated: now.Add(-48 * time.Hour).UnixMilli()},
					},
				},
				Metadata: PageMetadata{Total: 6, Limit: 2, Offset: 2, Count: 2},
			})
		default:
			t.Errorf("early stop failed: requested historical chat page at offset=%d", req.Offset)
			http.Error(w, "should have stopped paginating", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	bb, _ := newTestConnector(t, srv.URL)
	active, err := bb.getChatsWithActivityAfter(since, editLookback)
	if err != nil {
		t.Fatalf("getChatsWithActivityAfter failed: %v", err)
	}
	if len(pagesRequested) != 2 || pagesRequested[0] != 0 || pagesRequested[1] != 2 {
		t.Fatalf("expected exactly 2 chat pages [0, 2], got %v", pagesRequested)
	}
	if len(active) != 2 {
		t.Fatalf("expected 2 active chats (recent + 2h-old retracted quiet chat), got %d: %+v", len(active), active)
	}
	if active[0].GUID != "iMessage;-;+15550000001" || active[1].GUID != "iMessage;-;+15550000003" {
		t.Fatalf("unexpected active chats: %+v", active)
	}
}

func TestEditKVCleanupPrunesExpiredEntries(t *testing.T) {
	fb := newFakeBridge("http://localhost:1234", "secret-pw")
	api, err := NewBlueBubblesConnector(fb)
	if err != nil {
		t.Fatal(err)
	}
	bb := api.(*blueBubbles)
	bb.editKVTTL = 7 * 24 * time.Hour
	bb.editKVCleanupInterval = 20 * time.Millisecond

	now := time.Now()
	expiredTS := now.Add(-10 * 24 * time.Hour).UnixMilli()
	freshTS := now.Add(-2 * 24 * time.Hour).UnixMilli()

	bb.markEdited("msg-expired", expiredTS)
	bb.markEdited("msg-fresh", freshTS)
	fb.SetKV("unrelated_key", strconv.FormatInt(expiredTS, 10))

	// Run cleanup loop briefly and verify both initial cleanup and clean shutdown on Stop().
	done := make(chan struct{})
	go func() {
		defer close(done)
		bb.editKVCleanupLoop()
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fb.GetKV(kvEditedPrefix+"msg-expired") == "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := fb.GetKV(kvEditedPrefix + "msg-expired"); got != "" {
		t.Fatalf("expected expired bb_edit KV entry to be pruned, got %q", got)
	}
	if got := fb.GetKV(kvEditedPrefix + "msg-fresh"); got != strconv.FormatInt(freshTS, 10) {
		t.Fatalf("expected fresh bb_edit KV entry to remain, got %q", got)
	}
	if got := fb.GetKV("unrelated_key"); got != strconv.FormatInt(expiredTS, 10) {
		t.Fatalf("expected unrelated KV entry to remain untouched, got %q", got)
	}

	bb.editedLock.Lock()
	_, expiredInMem := bb.lastEdited["msg-expired"]
	freshInMemTS := bb.lastEdited["msg-fresh"]
	bb.editedLock.Unlock()
	if expiredInMem {
		t.Fatal("expected expired entry to also be pruned from in-memory lastEdited")
	}
	if freshInMemTS != freshTS {
		t.Fatalf("expected fresh in-memory lastEdited entry %d, got %d", freshTS, freshInMemTS)
	}

	// Verify periodic tick prunes an entry added after loop startup.
	bb.markEdited("msg-expired-later", expiredTS)
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if fb.GetKV(kvEditedPrefix+"msg-expired-later") == "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := fb.GetKV(kvEditedPrefix + "msg-expired-later"); got != "" {
		t.Fatalf("expected periodic tick to prune msg-expired-later, got %q", got)
	}

	bb.Stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("editKVCleanupLoop did not exit cleanly on Stop()")
	}
}

package bluebubbles

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/sahilm/fuzzy"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-imessage/imessage"
)

const (
	// Ref: https://github.com/BlueBubblesApp/bluebubbles-server/blob/master/packages/server/src/server/events.ts
	NewMessage            string = "new-message"
	MessageSendError      string = "message-send-error"
	MessageUpdated        string = "updated-message"
	ParticipantRemoved    string = "participant-removed"
	ParticipantAdded      string = "participant-added"
	ParticipantLeft       string = "participant-left"
	GroupIconChanged      string = "group-icon-changed"
	GroupIconRemoved      string = "group-icon-removed"
	ChatReadStatusChanged string = "chat-read-status-changed"
	TypingIndicator       string = "typing-indicator"
	GroupNameChanged      string = "group-name-change"
	IMessageAliasRemoved  string = "imessage-alias-removed"

	defaultHTTPTimeout            = 30 * time.Second
	defaultProbeHTTPTimeout       = 5 * time.Second
	defaultSendHTTPTimeout        = 2 * time.Minute
	defaultMediaHTTPTimeout       = 2 * time.Minute
	defaultWSReadTimeout          = 90 * time.Second
	defaultWSWriteTimeout         = 15 * time.Second
	defaultWSHandshakeTimeout     = 15 * time.Second
	defaultReconnectBaseDelay     = 1 * time.Second
	defaultReconnectMaxDelay      = 60 * time.Second
	defaultContactRefreshInterval = 1 * time.Hour
	defaultPendingEditTTL         = 60 * time.Second
	defaultPendingGroupActionTTL  = 60 * time.Second
	defaultCatchUpExtraMargin     = 30 * time.Second
	defaultEditRetractLookback    = 20 * time.Minute
	kvEditedPrefix                = "bb_edit:"
	maxTrackedEditedMessages      = 10000
)

type pendingEdit struct {
	text      string
	expiresAt time.Time
}

type pendingGroupName struct {
	name      string
	expiresAt time.Time
}

var (
	_ imessage.API                         = (*blueBubbles)(nil)
	_ imessage.VenturaFeatures             = (*blueBubbles)(nil)
	_ imessage.CustomEmojiTapbackAPI       = (*blueBubbles)(nil)
	_ imessage.GroupCreationWithMessageAPI = (*blueBubbles)(nil)
	_ imessage.GroupAdminAPI               = (*blueBubbles)(nil)
	_ imessage.HealthAPI                   = (*blueBubbles)(nil)
)

type blueBubbles struct {
	bridge             imessage.Bridge
	log                zerolog.Logger
	ws                 *websocket.Conn
	wsMu               sync.Mutex
	wsWriteLock        sync.Mutex
	wsReadTimeout      atomic.Int64
	wsHandshakeTimeout time.Duration
	messageChan        chan *imessage.Message
	receiptChan        chan *imessage.ReadReceipt
	typingChan         chan *imessage.TypingNotification
	chatChan           chan *imessage.ChatInfo
	contactChan        chan *imessage.Contact
	messageStatusChan  chan *imessage.SendMessageStatus
	backfillTaskChan   chan *imessage.BackfillTask

	contactsLock           sync.RWMutex
	contactsLastRefresh    time.Time
	contacts               []Contact
	contactRefreshInterval time.Duration

	editedLock          sync.Mutex
	lastEdited          map[string]int64
	inflightEdits       map[string][]int64
	inflightRetractions map[string]struct{}
	pendingEdits        map[string]pendingEdit
	pendingGroupNames   map[string]pendingGroupName

	httpClient      *http.Client
	probeHTTPClient *http.Client
	sendHTTPClient  *http.Client
	mediaHTTPClient *http.Client
	bbRequestLock   sync.Mutex

	serviceCacheLock       sync.RWMutex
	resolvedHandleServices map[string]string
	chatServices           map[string]string

	reconnectBaseDelay time.Duration
	reconnectMaxDelay  time.Duration

	ctxMu  sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	wsConnected    atomic.Bool
	connectedSince atomic.Int64
	disconnectedAt atomic.Int64
	lastEventAt    atomic.Int64
	reconnectCount atomic.Int64

	healthMu          sync.RWMutex
	serverInfo        ServerInfo
	hasServerInfo     bool
	lastDisconnectErr string
	lastCatchUpAt     time.Time
	lastCatchUpChats  int
	lastCatchUpMsgs   int
	lastCatchUpErr    string

	catchUpMu      sync.Mutex
	catchUpOverlap time.Duration

	usingPrivateAPI atomic.Bool
	// Outbound custom emoji tapbacks are intentionally gated off by default as
	// groundwork until upstream BlueBubbles Server's /api/v1/message/react
	// validator accepts custom emoji. Inbound 2006/3006 custom emoji reactions
	// remain active regardless of this flag.
	supportsCustomEmojiTapbacks atomic.Bool
}

func NewBlueBubblesConnector(bridge imessage.Bridge) (imessage.API, error) {
	ctx, cancel := context.WithCancel(context.Background())
	bb := &blueBubbles{
		bridge: bridge,
		log:    bridge.GetZLog().With().Str("component", "bluebubbles").Logger(),

		wsHandshakeTimeout: defaultWSHandshakeTimeout,

		messageChan:       make(chan *imessage.Message, 256),
		receiptChan:       make(chan *imessage.ReadReceipt, 32),
		typingChan:        make(chan *imessage.TypingNotification, 32),
		chatChan:          make(chan *imessage.ChatInfo, 32),
		contactChan:       make(chan *imessage.Contact, 2048),
		messageStatusChan: make(chan *imessage.SendMessageStatus, 32),
		backfillTaskChan:  make(chan *imessage.BackfillTask, 32),

		contactRefreshInterval: defaultContactRefreshInterval,
		lastEdited:             make(map[string]int64),
		inflightEdits:          make(map[string][]int64),
		inflightRetractions:    make(map[string]struct{}),
		pendingEdits:           make(map[string]pendingEdit),
		pendingGroupNames:      make(map[string]pendingGroupName),

		httpClient:      &http.Client{Timeout: defaultHTTPTimeout},
		probeHTTPClient: &http.Client{Timeout: defaultProbeHTTPTimeout},
		sendHTTPClient:  &http.Client{Timeout: defaultSendHTTPTimeout},
		mediaHTTPClient: &http.Client{Timeout: defaultMediaHTTPTimeout},

		resolvedHandleServices: make(map[string]string),
		chatServices:           make(map[string]string),

		reconnectBaseDelay: defaultReconnectBaseDelay,
		reconnectMaxDelay:  defaultReconnectMaxDelay,

		ctx:    ctx,
		cancel: cancel,
	}
	bb.wsReadTimeout.Store(int64(defaultWSReadTimeout))
	return bb, nil
}

func init() {
	imessage.Implementations["bluebubbles"] = NewBlueBubblesConnector
}

func (bb *blueBubbles) context() context.Context {
	bb.ctxMu.RLock()
	ctx := bb.ctx
	bb.ctxMu.RUnlock()
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

type httpClientKind int

const (
	clientDefault httpClientKind = iota
	clientProbe
	clientSend
	clientMedia
)

func (bb *blueBubbles) getHTTPClient(kind httpClientKind) *http.Client {
	switch kind {
	case clientProbe:
		if bb.probeHTTPClient != nil {
			return bb.probeHTTPClient
		}
		return &http.Client{Timeout: defaultProbeHTTPTimeout}
	case clientSend:
		if bb.sendHTTPClient != nil {
			return bb.sendHTTPClient
		}
		return &http.Client{Timeout: defaultSendHTTPTimeout}
	case clientMedia:
		if bb.mediaHTTPClient != nil {
			return bb.mediaHTTPClient
		}
		return &http.Client{Timeout: defaultMediaHTTPTimeout}
	default:
		if bb.httpClient != nil {
			return bb.httpClient
		}
		return &http.Client{Timeout: defaultHTTPTimeout}
	}
}

func (bb *blueBubbles) sendBridgeState(stateEvent, errCode, message string) {
	if bb.bridge == nil {
		return
	}
	bb.bridge.SendBridgeStatus(imessage.BridgeStatus{
		StateEvent: stateEvent,
		Error:      errCode,
		Message:    message,
		RemoteID:   bb.bridge.GetConnectorConfig().BlueBubblesURL,
	})
}

func (bb *blueBubbles) refreshPrivateAPI() {
	info, err := bb.fetchServerInfo()
	if err != nil {
		bb.log.Warn().Err(err).Bool("keepingPrivateAPI", bb.usingPrivateAPI.Load()).Msg("Failed to refresh BlueBubbles server info; keeping previous Private API state")
		return
	}
	bb.applyServerInfo(info)
}

func (bb *blueBubbles) applyServerInfo(info ServerInfo) {
	bb.usingPrivateAPI.Store(info.PrivateAPI)
	bb.healthMu.Lock()
	bb.serverInfo = info
	bb.hasServerInfo = true
	bb.healthMu.Unlock()
	if info.PrivateAPI && info.HelperConnected != nil && !*info.HelperConnected {
		bb.log.Warn().
			Str("serverVersion", info.ServerVersion).
			Str("osVersion", info.OSVersion).
			Msg("BlueBubbles reports Private API enabled, but helper_connected is false; Private API actions may fail until the helper reconnects to Messages.app")
	}
}

func (bb *blueBubbles) GetHealth() *imessage.ConnectorHealth {
	health := &imessage.ConnectorHealth{
		WSConnected:    bb.wsConnected.Load(),
		ReconnectCount: bb.reconnectCount.Load(),
	}
	if ts := bb.connectedSince.Load(); ts > 0 {
		health.ConnectedSince = time.Unix(0, ts)
	}
	if ts := bb.disconnectedAt.Load(); ts > 0 {
		health.DisconnectedSince = time.Unix(0, ts)
	}
	if ts := bb.lastEventAt.Load(); ts > 0 {
		health.LastEventAt = time.Unix(0, ts)
	}

	info, err := bb.fetchServerInfo()
	if err != nil {
		health.ServerInfoErr = err.Error()
	} else {
		bb.applyServerInfo(info)
	}

	bb.healthMu.RLock()
	if bb.hasServerInfo {
		health.ServerVersion = bb.serverInfo.ServerVersion
		health.OSVersion = bb.serverInfo.OSVersion
		health.ProxyService = bb.serverInfo.ProxyService
		health.PrivateAPI = bb.serverInfo.PrivateAPI
		if bb.serverInfo.HelperConnected != nil {
			v := *bb.serverInfo.HelperConnected
			health.HelperConnected = &v
		}
	} else {
		health.PrivateAPI = bb.usingPrivateAPI.Load()
	}
	health.LastDisconnectErr = bb.lastDisconnectErr
	health.LastCatchUpAt = bb.lastCatchUpAt
	health.LastCatchUpChats = bb.lastCatchUpChats
	health.LastCatchUpMsgs = bb.lastCatchUpMsgs
	health.LastCatchUpErr = bb.lastCatchUpErr
	bb.healthMu.RUnlock()

	return health
}

func (bb *blueBubbles) Start(readyCallback func()) error {
	bb.log.Trace().Msg("Start")

	bb.ctxMu.Lock()
	if bb.ctx == nil || bb.ctx.Err() != nil {
		bb.ctx, bb.cancel = context.WithCancel(context.Background())
	}
	bb.ctxMu.Unlock()

	// Preload some caches
	bb.refreshPrivateAPI()
	if err := bb.RefreshContactList(); err != nil {
		bb.log.Warn().Err(err).Msg("Failed to preload contact list from BlueBubbles")
	}

	ws, err := bb.connectToWebSocket()
	if err != nil {
		bb.wsConnected.Store(false)
		bb.disconnectedAt.Store(time.Now().UnixNano())
		bb.healthMu.Lock()
		bb.lastDisconnectErr = err.Error()
		bb.healthMu.Unlock()
		bb.sendBridgeState("TRANSIENT_DISCONNECT", "bb-connect-failed", err.Error())
		return err
	}
	bb.setWS(ws)
	bb.wsConnected.Store(true)
	bb.connectedSince.Store(time.Now().UnixNano())
	bb.disconnectedAt.Store(0)
	bb.healthMu.Lock()
	bb.lastDisconnectErr = ""
	bb.healthMu.Unlock()
	bb.sendBridgeState("CONNECTED", "", "")

	go bb.wsLoop()
	go bb.contactRefreshLoop()

	// Notify main this API is fully loaded
	readyCallback()

	return nil
}

func (bb *blueBubbles) Stop() {
	bb.log.Trace().Msg("Stop")
	bb.ctxMu.RLock()
	cancel := bb.cancel
	bb.ctxMu.RUnlock()
	if cancel != nil {
		cancel()
	}
	bb.stopListening()
}

func (bb *blueBubbles) contactRefreshLoop() {
	interval := bb.contactRefreshInterval
	if interval <= 0 {
		interval = defaultContactRefreshInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-bb.context().Done():
			return
		case <-ticker.C:
			if err := bb.RefreshContactList(); err != nil {
				bb.log.Warn().Err(err).Msg("Failed to periodically refresh contacts from BlueBubbles")
			}
		}
	}
}

func (bb *blueBubbles) setWS(ws *websocket.Conn) {
	bb.wsMu.Lock()
	bb.ws = ws
	bb.wsMu.Unlock()
}

func (bb *blueBubbles) getWS() *websocket.Conn {
	bb.wsMu.Lock()
	ws := bb.ws
	bb.wsMu.Unlock()
	return ws
}

func (bb *blueBubbles) writeWSMessage(messageType int, data []byte) error {
	ws := bb.getWS()
	if ws == nil {
		return errors.New("websocket is not connected")
	}
	bb.wsWriteLock.Lock()
	defer bb.wsWriteLock.Unlock()
	_ = ws.SetWriteDeadline(time.Now().Add(defaultWSWriteTimeout))
	return ws.WriteMessage(messageType, data)
}

func (bb *blueBubbles) applyHandshakePayload(payload []byte) {
	var hs SocketIOHandshake
	if err := json.Unmarshal(payload, &hs); err != nil {
		bb.log.Warn().Err(err).Msg("Failed to parse Socket.IO handshake")
		return
	}
	if hs.PingInterval > 0 || hs.PingTimeout > 0 {
		timeout := time.Duration(hs.PingInterval+hs.PingTimeout)*time.Millisecond + 15*time.Second
		if timeout > 0 {
			bb.wsReadTimeout.Store(int64(timeout))
		}
		bb.log.Debug().Int64("pingIntervalMs", hs.PingInterval).Int64("pingTimeoutMs", hs.PingTimeout).Msg("Connected to BlueBubbles Socket.IO")
	}
}

func (bb *blueBubbles) connectToWebSocket() (*websocket.Conn, error) {
	wsURL, err := bb.wsUrl()
	if err != nil {
		return nil, err
	}
	handshakeTimeout := bb.wsHandshakeTimeout
	if handshakeTimeout <= 0 {
		handshakeTimeout = defaultWSHandshakeTimeout
	}

	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = handshakeTimeout
	ws, resp, err := dialer.DialContext(bb.context(), wsURL, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, bb.sanitizeError(err)
	}

	deadline := time.Now().Add(handshakeTimeout)
	_ = ws.SetWriteDeadline(deadline)
	if err := ws.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		_ = ws.Close()
		return nil, err
	}

	// Wait for the server's Socket.IO namespace acceptance ("40") within the handshake deadline.
	for {
		if err := bb.context().Err(); err != nil {
			_ = ws.Close()
			return nil, err
		}
		_ = ws.SetReadDeadline(deadline)
		_, payload, err := ws.ReadMessage()
		if err != nil {
			_ = ws.Close()
			return nil, fmt.Errorf("failed waiting for BlueBubbles Socket.IO namespace acknowledgment: %w", err)
		}

		switch {
		case bytes.HasPrefix(payload, []byte("0{")):
			bb.applyHandshakePayload(payload[1:])
		case bytes.Equal(payload, []byte("2")):
			_ = ws.SetWriteDeadline(deadline)
			if err := ws.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
				_ = ws.Close()
				return nil, fmt.Errorf("failed to write pong during handshake: %w", err)
			}
		case bytes.HasPrefix(payload, []byte("2")):
			_ = ws.SetWriteDeadline(deadline)
			if err := ws.WriteMessage(websocket.TextMessage, append([]byte("3"), payload[1:]...)); err != nil {
				_ = ws.Close()
				return nil, fmt.Errorf("failed to write probe pong during handshake: %w", err)
			}
		case bytes.HasPrefix(payload, []byte("40")):
			bb.log.Debug().Msg("BlueBubbles Socket.IO namespace connected")
			return ws, nil
		case bytes.HasPrefix(payload, []byte("44")):
			_ = ws.Close()
			return nil, fmt.Errorf("BlueBubbles Socket.IO namespace rejected: %s", strings.TrimSpace(string(payload[2:])))
		case bytes.HasPrefix(payload, []byte("41")):
			_ = ws.Close()
			return nil, errors.New("BlueBubbles websocket disconnected during handshake")
		}
	}
}

func (bb *blueBubbles) wsLoop() {
	for {
		err := bb.pollMessages()
		if err == nil {
			continue
		}
		if bb.context().Err() != nil {
			bb.log.Debug().Msg("BlueBubbles WebSocket loop stopped")
			return
		}
		bb.log.Error().Err(err).Msg("Error polling messages from WebSocket")
		bb.wsConnected.Store(false)
		bb.disconnectedAt.CompareAndSwap(0, time.Now().UnixNano())
		bb.healthMu.Lock()
		bb.lastDisconnectErr = err.Error()
		bb.healthMu.Unlock()
		bb.stopListening()
		bb.sendBridgeState("TRANSIENT_DISCONNECT", "bb-ws-disconnected", err.Error())

		if !bb.reconnect() {
			return
		}
	}
}

func (bb *blueBubbles) pollMessages() error {
	ws := bb.getWS()
	if ws == nil {
		return errors.New("websocket is not connected")
	}

	readTimeout := time.Duration(bb.wsReadTimeout.Load())
	if readTimeout <= 0 {
		readTimeout = defaultWSReadTimeout
	}
	_ = ws.SetReadDeadline(time.Now().Add(readTimeout))

	_, payload, err := ws.ReadMessage()
	if err != nil {
		return err
	}

	// Engine.IO v4 open handshake: 0{"sid":"...","pingInterval":25000,"pingTimeout":20000}
	if bytes.HasPrefix(payload, []byte("0{")) {
		bb.applyHandshakePayload(payload[1:])
		return nil
	}

	// Engine.IO v4 ping ("2" or "2probe") -> pong ("3" or "3probe")
	if bytes.Equal(payload, []byte("2")) {
		bb.log.Debug().Msg("Received ping from BlueBubbles websocket")
		if err := bb.writeWSMessage(websocket.TextMessage, []byte("3")); err != nil {
			return fmt.Errorf("failed to write pong: %w", err)
		}
		return nil
	}
	if bytes.HasPrefix(payload, []byte("2")) {
		pong := append([]byte("3"), payload[1:]...)
		if err := bb.writeWSMessage(websocket.TextMessage, pong); err != nil {
			return fmt.Errorf("failed to write probe pong: %w", err)
		}
		return nil
	}

	// Socket.IO v4 namespace connect ack ("40...")
	if bytes.HasPrefix(payload, []byte("40")) {
		bb.log.Debug().Msg("BlueBubbles Socket.IO namespace connected")
		return nil
	}

	// Socket.IO v4 namespace connect error ("44...")
	if bytes.HasPrefix(payload, []byte("44")) {
		return fmt.Errorf("BlueBubbles Socket.IO namespace error: %s", strings.TrimSpace(string(payload[2:])))
	}

	// Socket.IO v4 namespace disconnect ("41...")
	if bytes.HasPrefix(payload, []byte("41")) {
		return errors.New("BlueBubbles websocket requested disconnect")
	}

	if bytes.HasPrefix(payload, []byte("42")) {
		payload = bytes.TrimPrefix(payload, []byte("42"))

		var incomingWebsocketMessage []json.RawMessage
		if err := json.Unmarshal(payload, &incomingWebsocketMessage); err != nil {
			bb.log.Error().Err(err).Msg("Error parsing message from BlueBubbles websocket")
			return nil
		}
		if len(incomingWebsocketMessage) < 1 {
			bb.log.Warn().Msg("Received empty event array from BlueBubbles websocket")
			return nil
		}

		var websocketMessageType string
		if err := json.Unmarshal(incomingWebsocketMessage[0], &websocketMessageType); err != nil {
			bb.log.Error().Err(err).Msg("Error parsing message type from BlueBubbles websocket")
			return nil
		}
		if len(incomingWebsocketMessage) < 2 {
			bb.log.Warn().Str("event", websocketMessageType).Msg("Received BlueBubbles websocket event without payload")
			return nil
		}
		bb.lastEventAt.Store(time.Now().UnixNano())

		switch websocketMessageType {
		case NewMessage:
			err = bb.handleNewMessage(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling new message")
			}
		case MessageSendError:
			err = bb.handleMessageSendError(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling message send error")
			}
		case MessageUpdated:
			err = bb.handleMessageUpdated(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling message updated")
			}
		case ParticipantRemoved:
			err = bb.handleParticipantRemoved(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling participant removed")
			}
		case ParticipantAdded:
			err = bb.handleParticipantAdded(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling participant added")
			}
		case ParticipantLeft:
			err = bb.handleParticipantLeft(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling participant left")
			}
		case GroupIconChanged:
			err = bb.handleGroupIconChanged(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling group icon changed")
			}
		case GroupIconRemoved:
			err = bb.handleGroupIconRemoved(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling group icon removed")
			}
		case ChatReadStatusChanged:
			err = bb.handleChatReadStatusChanged(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling chat read status changed")
			}
		case TypingIndicator:
			err = bb.handleTypingIndicator(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling typing indicator")
			}
		case GroupNameChanged:
			err = bb.handleGroupNameChanged(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling group name changed")
			}
		case IMessageAliasRemoved:
			err = bb.handleIMessageAliasRemoved(incomingWebsocketMessage[1])
			if err != nil {
				bb.log.Error().Err(err).Msg("Error handling iMessage alias removed")
			}
		default:
			bb.log.Warn().Str("WebsocketMessageType", websocketMessageType).Msg("Unknown websocket message type")
		}
	}
	return nil
}

func (bb *blueBubbles) reconnect() bool {
	baseDelay := bb.reconnectBaseDelay
	if baseDelay <= 0 {
		baseDelay = defaultReconnectBaseDelay
	}
	maxDelay := bb.reconnectMaxDelay
	if maxDelay <= 0 {
		maxDelay = defaultReconnectMaxDelay
	}

	delay := baseDelay
	attempt := 0
	for {
		attempt++
		// Apply ±20% jitter so reconnects don't synchronize
		jitterFactor := 0.8 + rand.Float64()*0.4
		sleepTime := time.Duration(float64(delay) * jitterFactor)
		bb.log.Info().Int("attempt", attempt).Dur("sleepTime", sleepTime).Msg("Sleeping before reconnecting to BlueBubbles WebSocket...")

		timer := time.NewTimer(sleepTime)
		select {
		case <-bb.context().Done():
			timer.Stop()
			return false
		case <-timer.C:
		}

		ws, err := bb.connectToWebSocket()
		if err != nil {
			bb.log.Error().Err(err).Int("attempt", attempt).Msg("Error reconnecting to BlueBubbles WebSocket")
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
			continue
		}

		bb.setWS(ws)
		now := time.Now()
		prevDisconnect := bb.disconnectedAt.Swap(0)
		bb.wsConnected.Store(true)
		bb.connectedSince.Store(now.UnixNano())
		bb.reconnectCount.Add(1)
		bb.healthMu.Lock()
		bb.lastDisconnectErr = ""
		bb.healthMu.Unlock()

		bb.refreshPrivateAPI()
		bb.sendBridgeState("CONNECTED", "", "")
		bb.log.Info().Int("attempt", attempt).Msg("Successfully reconnected to BlueBubbles WebSocket")

		catchUpSince := bb.computeCatchUpSince(prevDisconnect, now)
		go bb.runReconnectCatchUp(catchUpSince)
		return true
	}
}

func (bb *blueBubbles) computeCatchUpSince(disconnectedAtNano int64, now time.Time) time.Time {
	overlap := bb.catchUpOverlap
	if overlap <= 0 {
		readTimeout := time.Duration(bb.wsReadTimeout.Load())
		if readTimeout <= 0 {
			readTimeout = defaultWSReadTimeout
		}
		overlap = readTimeout + defaultCatchUpExtraMargin
	}
	base := now
	if disconnectedAtNano > 0 {
		base = time.Unix(0, disconnectedAtNano)
	}
	return base.Add(-overlap)
}

func (bb *blueBubbles) getChatsWithActivityAfter(since, editLookback time.Time) ([]Chat, error) {
	request := ChatQueryRequest{
		Limit:  1000,
		Offset: 0,
		With: []ChatQueryWith{
			ChatQueryWithLastMessage,
			ChatQueryWithSMS,
		},
		Sort: QuerySortLastMessage,
	}
	chats, err := bb.queryChats(request, []Chat{})
	if err != nil {
		return nil, err
	}
	sinceMs := since.UnixMilli()
	lookbackMs := editLookback.UnixMilli()
	active := make([]Chat, 0)
	for _, chat := range chats {
		if chat.LastMessage == nil {
			continue
		}
		lm := chat.LastMessage
		if lm.DateCreated >= lookbackMs || lm.DateEdited >= sinceMs || lm.DateRetracted >= sinceMs {
			active = append(active, chat)
		}
	}
	return active, nil
}

func (bb *blueBubbles) runReconnectCatchUp(since time.Time) {
	bb.catchUpMu.Lock()
	defer bb.catchUpMu.Unlock()

	if bb.context().Err() != nil {
		return
	}

	editLookback := since.Add(-defaultEditRetractLookback)
	bb.log.Info().Time("since", since).Time("editLookback", editLookback).Msg("Starting reconnect catch-up sync")
	chats, err := bb.getChatsWithActivityAfter(since, editLookback)
	if err != nil {
		bb.log.Warn().Err(err).Time("since", since).Msg("Reconnect catch-up sync failed to query active chats")
		bb.healthMu.Lock()
		bb.lastCatchUpAt = time.Now()
		bb.lastCatchUpChats = 0
		bb.lastCatchUpMsgs = 0
		bb.lastCatchUpErr = err.Error()
		bb.healthMu.Unlock()
		return
	}

	totalEnqueued := 0
	var firstErr error
	for _, chat := range chats {
		if bb.context().Err() != nil {
			return
		}
		enqueued, chatErr := bb.catchUpChat(chat, since, editLookback)
		if chatErr != nil {
			bb.log.Warn().Err(chatErr).Str("chatGUID", chat.GUID).Msg("Reconnect catch-up sync failed for chat")
			if firstErr == nil {
				firstErr = chatErr
			}
			continue
		}
		totalEnqueued += enqueued
	}

	bb.healthMu.Lock()
	bb.lastCatchUpAt = time.Now()
	bb.lastCatchUpChats = len(chats)
	bb.lastCatchUpMsgs = totalEnqueued
	if firstErr != nil {
		bb.lastCatchUpErr = firstErr.Error()
	} else {
		bb.lastCatchUpErr = ""
	}
	bb.healthMu.Unlock()

	bb.log.Info().Int("chats", len(chats)).Int("messages", totalEnqueued).Time("since", since).Msg("Finished reconnect catch-up sync")
}

func (bb *blueBubbles) catchUpChat(chat Chat, since, editLookback time.Time) (int, error) {
	chatID := chat.GUID
	sinceMs := since.UnixMilli()
	after := editLookback.UnixMilli()
	request := MessageQueryRequest{
		ChatGUID: chatID,
		Limit:    100,
		Offset:   0,
		With:     defaultMessageQueryWith(),
		After:    &after,
		Sort:     MessageQuerySortDesc,
	}

	rawMessages, err := bb.queryChatMessages(request, []Message{}, true)
	if err != nil {
		return 0, err
	}

	// Build chronological candidate list (oldest -> newest) so replies and tapbacks
	// follow their target messages. Also include a pre-lookback LastMessage if it was
	// edited or retracted during the disconnect window.
	candidates := make([]Message, 0, len(rawMessages)+1)
	seenCandidateGUIDs := make(map[string]struct{}, len(rawMessages)+1)
	if chat.LastMessage != nil && chat.LastMessage.GUID != "" &&
		(chat.LastMessage.DateRetracted >= sinceMs || chat.LastMessage.DateEdited >= sinceMs) {
		inRaw := false
		for _, r := range rawMessages {
			if r.GUID == chat.LastMessage.GUID {
				inRaw = true
				break
			}
		}
		if !inRaw {
			lm := *chat.LastMessage
			if full, fetchErr := bb.getMessageWithDetails(chat.LastMessage.GUID); fetchErr == nil && full != nil && full.GUID != "" {
				lm = *full
			}
			if len(lm.Chats) == 0 {
				lm.Chats = []Chat{{GUID: chatID, GroupID: chat.GroupID}}
			}
			candidates = append(candidates, lm)
			seenCandidateGUIDs[lm.GUID] = struct{}{}
		}
	}
	for i := len(rawMessages) - 1; i >= 0; i-- {
		raw := rawMessages[i]
		if _, dup := seenCandidateGUIDs[raw.GUID]; dup {
			continue
		}
		seenCandidateGUIDs[raw.GUID] = struct{}{}
		candidates = append(candidates, raw)
	}
	if len(candidates) == 0 {
		return 0, nil
	}

	// Build a set of already-bridged message GUIDs so we skip downloading attachments
	// for messages that already arrived before/during the overlap window.
	seenGUIDs := make(map[string]struct{})
	if bb.bridge != nil {
		for _, guid := range bb.bridge.GetMessagesSince(chatID, editLookback.Add(-5*time.Minute)) {
			seenGUIDs[guid] = struct{}{}
		}
	}

	enqueued := 0
	for _, raw := range candidates {
		if bb.context().Err() != nil {
			return enqueued, bb.context().Err()
		}
		if len(raw.Chats) == 0 {
			raw.Chats = []Chat{{GUID: chatID, GroupID: chat.GroupID}}
		}

		_, alreadySeen := seenGUIDs[raw.GUID]
		isRetracted := raw.DateRetracted != 0
		isNewEdit := false
		if raw.DateEdited != 0 {
			if bb.consumePendingEdit(raw.GUID, raw.Text) {
				bb.markEdited(raw.GUID, raw.DateEdited)
			} else {
				isNewEdit = bb.isNewEdit(raw.GUID, raw.DateEdited)
			}
		}

		if raw.DateCreated < sinceMs {
			// Pre-window message: only replay if it was retracted or newly edited during the disconnect window.
			retractedInWindow := raw.DateRetracted >= sinceMs
			editedInWindow := raw.DateEdited >= sinceMs && isNewEdit
			if !retractedInWindow && !editedInWindow {
				continue
			}
		} else if alreadySeen && !isRetracted && !isNewEdit {
			continue
		}

		downloadAttachments := !isRetracted && (!alreadySeen || bb.shouldDownloadEditAttachments(len(raw.Attachments)))
		converted, convErr := bb.convertBBMessageToiMessageWithOptions(raw, downloadAttachments)
		if convErr != nil {
			bb.log.Warn().Err(convErr).Str("guid", raw.GUID).Str("chatGUID", chatID).Msg("Failed to convert catch-up message")
			continue
		}

		select {
		case bb.messageChan <- converted:
			enqueued++
			if isRetracted {
				bb.markRetractionInflight(raw.GUID)
			} else if raw.DateEdited != 0 {
				bb.markEditInflight(raw.GUID, raw.DateEdited)
			}
		case <-bb.context().Done():
			converted.CleanupTempAttachments()
			return enqueued, bb.context().Err()
		}
	}

	return enqueued, nil
}

func (bb *blueBubbles) stopListening() {
	bb.wsConnected.Store(false)
	bb.wsMu.Lock()
	ws := bb.ws
	bb.ws = nil
	bb.wsMu.Unlock()

	if ws != nil {
		bb.wsWriteLock.Lock()
		_ = ws.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_ = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		bb.wsWriteLock.Unlock()
		_ = ws.Close()
	}
}

func (bb *blueBubbles) getPersistedEditTS(guid string) int64 {
	kv, ok := bb.bridge.(imessage.KVStore)
	if !ok || kv == nil {
		return 0
	}
	raw := kv.GetKV(kvEditedPrefix + guid)
	if raw == "" {
		return 0
	}
	ts, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return ts
}

func (bb *blueBubbles) persistEditTS(guid string, dateEdited int64) {
	kv, ok := bb.bridge.(imessage.KVStore)
	if !ok || kv == nil {
		return
	}
	kv.SetKV(kvEditedPrefix+guid, strconv.FormatInt(dateEdited, 10))
}

func (bb *blueBubbles) deletePersistedEditTS(guid string) {
	kv, ok := bb.bridge.(imessage.KVStore)
	if !ok || kv == nil {
		return
	}
	kv.DeleteKV(kvEditedPrefix + guid)
}

func (bb *blueBubbles) shouldDownloadEditAttachments(attachmentCount int) bool {
	if attachmentCount == 0 || attachmentCount > 1 {
		return false
	}
	if cfg, ok := bb.bridge.(imessage.CaptionConfig); ok && cfg != nil && !cfg.IsCaptionInMessage() {
		return false
	}
	return true
}

func (bb *blueBubbles) isNewEdit(guid string, dateEdited int64) bool {
	if guid == "" || dateEdited == 0 {
		return false
	}
	bb.editedLock.Lock()
	if queue := bb.inflightEdits[guid]; len(queue) > 0 {
		maxInflight := queue[len(queue)-1]
		for _, ts := range queue {
			if ts > maxInflight {
				maxInflight = ts
			}
		}
		if dateEdited <= maxInflight {
			bb.editedLock.Unlock()
			return false
		}
	}
	if prev, ok := bb.lastEdited[guid]; ok {
		bb.editedLock.Unlock()
		return dateEdited > prev
	}
	bb.editedLock.Unlock()

	if persisted := bb.getPersistedEditTS(guid); persisted > 0 {
		bb.editedLock.Lock()
		if bb.lastEdited == nil {
			bb.lastEdited = make(map[string]int64)
		}
		if prev, ok := bb.lastEdited[guid]; !ok || persisted > prev {
			if len(bb.lastEdited) >= maxTrackedEditedMessages {
				bb.lastEdited = make(map[string]int64)
			}
			bb.lastEdited[guid] = persisted
		}
		bb.editedLock.Unlock()
		return dateEdited > persisted
	}
	return true
}

func (bb *blueBubbles) markEditInflight(guid string, dateEdited int64) {
	if guid == "" || dateEdited == 0 {
		return
	}
	bb.editedLock.Lock()
	if bb.inflightEdits == nil {
		bb.inflightEdits = make(map[string][]int64)
	}
	bb.inflightEdits[guid] = append(bb.inflightEdits[guid], dateEdited)
	bb.editedLock.Unlock()
}

func (bb *blueBubbles) markRetractionInflight(guid string) {
	if guid == "" {
		return
	}
	bb.editedLock.Lock()
	if bb.inflightRetractions == nil {
		bb.inflightRetractions = make(map[string]struct{})
	}
	bb.inflightRetractions[guid] = struct{}{}
	bb.editedLock.Unlock()
}

func (bb *blueBubbles) markEdited(guid string, dateEdited int64) {
	if guid == "" || dateEdited == 0 {
		return
	}
	bb.editedLock.Lock()
	if bb.lastEdited == nil {
		bb.lastEdited = make(map[string]int64)
	}
	if prev, ok := bb.lastEdited[guid]; ok && dateEdited <= prev {
		bb.editedLock.Unlock()
		return
	}
	if len(bb.lastEdited) >= maxTrackedEditedMessages {
		bb.lastEdited = make(map[string]int64)
	}
	bb.lastEdited[guid] = dateEdited
	bb.editedLock.Unlock()

	bb.persistEditTS(guid, dateEdited)
}

func (bb *blueBubbles) setPendingEdit(guid, newText string) {
	if guid == "" {
		return
	}
	now := time.Now()
	bb.editedLock.Lock()
	defer bb.editedLock.Unlock()
	if bb.pendingEdits == nil {
		bb.pendingEdits = make(map[string]pendingEdit)
	}
	for k, v := range bb.pendingEdits {
		if now.After(v.expiresAt) {
			delete(bb.pendingEdits, k)
		}
	}
	bb.pendingEdits[guid] = pendingEdit{
		text:      newText,
		expiresAt: now.Add(defaultPendingEditTTL),
	}
}

func (bb *blueBubbles) clearPendingEdit(guid, text string) {
	if guid == "" {
		return
	}
	bb.editedLock.Lock()
	defer bb.editedLock.Unlock()
	if p, ok := bb.pendingEdits[guid]; ok && p.text == text {
		delete(bb.pendingEdits, guid)
	}
}

func (bb *blueBubbles) consumePendingEdit(guid, text string) bool {
	if guid == "" {
		return false
	}
	now := time.Now()
	bb.editedLock.Lock()
	defer bb.editedLock.Unlock()
	p, ok := bb.pendingEdits[guid]
	if !ok {
		return false
	}
	if now.After(p.expiresAt) {
		delete(bb.pendingEdits, guid)
		return false
	}
	if p.text != text {
		return false
	}
	delete(bb.pendingEdits, guid)
	return true
}

func (bb *blueBubbles) setPendingGroupName(chatID, name string) {
	if chatID == "" {
		return
	}
	now := time.Now()
	bb.editedLock.Lock()
	defer bb.editedLock.Unlock()
	if bb.pendingGroupNames == nil {
		bb.pendingGroupNames = make(map[string]pendingGroupName)
	}
	for k, v := range bb.pendingGroupNames {
		if now.After(v.expiresAt) {
			delete(bb.pendingGroupNames, k)
		}
	}
	bb.pendingGroupNames[chatID] = pendingGroupName{
		name:      name,
		expiresAt: now.Add(defaultPendingGroupActionTTL),
	}
}

func (bb *blueBubbles) clearPendingGroupName(chatID, name string) {
	if chatID == "" {
		return
	}
	bb.editedLock.Lock()
	defer bb.editedLock.Unlock()
	if p, ok := bb.pendingGroupNames[chatID]; ok && p.name == name {
		delete(bb.pendingGroupNames, chatID)
	}
}

func (bb *blueBubbles) consumePendingGroupName(chatID, name string) bool {
	if chatID == "" {
		return false
	}
	now := time.Now()
	bb.editedLock.Lock()
	defer bb.editedLock.Unlock()
	p, ok := bb.pendingGroupNames[chatID]
	if !ok {
		return false
	}
	if now.After(p.expiresAt) {
		delete(bb.pendingGroupNames, chatID)
		return false
	}
	if p.name != name {
		return false
	}
	delete(bb.pendingGroupNames, chatID)
	return true
}

func (bb *blueBubbles) handleNewMessage(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleNewMessage")

	var data Message
	err = json.Unmarshal(rawMessage, &data)
	if err != nil {
		bb.log.Warn().Err(err).RawJSON("rawMessage", rawMessage).Str("event", "handleNewMessage").Msg("Failed to parse event data")
		return err
	}

	if imessage.ItemType(data.ItemType) == imessage.ItemTypeName && len(data.Chats) > 0 &&
		bb.consumePendingGroupName(data.Chats[0].GUID, data.GroupTitle) {
		bb.log.Debug().Str("chatGUID", data.Chats[0].GUID).Str("groupTitle", data.GroupTitle).Msg("Suppressing echo of Matrix-initiated group rename")
		return nil
	}

	message, err := bb.convertBBMessageToiMessage(data)

	if err != nil {
		bb.log.Warn().Err(err).RawJSON("rawMessage", rawMessage).Str("event", "handleNewMessage").Msg("Failed to convert message data")
		return err
	}

	select {
	case bb.messageChan <- message:
		if data.DateRetracted != 0 {
			bb.markRetractionInflight(data.GUID)
		} else if data.DateEdited != 0 {
			bb.markEditInflight(data.GUID, data.DateEdited)
		}
	default:
		message.CleanupTempAttachments()
		bb.log.Warn().Msg("Incoming message buffer is full")
	}

	if message.IsEdited || message.IsRetracted {
		return nil // the regular message channel should handle edits and unsends updates
	} else if message.IsRead {
		select {
		case bb.receiptChan <- &imessage.ReadReceipt{
			SenderGUID: message.ChatGUID,
			IsFromMe:   !message.IsFromMe,
			ChatGUID:   message.ChatGUID,
			ReadUpTo:   message.GUID,
			ReadAt:     message.ReadAt,
		}:
		default:
			bb.log.Warn().Msg("Incoming message buffer is full")
		}
	} else if message.IsDelivered {
		select {
		case bb.messageStatusChan <- &imessage.SendMessageStatus{
			GUID:     message.GUID,
			ChatGUID: message.ChatGUID,
			Status:   "delivered",
			Service:  imessage.ParseIdentifier(message.ChatGUID).Service,
		}:
		default:
			bb.log.Warn().Msg("Incoming message buffer is full")
		}
	}

	return nil
}

func (bb *blueBubbles) handleMessageSendError(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleMessageSendError")

	var data Message
	if err = json.Unmarshal(rawMessage, &data); err != nil {
		bb.log.Warn().Err(err).RawJSON("rawMessage", rawMessage).Str("event", "handleMessageSendError").Msg("Failed to parse message send error")
		return err
	}

	if data.GUID == "" || len(data.Chats) == 0 {
		return nil
	}

	chatGUID := data.Chats[0].GUID
	service := data.Handle.Service
	if service == "" {
		service = imessage.ParseIdentifier(chatGUID).Service
	}
	errMsg := "BlueBubbles failed to send message"
	if data.Error != 0 {
		errMsg = fmt.Sprintf("BlueBubbles failed to send message (error code %d)", data.Error)
	}

	select {
	case bb.messageStatusChan <- &imessage.SendMessageStatus{
		GUID:       data.GUID,
		ChatGUID:   chatGUID,
		Status:     "failed",
		Service:    service,
		StatusCode: strconv.Itoa(data.Error),
		Message:    errMsg,
	}:
	default:
		bb.log.Warn().Msg("Incoming message status buffer is full")
	}
	return nil
}

func (bb *blueBubbles) handleMessageUpdated(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleMessageUpdated")

	var data Message
	err = json.Unmarshal(rawMessage, &data)
	if err != nil {
		bb.log.Warn().Err(err).RawJSON("rawMessage", rawMessage).Str("event", "handleMessageUpdated").Msg("Failed to parse event data")
		return err
	}

	isRetracted := data.DateRetracted != 0
	isNewEdit := false
	if data.DateEdited != 0 {
		if bb.consumePendingEdit(data.GUID, data.Text) {
			// Echo of our own Matrix-initiated edit (arrived before HTTP response or without dateEdited in response).
			bb.markEdited(data.GUID, data.DateEdited)
		} else {
			isNewEdit = bb.isNewEdit(data.GUID, data.DateEdited)
		}
	}

	// Only download attachments when forwarding a genuine new edit that may re-upload
	// a single combined attachment+caption; delivery/read/retract updates, suppressed
	// echoes, and caption-only edits do not need media fetched to disk.
	downloadAttachments := isNewEdit && !isRetracted && bb.shouldDownloadEditAttachments(len(data.Attachments))
	message, err := bb.convertBBMessageToiMessageWithOptions(data, downloadAttachments)
	if err != nil {
		bb.log.Warn().Err(err).RawJSON("rawMessage", rawMessage).Str("event", "handleMessageUpdated").Msg("Failed to convert message data")
		return err
	}

	if isNewEdit || message.IsRetracted {
		select {
		case bb.messageChan <- message:
			if message.IsRetracted {
				bb.markRetractionInflight(data.GUID)
			} else if isNewEdit {
				bb.markEditInflight(data.GUID, data.DateEdited)
			}
		default:
			message.CleanupTempAttachments()
			bb.log.Warn().Msg("Incoming message buffer is full")
		}
	}

	if !message.IsRetracted {
		if message.IsRead {
			select {
			case bb.receiptChan <- &imessage.ReadReceipt{
				SenderGUID: message.ChatGUID,
				IsFromMe:   !message.IsFromMe,
				ChatGUID:   message.ChatGUID,
				ReadUpTo:   message.GUID,
				ReadAt:     message.ReadAt,
			}:
			default:
				bb.log.Warn().Msg("Incoming message buffer is full")
			}
		} else if message.IsDelivered {
			select {
			case bb.messageStatusChan <- &imessage.SendMessageStatus{
				GUID:     message.GUID,
				ChatGUID: message.ChatGUID,
				Status:   "delivered",
				Service:  imessage.ParseIdentifier(message.ChatGUID).Service,
			}:
			default:
				bb.log.Warn().Msg("Incoming message buffer is full")
			}
		}
	}

	return nil
}

func (bb *blueBubbles) handleParticipantRemoved(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleParticipantRemoved")

	return bb.chatRoomUpdate(rawMessage, "handleParticipantRemoved")
}

func (bb *blueBubbles) handleParticipantAdded(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleParticipantAdded")

	return bb.chatRoomUpdate(rawMessage, "handleParticipantAdded")
}

func (bb *blueBubbles) handleParticipantLeft(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleParticipantLeft")

	return bb.chatRoomUpdate(rawMessage, "handleParticipantLeft")
}

func (bb *blueBubbles) handleGroupIconChanged(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleGroupIconChanged")
	// BB also fires the new message event where the icon gets updated, NOP here
	return nil
}

func (bb *blueBubbles) handleGroupIconRemoved(rawMessage json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("rawMessage", rawMessage).Msg("handleGroupIconRemoved")
	// BB also fires the new message event where the icon gets removed, NOP here
	return nil
}

func (bb *blueBubbles) handleGroupNameChanged(data json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("data", data).Msg("handleGroupNameChanged")
	// BB also fires the new message event where the name gets updated, NOP here
	return nil
}

func (bb *blueBubbles) chatRoomUpdate(rawMessage json.RawMessage, eventName string) (err error) {

	var data Message
	err = json.Unmarshal(rawMessage, &data)
	if err != nil {
		bb.log.Warn().Err(err).RawJSON("rawMessage", rawMessage).Str("event", eventName).Msg("Failed to parse event data")
		return err
	}

	if len(data.Chats) == 0 {
		bb.log.Error().RawJSON("rawMessage", rawMessage).Str("event", eventName).Msg("Received chat update event without a chat to update")
		return errors.New("chat update event has no chats")
	}

	chat, err := bb.convertBBChatToiMessageChat(data.Chats[0])

	if err != nil {
		bb.log.Warn().Err(err).RawJSON("rawMessage", rawMessage).Str("event", eventName).Msg("Failed to convert chat data")
		return err
	}

	select {
	case bb.chatChan <- chat:
	default:
		bb.log.Warn().Msg("Incoming chat buffer is full")
	}

	return nil
}

func (bb *blueBubbles) handleChatReadStatusChanged(data json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("data", data).Msg("handleChatReadStatusChanged")

	var rec MessageReadResponse
	err = json.Unmarshal(data, &rec)
	if err != nil {
		bb.log.Warn().Err(err).Msg("Failed to parse incoming read receipt")
		return nil
	}

	if !rec.Read || rec.ChatGUID == "" {
		return nil
	}

	chatInfo, err := bb.getChatInfo(rec.ChatGUID)
	if err != nil {
		bb.log.Warn().Err(err).Str("chatID", rec.ChatGUID).Str("event", "handleChatReadStatusChanged").Msg("Failed to fetch chat info")
		return nil
	}

	if chatInfo.Data == nil || chatInfo.Data.LastMessage == nil {
		bb.log.Warn().Msg("Chat info is missing last message")
		return nil
	}

	now := time.Now()

	// chat-read-status-changed fires when the local BlueBubbles user marks a chat read/unread.
	receipt := imessage.ReadReceipt{
		SenderGUID:     rec.ChatGUID,
		IsFromMe:       true,
		ChatGUID:       rec.ChatGUID,
		ReadUpTo:       chatInfo.Data.LastMessage.GUID,
		ReadAt:         now,
		JSONUnixReadAt: timeToFloat(now),
	}

	select {
	case bb.receiptChan <- &receipt:
	default:
		bb.log.Warn().Msg("Incoming receipt buffer is full")
	}
	return nil
}

func (bb *blueBubbles) handleTypingIndicator(data json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("data", data).Msg("handleTypingIndicator")

	var typingNotification TypingNotification
	err = json.Unmarshal(data, &typingNotification)
	if err != nil {
		return err
	}

	notif := imessage.TypingNotification{
		ChatGUID: typingNotification.GUID,
		Typing:   typingNotification.Display,
	}

	select {
	case bb.typingChan <- &notif:
	default:
		bb.log.Warn().Msg("Incoming typing notification buffer is full")
	}
	return nil
}

func (bb *blueBubbles) handleIMessageAliasRemoved(data json.RawMessage) (err error) {
	bb.log.Trace().RawJSON("data", data).Msg("handleIMessageAliasRemoved")
	return ErrNotImplemented
}

// These functions should all be "get" -ting data FROM bluebubbles

var ErrNotImplemented = errors.New("not implemented")

func (bb *blueBubbles) queryChatMessages(query MessageQueryRequest, allResults []Message, paginate bool) ([]Message, error) {
	bb.log.Info().Interface("query", query).Msg("queryChatMessages")

	var resp MessageQueryResponse
	err := bb.apiGet(fmt.Sprintf("/api/v1/chat/%s/message", query.ChatGUID), bb.messageQueryRequestToMap(&query), &resp)
	if err != nil {
		return nil, err
	}

	allResults = append(allResults, resp.Data...)

	nextPageOffset := resp.Metadata.Offset + resp.Metadata.Limit

	// Determine the limit for the next page
	var nextLimit int
	if query.Max != nil && *query.Max > 0 {
		nextLimit = int(math.Min(float64(*query.Max), 1000))
	} else {
		nextLimit = 1000
	}

	// If there are more messages to fetch and pagination is enabled
	if paginate && len(resp.Data) > 0 && resp.Metadata.Limit > 0 && (nextPageOffset < resp.Metadata.Total) {
		// If the next page offset exceeds the maximum limit, adjust the query
		if nextLimit > 0 && nextPageOffset+int64(nextLimit) > resp.Metadata.Total {
			nextLimit = int(resp.Metadata.Total - nextPageOffset)
		}

		// Update the query with the new offset and limit
		query.Offset = int(nextPageOffset)
		query.Limit = nextLimit

		// Recursively call the function for the next page
		return bb.queryChatMessages(query, allResults, paginate)
	}

	return allResults, nil
}

func (bb *blueBubbles) messageQueryRequestToMap(req *MessageQueryRequest) map[string]string {
	m := make(map[string]string)

	m["limit"] = fmt.Sprintf("%d", req.Limit)
	m["offset"] = fmt.Sprintf("%d", req.Offset)
	m["sort"] = string(req.Sort)

	if req.Before != nil {
		m["before"] = fmt.Sprintf("%d", *req.Before)
	}

	if req.After != nil {
		m["after"] = fmt.Sprintf("%d", *req.After)
	}

	// Handling slice of MessageQueryWith
	if len(req.With) > 0 {
		with := ""
		for index, withItem := range req.With {
			if index == 0 {
				with = string(withItem)
			} else {
				with = with + "," + string(withItem)
			}
		}
		m["with"] = with
	}

	return m
}

func defaultMessageQueryWith() []MessageQueryWith {
	return []MessageQueryWith{
		MessageQueryWithChat,
		MessageQueryWithChatParticipants,
		MessageQueryWithAttachment,
		MessageQueryWithHandle,
		MessageQueryWithAttributeBody,
		MessageQueryWithMessageSummary,
		MessageQueryWithPayloadData,
	}
}

func (bb *blueBubbles) convertChatQueryMessages(chatID string, messages []Message) []*imessage.Message {
	resp := make([]*imessage.Message, 0, len(messages))
	for _, msg := range messages {
		if len(msg.Chats) == 0 {
			msg.Chats = []Chat{{GUID: chatID}}
		}
		converted, err := bb.convertBBMessageToiMessage(msg)
		if err != nil {
			bb.log.Warn().Err(err).Str("guid", msg.GUID).Msg("Failed to convert message from BlueBubbles format to Matrix format")
			continue
		}
		resp = append(resp, converted)
	}
	return reverseList(resp)
}

func (bb *blueBubbles) GetMessagesSinceDate(chatID string, minDate time.Time, backfillID string) (resp []*imessage.Message, err error) {
	bb.log.Trace().Str("chatID", chatID).Time("minDate", minDate).Str("backfillID", backfillID).Msg("GetMessagesSinceDate")

	after := minDate.UnixNano() / int64(time.Millisecond)
	request := MessageQueryRequest{
		ChatGUID: chatID,
		Limit:    100,
		Offset:   0,
		With:     defaultMessageQueryWith(),
		After:    &after,
		Sort:     MessageQuerySortDesc,
	}

	messages, err := bb.queryChatMessages(request, []Message{}, true)
	if err != nil {
		bb.log.Error().Err(err).Interface("request", request).Str("chatID", chatID).Time("minDate", minDate).Str("backfillID", backfillID).Str("search", "GetMessagesSinceDate").Msg("Failed to query chat Messages")
		return nil, err
	}

	return bb.convertChatQueryMessages(chatID, messages), nil
}

func (bb *blueBubbles) GetMessagesBetween(chatID string, minDate, maxDate time.Time) (resp []*imessage.Message, err error) {
	bb.log.Trace().Str("chatID", chatID).Time("minDate", minDate).Time("maxDate", maxDate).Msg("GetMessagesBetween")

	after := minDate.UnixNano() / int64(time.Millisecond)
	before := maxDate.UnixNano() / int64(time.Millisecond)
	request := MessageQueryRequest{
		ChatGUID: chatID,
		Limit:    100,
		Offset:   0,
		With:     defaultMessageQueryWith(),
		After:    &after,
		Before:   &before,
		Sort:     MessageQuerySortDesc,
	}

	messages, err := bb.queryChatMessages(request, []Message{}, true)
	if err != nil {
		bb.log.Error().Err(err).Interface("request", request).Str("chatID", chatID).Time("minDate", minDate).Time("maxDate", maxDate).Str("search", "GetMessagesBetween").Msg("Failed to query chat Messages")
		return nil, err
	}

	return bb.convertChatQueryMessages(chatID, messages), nil
}

func (bb *blueBubbles) GetMessagesBeforeWithLimit(chatID string, before time.Time, limit int) (resp []*imessage.Message, err error) {
	bb.log.Trace().Str("chatID", chatID).Time("before", before).Int("limit", limit).Msg("GetMessagesBeforeWithLimit")

	_before := before.UnixNano() / int64(time.Millisecond)

	queryLimit := limit
	if queryLimit > 1000 {
		queryLimit = 1000
	}

	request := MessageQueryRequest{
		ChatGUID: chatID,
		Limit:    queryLimit,
		Max:      &limit,
		Offset:   0,
		With:     defaultMessageQueryWith(),
		Before:   &_before,
		Sort:     MessageQuerySortDesc,
	}

	messages, err := bb.queryChatMessages(request, []Message{}, false)
	if err != nil {
		bb.log.Error().Err(err).Interface("request", request).Str("chatID", chatID).Time("before", before).Int("limit", limit).Str("search", "GetMessagesBeforeWithLimit").Msg("Failed to query chat Messages")
		return nil, err
	}

	return bb.convertChatQueryMessages(chatID, messages), nil
}

func (bb *blueBubbles) GetMessagesWithLimit(chatID string, limit int, backfillID string) (resp []*imessage.Message, err error) {
	bb.log.Trace().Str("chatID", chatID).Int("limit", limit).Str("backfillID", backfillID).Msg("GetMessagesWithLimit")

	queryLimit := limit
	if queryLimit > 1000 {
		queryLimit = 1000
	}

	request := MessageQueryRequest{
		ChatGUID: chatID,
		Limit:    queryLimit,
		Max:      &limit,
		Offset:   0,
		With:     defaultMessageQueryWith(),
		Sort:     MessageQuerySortDesc,
	}

	messages, err := bb.queryChatMessages(request, []Message{}, false)
	if err != nil {
		bb.log.Error().Err(err).Interface("request", request).Str("chatID", chatID).Int("limit", limit).Str("backfillID", backfillID).Str("search", "GetMessagesWithLimit").Msg("Failed to query chat Messages")
		return nil, err
	}

	return bb.convertChatQueryMessages(chatID, messages), nil
}

func (bb *blueBubbles) getMessage(guid string) (*Message, error) {
	bb.log.Trace().Str("guid", guid).Msg("getMessage")

	var messageResponse MessageResponse

	err := bb.apiGet(fmt.Sprintf("/api/v1/message/%s", guid), map[string]string{
		"with": "chats",
	}, &messageResponse)
	if err != nil {
		return nil, err
	}

	return &messageResponse.Data, nil
}

func (bb *blueBubbles) getMessageWithDetails(guid string) (*Message, error) {
	bb.log.Trace().Str("guid", guid).Msg("getMessageWithDetails")

	var messageResponse MessageResponse
	err := bb.apiGet(fmt.Sprintf("/api/v1/message/%s", guid), map[string]string{
		"with": "chats,attachment,handle,message.attributedBody,message.messageSummaryInfo,message.payloadData",
	}, &messageResponse)
	if err != nil {
		return nil, err
	}

	return &messageResponse.Data, nil
}

func (bb *blueBubbles) GetMessage(guid string) (resp *imessage.Message, err error) {
	bb.log.Trace().Str("guid", guid).Msg("GetMessage")

	message, err := bb.getMessage(guid)

	if err != nil {
		bb.log.Err(err).Str("guid", guid).Any("response", message).Msg("Failed to get a message from BlueBubbles")

		return nil, err
	}

	resp, err = bb.convertBBMessageToiMessage(*message)

	if err != nil {
		bb.log.Err(err).Str("guid", guid).Any("response", message).Msg("Failed to parse a message from BlueBubbles")

		return nil, err
	}

	return resp, nil
}

func (bb *blueBubbles) queryChats(query ChatQueryRequest, allResults []Chat) ([]Chat, error) {
	return bb.queryChatsWithStop(query, allResults, 0)
}

func (bb *blueBubbles) queryChatsWithStop(query ChatQueryRequest, allResults []Chat, stopBeforeCreatedMs int64) ([]Chat, error) {
	bb.log.Debug().Interface("query", query).Msg("queryChats")

	var resp ChatQueryResponse
	err := bb.apiPost("/api/v1/chat/query", query, &resp)
	if err != nil {
		return nil, err
	}

	allResults = append(allResults, resp.Data...)

	if len(resp.Data) == 0 || resp.Metadata.Limit <= 0 {
		return allResults, nil
	}

	if stopBeforeCreatedMs > 0 && query.Sort == QuerySortLastMessage {
		lastChat := resp.Data[len(resp.Data)-1]
		if lastChat.LastMessage == nil || lastChat.LastMessage.DateCreated < stopBeforeCreatedMs {
			return allResults, nil
		}
	}

	nextPageOffset := resp.Metadata.Offset + resp.Metadata.Limit
	if nextPageOffset < resp.Metadata.Total {
		query.Offset = nextPageOffset
		query.Limit = resp.Metadata.Limit
		return bb.queryChatsWithStop(query, allResults, stopBeforeCreatedMs)
	}

	return allResults, nil
}

func (bb *blueBubbles) GetChatsWithMessagesAfter(minDate time.Time) (resp []imessage.ChatIdentifier, err error) {
	bb.log.Trace().Time("minDate", minDate).Msg("GetChatsWithMessagesAfter")

	request := ChatQueryRequest{
		Limit:  1000,
		Offset: 0,
		With: []ChatQueryWith{
			ChatQueryWithLastMessage,
			ChatQueryWithSMS,
		},
		Sort: QuerySortLastMessage,
	}

	chats, err := bb.queryChatsWithStop(request, []Chat{}, minDate.UnixMilli())
	if err != nil {
		bb.log.Error().Err(err).Time("minDate", minDate).Str("search", "GetChatsWithMessagesAfter").Msg("Failed to search for chats")
		return nil, err
	}

	for _, chat := range chats {
		if chat.LastMessage == nil {
			continue
		}
		if (chat.LastMessage.DateCreated / 1000) < minDate.Unix() {
			continue
		}
		resp = append(resp, imessage.ChatIdentifier{
			ChatGUID: chat.GUID,
			ThreadID: chat.GroupID,
		})
	}

	return resp, nil
}

func (bb *blueBubbles) matchHandleToContact(address string) *Contact {
	bb.contactsLock.RLock()
	defer bb.contactsLock.RUnlock()

	var matchedContact *Contact
	isEmail := strings.Contains(address, "@")
	numericAddress := numericOnly(address)

	for _, c := range bb.contacts {
		var contact *Contact

		if isEmail {
			for _, e := range c.Emails {
				if strings.EqualFold(strings.TrimSpace(e.Address), strings.TrimSpace(address)) {
					copied := c
					contact = &copied
					break
				}
			}
		} else if numericAddress != "" {
			for _, p := range c.PhoneNumbers {
				numericPhone := numericOnly(p.Address)
				if numericPhone == "" {
					continue
				}
				if numericPhone == numericAddress {
					copied := c
					contact = &copied
					break
				}
				if len(numericPhone) >= 7 && len(numericPhone) <= 15 && strings.HasSuffix(numericAddress, numericPhone) {
					copied := c
					contact = &copied
					break
				}
				// Require at least 10 digits on the incoming address for reverse suffix matching
				// so SMS short codes (e.g. 262966) don't falsely match contacts' phone numbers.
				if len(numericAddress) >= 10 && len(numericAddress) <= 15 && strings.HasSuffix(numericPhone, numericAddress) {
					copied := c
					contact = &copied
					break
				}
			}
		}

		// Contacts with a source type of "db" were imported into BB and preferable
		if contact != nil && contact.SourceType == "db" {
			return contact
		}

		// Contacts with a source type of "api" are stored on the mac and can be used as fallback in case an imported one isn't found
		if contact != nil && matchedContact == nil {
			copied := *contact
			matchedContact = &copied
		}
	}

	return matchedContact
}

func (bb *blueBubbles) SearchContactList(input string) ([]*imessage.Contact, error) {
	bb.log.Trace().Str("input", input).Msg("SearchContactList")

	query := strings.ToLower(strings.TrimSpace(input))
	if query == "" {
		return nil, nil
	}

	bb.contactsLock.RLock()
	defer bb.contactsLock.RUnlock()

	var matchedContacts []*imessage.Contact

	for i := range bb.contacts {
		contact := bb.contacts[i]
		contactFields := []string{
			strings.ToLower(contact.FirstName + " " + contact.LastName),
			strings.ToLower(contact.DisplayName),
			strings.ToLower(contact.Nickname),
		}

		for _, phoneNumber := range contact.PhoneNumbers {
			contactFields = append(contactFields, strings.ToLower(phoneNumber.Address))
		}

		for _, email := range contact.Emails {
			contactFields = append(contactFields, strings.ToLower(email.Address))
		}

		matches := fuzzy.Find(query, contactFields)

		if len(matches) > 0 {
			imessageContact, _ := bb.convertBBContactToiMessageContact(&contact)
			matchedContacts = append(matchedContacts, imessageContact)
		}
	}

	return matchedContacts, nil
}

func (bb *blueBubbles) GetContactInfo(identifier string) (resp *imessage.Contact, err error) {
	bb.log.Trace().Str("identifier", identifier).Msg("GetContactInfo")

	contact := bb.matchHandleToContact(identifier)

	bb.log.Trace().Str("identifier", identifier).Interface("contact", contact).Msg("GetContactInfo: found a contact")

	if contact != nil {
		resp, _ = bb.convertBBContactToiMessageContact(contact)
		return resp, nil
	}
	err = errors.New("no contacts found for address")
	bb.log.Debug().Err(err).Str("identifier", identifier).Msg("No contacts matched address, aborting contact retrieval")
	return nil, err
}

func (bb *blueBubbles) GetContactList() (resp []*imessage.Contact, err error) {
	bb.log.Trace().Msg("GetContactList")

	bb.contactsLock.RLock()
	defer bb.contactsLock.RUnlock()

	resp = make([]*imessage.Contact, 0, len(bb.contacts))
	for i := range bb.contacts {
		contact := bb.contacts[i]
		imessageContact, _ := bb.convertBBContactToiMessageContact(&contact)
		resp = append(resp, imessageContact)
	}

	return resp, nil
}

func (bb *blueBubbles) RefreshContactList() error {
	bb.log.Trace().Msg("refreshContactsList")

	var contactResponse ContactResponse

	err := bb.apiGet("/api/v1/contact", map[string]string{
		"extraProperties": "avatar",
	}, &contactResponse)
	if err != nil {
		return err
	}

	bb.contactsLock.Lock()
	bb.contacts = contactResponse.Data
	bb.contactsLastRefresh = time.Now()
	bb.contactsLock.Unlock()

	return nil
}

func (bb *blueBubbles) getChatInfo(chatID string) (*ChatResponse, error) {
	bb.log.Trace().Str("chatID", chatID).Msg("getChatInfo")

	var chatResponse ChatResponse

	// DEVNOTE: it doesn't appear we should URL Encode the chatID... 😬
	//          the BlueBubbles API returned 404s, sometimes, with URL encoding
	err := bb.apiGet(fmt.Sprintf("/api/v1/chat/%s", chatID), map[string]string{
		"with": "participants,lastMessage",
	}, &chatResponse)
	if err != nil {
		return nil, err
	}

	if chatResponse.Data == nil {
		return nil, errors.New("chat is missing data payload")
	}

	return &chatResponse, nil
}

func (bb *blueBubbles) GetChatInfo(chatID, threadID string) (*imessage.ChatInfo, error) {
	bb.log.Trace().Str("chatID", chatID).Str("threadID", threadID).Msg("GetChatInfo")

	chatResponse, err := bb.getChatInfo(chatID)
	if err != nil {
		bb.log.Error().Err(err).Str("chatID", chatID).Str("threadID", threadID).Msg("Failed to fetch chat info")
		return nil, err
	}

	chatInfo, err := bb.convertBBChatToiMessageChat(*chatResponse.Data)
	if err != nil {
		bb.log.Error().Err(err).Str("chatID", chatID).Str("threadID", threadID).Msg("Failed to convert chat info")
		return nil, err
	}

	return chatInfo, nil
}

func (bb *blueBubbles) GetGroupAvatar(chatID string) (*imessage.Attachment, error) {
	bb.log.Trace().Str("chatID", chatID).Msg("GetGroupAvatar")

	chatResponse, err := bb.getChatInfo(chatID)
	if err != nil {
		bb.log.Error().Err(err).Str("chatID", chatID).Msg("Failed to fetch chat info")
		return nil, err
	}

	if chatResponse.Data.Properties == nil ||
		len(chatResponse.Data.Properties) < 1 {
		return nil, nil
	}

	properties := chatResponse.Data.Properties[0]

	if properties.GroupPhotoGUID == nil {
		return nil, nil
	}

	attachment, err := bb.downloadAttachment(*properties.GroupPhotoGUID)
	if err != nil {
		bb.log.Error().Err(err).Str("chatID", chatID).Msg("Failed to download group avatar")
		return nil, err
	}

	return attachment, nil
}

// These functions all provide "channels" to allow concurrent processing in the bridge
func (bb *blueBubbles) MessageChan() <-chan *imessage.Message {
	return bb.messageChan
}

func (bb *blueBubbles) ReadReceiptChan() <-chan *imessage.ReadReceipt {
	return bb.receiptChan
}

func (bb *blueBubbles) TypingNotificationChan() <-chan *imessage.TypingNotification {
	return bb.typingChan
}

func (bb *blueBubbles) ChatChan() <-chan *imessage.ChatInfo {
	return bb.chatChan
}

func (bb *blueBubbles) ContactChan() <-chan *imessage.Contact {
	return bb.contactChan
}

func (bb *blueBubbles) MessageStatusChan() <-chan *imessage.SendMessageStatus {
	return bb.messageStatusChan
}

func (bb *blueBubbles) BackfillTaskChan() <-chan *imessage.BackfillTask {
	return bb.backfillTaskChan
}

func buildSendResponse(chatID string, msg Message) *imessage.SendResponse {
	service := msg.Handle.Service
	if service == "" {
		service = imessage.ParseIdentifier(chatID).Service
	}
	respChatGUID := chatID
	if len(msg.Chats) > 0 && msg.Chats[0].GUID != "" {
		respChatGUID = msg.Chats[0].GUID
	}
	return &imessage.SendResponse{
		GUID:     msg.GUID,
		Service:  service,
		ChatGUID: respChatGUID,
		Time:     time.UnixMilli(msg.DateCreated),
	}
}

func (bb *blueBubbles) SendMessage(chatID, text string, replyTo string, replyToPart int, richLink *imessage.RichLink, metadata imessage.MessageMetadata) (*imessage.SendResponse, error) {
	bb.log.Trace().Str("chatID", chatID).Str("text", text).Str("replyTo", replyTo).Int("replyToPart", replyToPart).Any("richLink", richLink).Interface("metadata", metadata).Msg("SendMessage")

	privateAPI := bb.usingPrivateAPI.Load()
	var method string
	if privateAPI {
		method = "private-api"
	} else {
		// we have to use apple-script and send a second message
		method = "apple-script"
	}

	request := SendTextRequest{
		ChatGUID: chatID,
		Method:   method,
		Message:  text,
		TempGUID: fmt.Sprintf("temp-%s", RandString(8)),
	}
	if replyTo != "" {
		request.SelectedMessageGUID = replyTo
		request.PartIndex = replyToPart
	}

	var res SendTextResponse

	err := bb.apiSendPost("/api/v1/message/text", request, &res)
	if err != nil {
		bb.log.Error().Err(err).Any("response", res).Msg("Failure when sending message to BlueBubbles")
		return nil, err
	}

	if res.Status != 200 {
		bb.log.Error().Int64("statusCode", res.Status).Any("response", res).Msg("Failure when sending message to BlueBubbles")
		return nil, fmt.Errorf("could not send message (status %d: %s)", res.Status, res.Message)
	}

	return buildSendResponse(chatID, res.Data), nil
}

func (bb *blueBubbles) UnsendMessage(chatID, targetGUID string, targetPart int) (*imessage.SendResponse, error) {
	bb.log.Trace().Str("chatID", chatID).Str("targetGUID", targetGUID).Int("targetPart", targetPart).Msg("UnsendMessage")

	if !bb.usingPrivateAPI.Load() {
		bb.log.Warn().Str("chatID", chatID).Str("targetGUID", targetGUID).Int("targetPart", targetPart).Msg("The private-api isn't enabled in BlueBubbles, can't unsend message")
		return nil, errors.ErrUnsupported
	}

	request := UnsendMessage{
		PartIndex: targetPart,
	}

	var res UnsendMessageResponse

	err := bb.apiSendPost("/api/v1/message/"+targetGUID+"/unsend", request, &res)
	if err != nil {
		bb.log.Error().Err(err).Any("response", res).Msg("Failure when unsending message in BlueBubbles")
		return nil, err
	}

	if res.Status != 200 {
		bb.log.Error().Int64("statusCode", res.Status).Any("response", res).Msg("Failure when unsending message in BlueBubbles")
		return nil, fmt.Errorf("could not unsend message (status %d: %s)", res.Status, res.Message)
	}

	return buildSendResponse(chatID, res.Data), nil
}

func (bb *blueBubbles) EditMessage(chatID string, targetGUID string, newText string, targetPart int) (*imessage.SendResponse, error) {
	bb.log.Trace().Str("chatID", chatID).Str("targetGUID", targetGUID).Str("newText", newText).Int("targetPart", targetPart).Msg("EditMessage")

	if !bb.usingPrivateAPI.Load() {
		bb.log.Warn().Str("chatID", chatID).Str("targetGUID", targetGUID).Str("newText", newText).Int("targetPart", targetPart).Msg("The private-api isn't enabled in BlueBubbles, can't edit message")
		return nil, errors.ErrUnsupported
	}

	request := EditMessage{
		EditedMessage:                 newText,
		BackwardsCompatibilityMessage: "Edited to \"" + newText + "\"",
		PartIndex:                     targetPart,
	}

	// Record a pending own edit before the POST so an updated-message echo that arrives
	// before the HTTP response (or when the response omits dateEdited) is still suppressed.
	bb.setPendingEdit(targetGUID, newText)

	var res EditMessageResponse

	err := bb.apiSendPost("/api/v1/message/"+targetGUID+"/edit", request, &res)
	if err != nil {
		bb.clearPendingEdit(targetGUID, newText)
		bb.log.Error().Err(err).Any("response", res).Msg("Failure when editing message in BlueBubbles")
		return nil, err
	}

	if res.Status != 200 {
		bb.clearPendingEdit(targetGUID, newText)
		bb.log.Error().Int64("statusCode", res.Status).Any("response", res).Msg("Failure when editing message in BlueBubbles")
		return nil, fmt.Errorf("could not edit message (status %d: %s)", res.Status, res.Message)
	}

	// Mark this edit timestamp as already processed (if returned) so BlueBubbles's
	// updated-message echo does not bridge our own Matrix edit back as a duplicate m.replace.
	if res.Data.DateEdited != 0 {
		bb.markEdited(targetGUID, res.Data.DateEdited)
		if res.Data.GUID != "" && res.Data.GUID != targetGUID {
			bb.markEdited(res.Data.GUID, res.Data.DateEdited)
		}
		bb.clearPendingEdit(targetGUID, newText)
	}

	return buildSendResponse(chatID, res.Data), nil
}

func (bb *blueBubbles) apiProbeGet(path string, queryParams map[string]string, target interface{}) error {
	reqURL, err := bb.apiURL(path, queryParams)
	if err != nil {
		return bb.sanitizeError(err)
	}
	req, err := http.NewRequestWithContext(bb.context(), http.MethodGet, reqURL, nil)
	if err != nil {
		return bb.sanitizeError(err)
	}
	response, err := bb.getHTTPClient(clientProbe).Do(req)
	if err != nil {
		return bb.sanitizeError(err)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return bb.sanitizeError(err)
	}
	if err := checkHTTPStatus(http.MethodGet, path, response, responseBody); err != nil {
		return bb.sanitizeError(err)
	}
	if target != nil {
		if err := json.Unmarshal(responseBody, target); err != nil {
			return err
		}
	}
	return nil
}

func (bb *blueBubbles) fetchServerInfo() (ServerInfo, error) {
	var serverInfo ServerInfoResponse
	if err := bb.apiProbeGet("/api/v1/server/info", nil, &serverInfo); err != nil {
		return ServerInfo{}, err
	}
	return serverInfo.Data, nil
}

func (bb *blueBubbles) SendFile(chatID, text, filename string, pathOnDisk string, replyTo string, replyToPart int, mimeType string, voiceMemo bool, metadata imessage.MessageMetadata) (*imessage.SendResponse, error) {
	bb.log.Trace().Str("chatID", chatID).Str("text", text).Str("filename", filename).Str("pathOnDisk", pathOnDisk).Str("replyTo", replyTo).Int("replyToPart", replyToPart).Str("mimeType", mimeType).Bool("voiceMemo", voiceMemo).Interface("metadata", metadata).Msg("SendFile")

	attachment, err := os.ReadFile(pathOnDisk)
	if err != nil {
		return nil, err
	}

	if filename == "" {
		filename = filepath.Base(pathOnDisk)
	}
	if filename == "" || filename == "." || filename == "/" {
		filename = "attachment"
	}

	bb.log.Info().Int("attachmentSize", len(attachment)).Str("filename", filename).Msg("Read attachment from disk")

	// Snapshot Private API mode once so method, subject, and fallback caption stay consistent.
	privateAPI := bb.usingPrivateAPI.Load()
	var method string
	if privateAPI {
		method = "private-api"
	} else {
		// we have to use apple-script and send a second message if there's a caption
		method = "apple-script"
	}

	formData := map[string]interface{}{
		"chatGuid":       chatID,
		"tempGuid":       fmt.Sprintf("temp-%s", RandString(8)),
		"name":           filename,
		"method":         method,
		"attachment":     formFile{Filename: filename, Data: attachment},
		"isAudioMessage": voiceMemo,
	}

	if replyTo != "" {
		formData["selectedMessageGuid"] = replyTo
		formData["partIndex"] = replyToPart
	}

	if privateAPI && text != "" {
		formData["subject"] = text
	}

	path := "/api/v1/message/attachment"

	var response SendTextResponse
	if err := bb.apiPostAsFormData(path, formData, &response); err != nil {
		return nil, err
	}

	if response.Status != 200 {
		bb.log.Error().Int64("statusCode", response.Status).Any("response", response).Msg("Failure when sending attachment to BlueBubbles")
		return nil, fmt.Errorf("could not send attachment (status %d: %s)", response.Status, response.Message)
	}

	if !privateAPI && text != "" {
		if _, err := bb.SendMessage(chatID, text, replyTo, replyToPart, nil, nil); err != nil {
			bb.log.Warn().Err(err).Str("chatID", chatID).Msg("Failed to send caption text after attachment")
		}
	}

	return buildSendResponse(chatID, response.Data), nil
}

func (bb *blueBubbles) SendFileCleanup(sendFileDir string) {
	_ = os.RemoveAll(sendFileDir)
}

func (bb *blueBubbles) sendReaction(chatID, targetGUID string, targetPart int, reaction string, remove bool) (*imessage.SendResponse, error) {
	if !bb.usingPrivateAPI.Load() {
		bb.log.Warn().Str("chatID", chatID).Str("targetGUID", targetGUID).Str("reaction", reaction).Bool("remove", remove).Msg("The private-api isn't enabled in BlueBubbles, can't send tapback")
		return nil, errors.ErrUnsupported
	}

	if reaction == "" {
		return nil, errors.New("empty tapback reaction")
	}

	if remove && !strings.HasPrefix(reaction, "-") {
		reaction = "-" + reaction
	}

	request := SendReactionRequest{
		ChatGUID:            chatID,
		SelectedMessageGUID: targetGUID,
		PartIndex:           targetPart,
		Reaction:            reaction,
	}

	var res SendReactionResponse

	err := bb.apiSendPost("/api/v1/message/react", request, &res)
	if err != nil {
		return nil, err
	}

	if res.Status != 200 {
		bb.log.Error().Any("response", res).Msg("Failure when sending reaction to BlueBubbles")
		return nil, fmt.Errorf("could not send reaction (status %d: %s)", res.Status, res.Message)
	}

	return buildSendResponse(chatID, res.Data), nil
}

func (bb *blueBubbles) SendTapback(chatID, targetGUID string, targetPart int, tapback imessage.TapbackType, remove bool) (*imessage.SendResponse, error) {
	bb.log.Trace().Str("chatID", chatID).Str("targetGUID", targetGUID).Int("targetPart", targetPart).Interface("tapback", tapback).Bool("remove", remove).Msg("SendTapback")
	return bb.sendReaction(chatID, targetGUID, targetPart, tapback.Name(), remove)
}

func (bb *blueBubbles) SendCustomEmojiTapback(chatID, targetGUID string, targetPart int, emoji string, remove bool) (*imessage.SendResponse, error) {
	bb.log.Trace().Str("chatID", chatID).Str("targetGUID", targetGUID).Int("targetPart", targetPart).Str("emoji", emoji).Bool("remove", remove).Msg("SendCustomEmojiTapback")
	if !bb.supportsCustomEmojiTapbacks.Load() {
		return nil, fmt.Errorf("%w: BlueBubbles server /api/v1/message/react does not support custom emoji reactions", errors.ErrUnsupported)
	}
	return bb.sendReaction(chatID, targetGUID, targetPart, emoji, remove)
}

func (bb *blueBubbles) SendReadReceipt(chatID, readUpTo string) error {
	bb.log.Trace().Str("chatID", chatID).Str("readUpTo", readUpTo).Msg("SendReadReceipt")

	if !bb.usingPrivateAPI.Load() {
		bb.log.Warn().Str("chatID", chatID).Msg("The private-api isn't enabled in BlueBubbles, can't send read receipt")
		return errors.ErrUnsupported
	}

	var res ReadReceiptResponse
	err := bb.apiPost(fmt.Sprintf("/api/v1/chat/%s/read", chatID), nil, &res)

	if err != nil {
		return err
	}

	if res.Status != 200 {
		bb.log.Error().Any("response", res).Str("chatID", chatID).Msg("Failure when marking a chat as read")

		return errors.New("could not mark chat as read")
	}

	bb.log.Trace().Str("chatID", chatID).Msg("Marked a chat as Read")

	return nil
}

func (bb *blueBubbles) SendTypingNotification(chatID string, typing bool) error {
	bb.log.Trace().Str("chatID", chatID).Bool("typing", typing).Msg("SendTypingNotification")

	if !bb.usingPrivateAPI.Load() {
		bb.log.Warn().Str("chatID", chatID).Bool("typing", typing).Msg("The private-api isn't enabled in BlueBubbles, can't send typing notification")
		return errors.ErrUnsupported
	}

	var res TypingResponse
	var err error

	if typing {
		err = bb.apiPost(fmt.Sprintf("/api/v1/chat/%s/typing", chatID), nil, &res)
	} else {
		err = bb.apiDelete(fmt.Sprintf("/api/v1/chat/%s/typing", chatID), nil, &res)
	}

	if err != nil {
		return err
	}

	if res.Status != 200 {
		bb.log.Error().Any("response", res).Str("chatID", chatID).Bool("typing", typing).Msg("Failure when updating typing status")

		return errors.New("could not update typing status")
	}

	bb.log.Trace().Str("chatID", chatID).Bool("typing", typing).Msg("Update typing status")

	return nil
}

func (bb *blueBubbles) setResolvedHandleService(address, service string) {
	norm := normalizeParticipantAddress(address)
	if norm == "" || service == "" || service == "any" {
		return
	}
	bb.serviceCacheLock.Lock()
	if bb.resolvedHandleServices == nil {
		bb.resolvedHandleServices = make(map[string]string)
	}
	bb.resolvedHandleServices[norm] = service
	bb.serviceCacheLock.Unlock()
}

func (bb *blueBubbles) getResolvedHandleService(address string) string {
	norm := normalizeParticipantAddress(address)
	if norm == "" {
		return ""
	}
	bb.serviceCacheLock.RLock()
	svc := bb.resolvedHandleServices[norm]
	bb.serviceCacheLock.RUnlock()
	return svc
}

func (bb *blueBubbles) setChatService(chatGUID, service string) {
	if chatGUID == "" || service == "" || service == "any" {
		return
	}
	bb.serviceCacheLock.Lock()
	if bb.chatServices == nil {
		bb.chatServices = make(map[string]string)
	}
	bb.chatServices[chatGUID] = service
	bb.serviceCacheLock.Unlock()
}

func (bb *blueBubbles) getChatService(chatGUID string) string {
	if chatGUID == "" {
		return ""
	}
	bb.serviceCacheLock.RLock()
	svc := bb.chatServices[chatGUID]
	bb.serviceCacheLock.RUnlock()
	return svc
}

func (bb *blueBubbles) lookupHandleService(address string) string {
	norm := normalizeParticipantAddress(address)
	if norm == "" {
		return ""
	}
	var resp ResolveIdentifierResponse
	if err := bb.apiProbeGet(fmt.Sprintf("/api/v1/handle/%s", norm), nil, &resp); err != nil {
		return ""
	}
	if resp.Data.Service != "" && resp.Data.Service != "any" {
		bb.setResolvedHandleService(norm, resp.Data.Service)
		return resp.Data.Service
	}
	return ""
}

func (bb *blueBubbles) ResolveIdentifier(address string) (string, error) {
	bb.log.Trace().Str("address", address).Msg("ResolveIdentifier")

	var identifierResponse ResolveIdentifierResponse

	var handle = address
	if !strings.Contains(address, "@") {
		handle = "+" + numericOnly(address)
	}

	err := bb.apiGet(fmt.Sprintf("/api/v1/handle/%s", handle), nil, &identifierResponse)
	if err != nil {
		var statusErr *HTTPStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
			// BlueBubbles only records handles in chat.db after at least one message has been
			// exchanged, so first-contact DMs return HTTP 404. Reuse any cached service first,
			// otherwise default to iMessage so starting a brand-new DM is possible.
			if cachedSvc := bb.getResolvedHandleService(handle); cachedSvc != "" {
				return cachedSvc + ";-;" + handle, nil
			}
			bb.log.Debug().Str("address", address).Str("handle", handle).Msg("Handle not found in BlueBubbles (404); assuming 'iMessage' service")
			return "iMessage;-;" + handle, nil
		}
		bb.log.Error().Err(err).Any("response", identifierResponse).Str("address", address).Str("handle", handle).Msg("Failure when Resolving Identifier")
		return "", err
	}

	if identifierResponse.Data.Service == "" || identifierResponse.Data.Address == "" {
		bb.log.Warn().Any("response", identifierResponse).Str("address", address).Msg("No results found for provided identifier. Assuming 'iMessage' service.")
		return "iMessage;-;" + handle, nil
	}

	bb.setResolvedHandleService(identifierResponse.Data.Address, identifierResponse.Data.Service)

	// On macOS Sequoia+, chat.db stores many DM chats under the "any;-;" prefix
	// regardless of whether the handle resolves to iMessage, SMS, or RCS.
	// Reuse an existing "any;-;" chat when present to avoid splitting portals.
	if anyGUID := bb.lookupExistingAnyChat(identifierResponse.Data.Address); anyGUID != "" {
		return anyGUID, nil
	}

	return identifierResponse.Data.Service + ";-;" + identifierResponse.Data.Address, nil
}

func (bb *blueBubbles) lookupExistingAnyChat(address string) string {
	if address == "" {
		return ""
	}
	candidate := "any;-;" + address
	var chatResp ChatResponse
	if err := bb.apiProbeGet(fmt.Sprintf("/api/v1/chat/%s", candidate), nil, &chatResp); err != nil {
		return ""
	}
	if chatResp.Data != nil && chatResp.Data.GUID != "" {
		return chatResp.Data.GUID
	}
	return ""
}

func (bb *blueBubbles) PrepareDM(guid string) error {
	bb.log.Trace().Str("guid", guid).Msg("PrepareDM")
	return nil
}

func (bb *blueBubbles) CreateGroup(users []string) (*imessage.CreateGroupResponse, error) {
	bb.log.Trace().Interface("users", users).Msg("CreateGroup")
	return nil, fmt.Errorf("%w: BlueBubbles requires a non-empty initial message to create a group chat", errors.ErrUnsupported)
}

func (bb *blueBubbles) CreateGroupWithMessage(users []string, initialMessage string) (*imessage.CreateGroupResponse, error) {
	bb.log.Trace().Interface("users", users).Msg("CreateGroupWithMessage")

	if !bb.usingPrivateAPI.Load() {
		bb.log.Warn().Interface("users", users).Msg("The private-api isn't enabled in BlueBubbles, can't create group")
		return nil, errors.ErrUnsupported
	}
	if len(users) == 0 {
		return nil, errors.New("at least one participant is required to create a group")
	}
	trimmedMsg := strings.TrimSpace(initialMessage)
	if trimmedMsg == "" {
		return nil, errors.New("initial message is required to create a group chat in BlueBubbles")
	}

	service := "iMessage"
	addresses := make([]string, 0, len(users))
	for _, user := range users {
		var addr string
		var explicitService string
		if strings.Contains(user, ";") {
			parsed := imessage.ParseIdentifier(user)
			explicitService = parsed.Service
			addr = parsed.LocalID
		} else {
			addr = strings.TrimSpace(user)
		}
		if addr == "" {
			continue
		}
		addresses = append(addresses, addr)

		if explicitService == "SMS" {
			service = "SMS"
		} else if explicitService == "" || explicitService == "any" {
			resolvedSvc := bb.getResolvedHandleService(addr)
			if resolvedSvc == "" {
				resolvedSvc = bb.lookupHandleService(addr)
			}
			if resolvedSvc == "SMS" {
				service = "SMS"
			}
		}
	}
	if len(addresses) == 0 {
		return nil, errors.New("no valid participant addresses provided")
	}

	req := CreateChatRequest{
		Addresses: addresses,
		Service:   service,
		Method:    "private-api",
		Message:   trimmedMsg,
		TempGUID:  fmt.Sprintf("temp-%s", RandString(8)),
	}

	var res CreateChatResponse
	if err := bb.apiSendPost("/api/v1/chat/new", req, &res); err != nil {
		bb.log.Error().Err(err).Any("response", res).Msg("Failure when creating group chat in BlueBubbles")
		return nil, err
	}
	if res.Status != 200 || res.Data.GUID == "" {
		bb.log.Error().Int64("statusCode", res.Status).Any("response", res).Msg("Failure when creating group chat in BlueBubbles")
		return nil, fmt.Errorf("could not create group chat (status %d: %s)", res.Status, res.Message)
	}

	return &imessage.CreateGroupResponse{
		GUID:     res.Data.GUID,
		ThreadID: res.Data.GroupID,
	}, nil
}

func (bb *blueBubbles) requireGroupAdmin(chatID string) error {
	if !bb.usingPrivateAPI.Load() {
		return errors.ErrUnsupported
	}
	parsed := imessage.ParseIdentifier(chatID)
	if !parsed.IsGroup {
		return errors.New("chat is not a group chat")
	}
	if parsed.Service == "SMS" || (parsed.Service == "any" && bb.getChatService(chatID) == "SMS") {
		return errors.New("SMS/MMS group chats cannot be modified")
	}
	return nil
}

func normalizeParticipantAddress(address string) string {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, ";") {
		if parsed := imessage.ParseIdentifier(trimmed); parsed.LocalID != "" {
			trimmed = parsed.LocalID
		}
	}
	if !strings.Contains(trimmed, "@") {
		if digits := numericOnly(trimmed); digits != "" {
			return "+" + digits
		}
	}
	return trimmed
}

func (bb *blueBubbles) SetGroupName(chatID, name string) error {
	bb.log.Trace().Str("chatID", chatID).Str("name", name).Msg("SetGroupName")
	if err := bb.requireGroupAdmin(chatID); err != nil {
		return err
	}

	bb.setPendingGroupName(chatID, name)
	var res GenericResponse
	if err := bb.apiPut(fmt.Sprintf("/api/v1/chat/%s", chatID), UpdateChatRequest{DisplayName: name}, &res); err != nil {
		bb.clearPendingGroupName(chatID, name)
		return err
	}
	if res.Status != 0 && res.Status != 200 {
		bb.clearPendingGroupName(chatID, name)
		return fmt.Errorf("could not rename group chat (status %d: %s)", res.Status, res.Message)
	}
	return nil
}

func (bb *blueBubbles) AddGroupParticipant(chatID, address string) error {
	bb.log.Trace().Str("chatID", chatID).Str("address", address).Msg("AddGroupParticipant")
	if err := bb.requireGroupAdmin(chatID); err != nil {
		return err
	}
	normalized := normalizeParticipantAddress(address)
	if normalized == "" {
		return errors.New("participant address is empty")
	}
	var res GenericResponse
	if err := bb.apiPost(fmt.Sprintf("/api/v1/chat/%s/participant/add", chatID), ChatParticipantRequest{Address: normalized}, &res); err != nil {
		return err
	}
	if res.Status != 0 && res.Status != 200 {
		return fmt.Errorf("could not add participant %s (status %d: %s)", normalized, res.Status, res.Message)
	}
	return nil
}

func (bb *blueBubbles) RemoveGroupParticipant(chatID, address string) error {
	bb.log.Trace().Str("chatID", chatID).Str("address", address).Msg("RemoveGroupParticipant")
	if err := bb.requireGroupAdmin(chatID); err != nil {
		return err
	}
	normalized := normalizeParticipantAddress(address)
	if normalized == "" {
		return errors.New("participant address is empty")
	}
	var res GenericResponse
	if err := bb.apiPost(fmt.Sprintf("/api/v1/chat/%s/participant/remove", chatID), ChatParticipantRequest{Address: normalized}, &res); err != nil {
		return err
	}
	if res.Status != 0 && res.Status != 200 {
		return fmt.Errorf("could not remove participant %s (status %d: %s)", normalized, res.Status, res.Message)
	}
	return nil
}

func (bb *blueBubbles) SetGroupAvatar(chatID, filename string, data []byte) error {
	bb.log.Trace().Str("chatID", chatID).Str("filename", filename).Int("size", len(data)).Msg("SetGroupAvatar")
	if err := bb.requireGroupAdmin(chatID); err != nil {
		return err
	}
	if len(data) == 0 {
		return errors.New("avatar data is empty")
	}
	if filename == "" {
		filename = "avatar.jpg"
	}
	var res GenericResponse
	if err := bb.apiPostAsFormData(fmt.Sprintf("/api/v1/chat/%s/icon", chatID), map[string]interface{}{
		"icon": formFile{Filename: filename, Data: data},
	}, &res); err != nil {
		return err
	}
	if res.Status != 0 && res.Status != 200 {
		return fmt.Errorf("could not set group avatar (status %d: %s)", res.Status, res.Message)
	}
	return nil
}

func (bb *blueBubbles) RemoveGroupAvatar(chatID string) error {
	bb.log.Trace().Str("chatID", chatID).Msg("RemoveGroupAvatar")
	if err := bb.requireGroupAdmin(chatID); err != nil {
		return err
	}
	var res GenericResponse
	if err := bb.apiDelete(fmt.Sprintf("/api/v1/chat/%s/icon", chatID), nil, &res); err != nil {
		return err
	}
	if res.Status != 0 && res.Status != 200 {
		return fmt.Errorf("could not remove group avatar (status %d: %s)", res.Status, res.Message)
	}
	return nil
}

func (bb *blueBubbles) LeaveGroup(chatID string) error {
	bb.log.Trace().Str("chatID", chatID).Msg("LeaveGroup")
	if err := bb.requireGroupAdmin(chatID); err != nil {
		return err
	}
	var res GenericResponse
	if err := bb.apiPost(fmt.Sprintf("/api/v1/chat/%s/leave", chatID), nil, &res); err != nil {
		return err
	}
	if res.Status != 0 && res.Status != 200 {
		return fmt.Errorf("could not leave group chat (status %d: %s)", res.Status, res.Message)
	}
	return nil
}

// Helper functions

type formFile struct {
	Filename string
	Data     []byte
}

func (bb *blueBubbles) joinBasePath(basePath, relPath string) string {
	base := strings.TrimRight(basePath, "/")
	rel := strings.TrimLeft(relPath, "/")
	if base == "" {
		return "/" + rel
	}
	return base + "/" + rel
}

func (bb *blueBubbles) wsUrl() (string, error) {
	rawURL := strings.TrimSpace(bb.bridge.GetConnectorConfig().BlueBubblesURL)
	if rawURL == "" {
		return "", errors.New("bluebubbles_url is not configured")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		bb.log.Error().Err(err).Msg("Error parsing BlueBubbles URL")
		return "", fmt.Errorf("invalid bluebubbles_url: %w", err)
	}

	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported bluebubbles_url scheme %q", u.Scheme)
	}

	u.Path = bb.joinBasePath(u.Path, "socket.io/")

	q := u.Query()
	q.Set("guid", bb.bridge.GetConnectorConfig().BlueBubblesPassword)
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func (bb *blueBubbles) apiURL(path string, queryParams map[string]string) (string, error) {
	rawURL := strings.TrimSpace(bb.bridge.GetConnectorConfig().BlueBubblesURL)
	if rawURL == "" {
		return "", errors.New("bluebubbles_url is not configured")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		bb.log.Error().Err(err).Msg("Error parsing BlueBubbles URL")
		return "", fmt.Errorf("invalid bluebubbles_url: %w", err)
	}

	u.Path = bb.joinBasePath(u.Path, path)

	q := u.Query()
	q.Set("password", bb.bridge.GetConnectorConfig().BlueBubblesPassword)

	for key, value := range queryParams {
		q.Add(key, value)
	}

	u.RawQuery = q.Encode()

	return u.String(), nil
}

type HTTPStatusError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("BlueBubbles %s %s returned HTTP %d: %s", e.Method, e.Path, e.StatusCode, e.Body)
}

type redactedError struct {
	msg   string
	cause error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.cause }

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func redactSecret(s, secret string) string {
	if s == "" || secret == "" {
		return s
	}
	s = strings.ReplaceAll(s, secret, "REDACTED")
	if escaped := url.QueryEscape(secret); escaped != secret {
		s = strings.ReplaceAll(s, escaped, "REDACTED")
	}
	return s
}

func redactURLString(rawURL, secret string) string {
	u, err := url.Parse(rawURL)
	if err == nil {
		q := u.Query()
		changed := false
		for _, key := range []string{"password", "guid", "token"} {
			if q.Has(key) {
				q.Set(key, "REDACTED")
				changed = true
			}
		}
		if changed {
			u.RawQuery = q.Encode()
			rawURL = u.String()
		}
	}
	return redactSecret(rawURL, secret)
}

func (bb *blueBubbles) sanitizeError(err error) error {
	if err == nil {
		return nil
	}
	var secret string
	if bb.bridge != nil && bb.bridge.GetConnectorConfig() != nil {
		secret = bb.bridge.GetConnectorConfig().BlueBubblesPassword
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		sanitizedInner := urlErr.Err
		if sanitizedInner != nil {
			innerMsg := redactSecret(sanitizedInner.Error(), secret)
			if innerMsg != sanitizedInner.Error() {
				sanitizedInner = &redactedError{msg: innerMsg, cause: sanitizedInner}
			}
		}
		return &url.Error{
			Op:  urlErr.Op,
			URL: redactURLString(urlErr.URL, secret),
			Err: sanitizedInner,
		}
	}

	msg := err.Error()
	redacted := redactSecret(msg, secret)
	if redacted != msg {
		return &redactedError{msg: redacted, cause: err}
	}
	return err
}

func checkHTTPStatus(method, path string, resp *http.Response, body []byte) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 256 {
		snippet = snippet[:256] + "..."
	}
	return &HTTPStatusError{
		Method:     method,
		Path:       path,
		StatusCode: resp.StatusCode,
		Body:       snippet,
	}
}

func (bb *blueBubbles) apiGet(path string, queryParams map[string]string, target interface{}) (err error) {
	reqURL, err := bb.apiURL(path, queryParams)
	if err != nil {
		return bb.sanitizeError(err)
	}

	req, err := http.NewRequestWithContext(bb.context(), http.MethodGet, reqURL, nil)
	if err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Str("path", path).Msg("Error creating GET request")
		return err
	}

	bb.bbRequestLock.Lock()
	response, err := bb.getHTTPClient(clientDefault).Do(req)
	bb.bbRequestLock.Unlock()
	if err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Str("path", path).Msg("Error making GET request")
		return err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Str("path", path).Msg("Error reading response body")
		return err
	}

	if err := checkHTTPStatus(http.MethodGet, path, response, responseBody); err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Int("status", response.StatusCode).Str("path", path).Msg("BlueBubbles GET request failed")
		return err
	}

	if target != nil {
		if err := json.Unmarshal(responseBody, target); err != nil {
			bb.log.Error().Err(err).Str("path", path).Msg("Error unmarshalling response body")
			return err
		}
	}

	return nil
}

func (bb *blueBubbles) apiPost(path string, payload interface{}, target interface{}) error {
	return bb.apiRequestWithClient(http.MethodPost, path, payload, target, clientDefault, false)
}

func (bb *blueBubbles) apiPut(path string, payload interface{}, target interface{}) error {
	return bb.apiRequestWithClient(http.MethodPut, path, payload, target, clientDefault, false)
}

func (bb *blueBubbles) apiSendPost(path string, payload interface{}, target interface{}) error {
	return bb.apiRequestWithClient(http.MethodPost, path, payload, target, clientSend, true)
}

func (bb *blueBubbles) apiDelete(path string, payload interface{}, target interface{}) error {
	return bb.apiRequestWithClient(http.MethodDelete, path, payload, target, clientDefault, false)
}

func (bb *blueBubbles) apiRequestWithClient(method, path string, payload interface{}, target interface{}, clientKind httpClientKind, isSend bool) (err error) {
	reqURL, err := bb.apiURL(path, nil)
	if err != nil {
		return bb.sanitizeError(err)
	}

	var bodyReader io.Reader
	if payload != nil {
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			bb.log.Error().Err(err).Str("path", path).Msg("Error marshalling payload")
			return err
		}
		bodyReader = bytes.NewBuffer(payloadJSON)
	}

	req, err := http.NewRequestWithContext(bb.context(), method, reqURL, bodyReader)
	if err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Str("method", method).Str("path", path).Msg("Error creating request")
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	bb.bbRequestLock.Lock()
	response, err := bb.getHTTPClient(clientKind).Do(req)
	bb.bbRequestLock.Unlock()
	if err != nil {
		err = bb.sanitizeError(err)
		if isSend && isTimeoutError(err) {
			err = fmt.Errorf("%w: %w", imessage.ErrSendTimeout, err)
		}
		bb.log.Error().Err(err).Str("method", method).Str("path", path).Msg("Error making request")
		return err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		err = bb.sanitizeError(err)
		if isSend && isTimeoutError(err) {
			err = fmt.Errorf("%w: %w", imessage.ErrSendTimeout, err)
		}
		bb.log.Error().Err(err).Str("path", path).Msg("Error reading response body")
		return err
	}

	if err := checkHTTPStatus(method, path, response, responseBody); err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Int("status", response.StatusCode).Str("method", method).Str("path", path).Msg("BlueBubbles request failed")
		return err
	}

	if target != nil {
		if err := json.Unmarshal(responseBody, target); err != nil {
			bb.log.Error().Err(err).Str("path", path).Msg("Error unmarshalling response body")
			return err
		}
	}

	return nil
}

func (bb *blueBubbles) apiPostAsFormData(path string, formData map[string]interface{}, target interface{}) error {
	reqURL, err := bb.apiURL(path, nil)
	if err != nil {
		return bb.sanitizeError(err)
	}

	// Create a new buffer to store the file content
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	for key, value := range formData {
		switch v := value.(type) {
		case int, bool:
			if err := writer.WriteField(key, fmt.Sprint(v)); err != nil {
				return err
			}
		case string:
			if err := writer.WriteField(key, v); err != nil {
				return err
			}
		case formFile:
			fname := v.Filename
			if fname == "" {
				fname = "attachment"
			}
			part, err := writer.CreateFormFile(key, fname)
			if err != nil {
				bb.log.Error().Err(err).Msg("Error creating form-data field")
				return err
			}
			if _, err = part.Write(v.Data); err != nil {
				bb.log.Error().Err(err).Msg("Error writing file to form-data")
				return err
			}
		case []byte:
			part, err := writer.CreateFormFile(key, "attachment")
			if err != nil {
				bb.log.Error().Err(err).Msg("Error creating form-data field")
				return err
			}
			if _, err = part.Write(v); err != nil {
				bb.log.Error().Err(err).Msg("Error writing file to form-data")
				return err
			}
		default:
			return fmt.Errorf("unable to serialize %s (type %T) into form-data", key, v)
		}
	}

	// Close the multipart writer
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(bb.context(), http.MethodPost, reqURL, &body)
	if err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Str("path", path).Msg("Error creating multipart POST request")
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Make the HTTP POST request
	bb.bbRequestLock.Lock()
	response, err := bb.getHTTPClient(clientMedia).Do(req)
	bb.bbRequestLock.Unlock()
	if err != nil {
		err = bb.sanitizeError(err)
		if isTimeoutError(err) {
			err = fmt.Errorf("%w: %w", imessage.ErrSendTimeout, err)
		}
		bb.log.Error().Err(err).Str("path", path).Msg("Error making POST request")
		return err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		err = bb.sanitizeError(err)
		if isTimeoutError(err) {
			err = fmt.Errorf("%w: %w", imessage.ErrSendTimeout, err)
		}
		bb.log.Error().Err(err).Str("path", path).Msg("Error reading response body")
		return err
	}

	if err := checkHTTPStatus(http.MethodPost, path, response, responseBody); err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Int("status", response.StatusCode).Str("path", path).Msg("BlueBubbles multipart POST failed")
		return err
	}

	if target != nil {
		if err := json.Unmarshal(responseBody, target); err != nil {
			bb.log.Error().Err(err).Str("path", path).Msg("Error unmarshalling response body")
			return err
		}
	}

	return nil
}

func (bb *blueBubbles) convertBBContactToiMessageContact(bbContact *Contact) (*imessage.Contact, error) {
	var convertedID string
	var imageData []byte
	var err error

	switch id := bbContact.ID.(type) {
	case string:
		// id is already a string, use it as is
		convertedID = id
	case float64:
		// id is a float, convert it to a string
		convertedID = strconv.FormatFloat(id, 'f', -1, 64)
	case nil:
		convertedID = ""
	default:
		bb.log.Error().Interface("id", id).Msg("Unknown type for contact ID")
		convertedID = ""
	}

	if bbContact.Avatar != nil && *bbContact.Avatar != "" {
		imageData, err = base64.StdEncoding.DecodeString(*bbContact.Avatar)
		if err != nil {
			bb.log.Error().Err(err).Str("DisplayName", bbContact.DisplayName).Msg("Error decoding contact avatar")
		}
	}

	return &imessage.Contact{
		FirstName: bbContact.FirstName,
		LastName:  bbContact.LastName,
		Nickname:  bbContact.DisplayName,
		Phones:    convertPhones(bbContact.PhoneNumbers),
		Emails:    convertEmails(bbContact.Emails),
		UserGUID:  convertedID,
		Avatar:    imageData,
	}, nil
}

func parseThreadOriginatorPart(part string) (int, error) {
	trimmed := strings.TrimSpace(part)
	if trimmed == "" {
		return 0, nil
	}
	parts := strings.Split(trimmed, ":")
	return strconv.Atoi(parts[0])
}

func (bb *blueBubbles) convertBBMessageToiMessage(bbMessage Message) (*imessage.Message, error) {
	return bb.convertBBMessageToiMessageWithOptions(bbMessage, true)
}

func (bb *blueBubbles) convertBBMessageToiMessageWithOptions(bbMessage Message, downloadAttachments bool) (*imessage.Message, error) {
	if len(bbMessage.Chats) == 0 {
		return nil, fmt.Errorf("BlueBubbles message %s has no chats", bbMessage.GUID)
	}

	var message imessage.Message

	// Convert bluebubbles.Message to imessage.Message
	message.GUID = bbMessage.GUID
	message.Time = time.UnixMilli(bbMessage.DateCreated)
	message.Subject = bbMessage.Subject
	message.Text = bbMessage.Text
	message.ChatGUID = bbMessage.Chats[0].GUID

	// bbMessage.Handle seems to always be the other person,
	// so the sender/target depends on whether the message is from you
	if bbMessage.IsFromMe {
		message.JSONTargetGUID = bbMessage.Handle.Address
		message.Target = imessage.Identifier{
			LocalID: bbMessage.Handle.Address,
			Service: bbMessage.Handle.Service,
			IsGroup: false,
		}
	} else {
		message.JSONSenderGUID = bbMessage.Handle.Address
		message.Sender = imessage.Identifier{
			LocalID: bbMessage.Handle.Address,
			Service: bbMessage.Handle.Service,
			IsGroup: false,
		}
	}

	message.Service = bbMessage.Handle.Service
	if message.Service == "" {
		message.Service = imessage.ParseIdentifier(message.ChatGUID).Service
	}
	if message.Service != "" && message.Service != "any" {
		bb.setChatService(message.ChatGUID, message.Service)
	}
	message.IsFromMe = bbMessage.IsFromMe
	message.IsRead = bbMessage.DateRead != 0
	if message.IsRead {
		message.ReadAt = time.UnixMilli(bbMessage.DateRead)
	}
	message.IsDelivered = bbMessage.DateDelivered != 0
	message.IsSent = bbMessage.DateCreated != 0 // assume yes because we made it to this part of the code
	message.IsEmote = false                     // emojis seem to send either way, and BB doesn't say whether there is one or not
	message.IsAudioMessage = bbMessage.IsAudioMessage
	message.IsEdited = bbMessage.DateEdited != 0
	message.IsRetracted = bbMessage.DateRetracted != 0

	message.ReplyToGUID = bbMessage.ThreadOriginatorGUID
	if bbMessage.ThreadOriginatorPart != "" {
		replyPart, err := parseThreadOriginatorPart(bbMessage.ThreadOriginatorPart)
		if err != nil {
			bb.log.Debug().Err(err).Str("threadOriginatorPart", bbMessage.ThreadOriginatorPart).Msg("Unable to parse ThreadOriginatorPart")
		} else {
			message.ReplyToPart = replyPart
		}
	}

	// Tapbacks (classic 6 + iOS 18 / macOS Sequoia custom emoji tapbacks)
	if bbMessage.AssociatedMessageGUID != "" &&
		(bbMessage.AssociatedMessageType != "" || bbMessage.AssociatedMessageEmoji != "") {
		tbType, customEmoji := bb.convertBBTapbackToImessageTapback(string(bbMessage.AssociatedMessageType), bbMessage.AssociatedMessageEmoji)
		if tbType != 0 {
			isRemove := tbType >= 3000 && tbType < 4000
			isCustomEmoji := tbType == imessage.TapbackCustomEmoji || tbType == imessage.TapbackCustomEmoji+imessage.TapbackRemoveOffset
			if !isCustomEmoji || customEmoji != "" || isRemove {
				tb := &imessage.Tapback{
					TargetGUID:  bbMessage.AssociatedMessageGUID,
					Type:        tbType,
					CustomEmoji: customEmoji,
				}
				if _, parseErr := tb.Parse(); parseErr != nil {
					if !errors.Is(parseErr, imessage.ErrUnknownTapbackTargetType) {
						bb.log.Debug().Err(parseErr).Str("associatedMessageGuid", bbMessage.AssociatedMessageGUID).Msg("Failed to parse tapback target GUID")
						tb = nil
					}
				}
				message.Tapback = tb
			}
		}
	}

	// Attachments: preserve exact source part indices even when a download fails
	// so surviving parts do not shift and attachment-only messages are not mistaken
	// for retractions.
	if len(bbMessage.Attachments) > 0 {
		message.Attachments = make([]*imessage.Attachment, len(bbMessage.Attachments))
		for i, bbAttachment := range bbMessage.Attachments {
			if !downloadAttachments {
				message.Attachments[i] = &imessage.Attachment{
					GUID:     bbAttachment.GUID,
					FileName: bbAttachment.TransferName,
					MimeType: bbAttachment.MimeType,
				}
				continue
			}
			downloaded, err := bb.downloadAttachment(bbAttachment.GUID)
			if err != nil {
				bb.log.Err(err).Str("attachmentGUID", bbAttachment.GUID).Msg("Failed to download attachment")
				message.Attachments[i] = &imessage.Attachment{
					GUID:          bbAttachment.GUID,
					FileName:      bbAttachment.TransferName,
					MimeType:      bbAttachment.MimeType,
					DownloadError: err.Error(),
				}
				continue
			}

			message.Attachments[i] = downloaded
		}
	}

	// Group name, member, and avatar changes all come through as messages
	// with a special ItemType to denote it isn't a regular message
	message.ItemType = imessage.ItemType(bbMessage.ItemType)

	// Changes based on the ItemType, but denotes user or icon add vs remove actions
	message.GroupActionType = imessage.GroupActionType(bbMessage.GroupActionType)
	message.NewGroupName = bbMessage.GroupTitle

	// TODO Richlinks
	// message.RichLink =

	message.ThreadID = bbMessage.Chats[0].GroupID

	return &message, nil
}

func (bb *blueBubbles) convertBBTapbackToImessageTapback(associatedMessageType, associatedMessageEmoji string) (tbType imessage.TapbackType, customEmoji string) {
	raw := strings.TrimSpace(strings.ToLower(associatedMessageType))

	// Numeric associated message types (2000-2006 add, 3000-3006 remove)
	if n, err := strconv.Atoi(raw); err == nil {
		if (n >= 2000 && n <= 2006) || (n >= 3000 && n <= 3006) {
			tbType = imessage.TapbackType(n)
			if n == int(imessage.TapbackCustomEmoji) || n == int(imessage.TapbackCustomEmoji+imessage.TapbackRemoveOffset) {
				customEmoji = associatedMessageEmoji
			}
			return tbType, customEmoji
		}
		return 0, ""
	}

	remove := strings.HasPrefix(raw, "-")
	name := strings.TrimPrefix(raw, "-")

	switch {
	case strings.Contains(name, "love"):
		tbType = imessage.TapbackLove
	case strings.Contains(name, "dislike"):
		tbType = imessage.TapbackDislike
	case strings.Contains(name, "like"):
		tbType = imessage.TapbackLike
	case strings.Contains(name, "laugh"):
		tbType = imessage.TapbackLaugh
	case strings.Contains(name, "emphasize") || strings.Contains(name, "emphasis"):
		tbType = imessage.TapbackEmphasis
	case strings.Contains(name, "question"):
		tbType = imessage.TapbackQuestion
	case name == "emoji" || associatedMessageEmoji != "":
		tbType = imessage.TapbackCustomEmoji
		customEmoji = associatedMessageEmoji
	default:
		return 0, ""
	}

	if remove {
		tbType += imessage.TapbackRemoveOffset
	}
	return tbType, customEmoji
}

func (bb *blueBubbles) convertBBChatToiMessageChat(bbChat Chat) (*imessage.ChatInfo, error) {
	members := make([]string, len(bbChat.Participants))

	for i, participant := range bbChat.Participants {
		members[i] = participant.Address
	}

	parsed := imessage.ParseIdentifier(bbChat.GUID)
	service := parsed.Service
	if bbChat.LastMessage != nil && bbChat.LastMessage.Handle.Service != "" && bbChat.LastMessage.Handle.Service != "any" {
		service = bbChat.LastMessage.Handle.Service
	} else if service == "any" {
		service = ""
	}
	if service != "" {
		bb.setChatService(bbChat.GUID, service)
	}

	chatInfo := &imessage.ChatInfo{
		JSONChatGUID: bbChat.GUID,
		Identifier:   parsed,
		DisplayName:  bbChat.DisplayName,
		Members:      members,
		ThreadID:     bbChat.GroupID,
		Service:      service,
	}

	return chatInfo, nil
}

func (bb *blueBubbles) downloadAttachment(guid string) (attachment *imessage.Attachment, err error) {
	bb.log.Trace().Str("guid", guid).Msg("downloadAttachment")

	var attachmentResponse AttachmentResponse
	err = bb.apiGet(fmt.Sprintf("/api/v1/attachment/%s", guid), nil, &attachmentResponse)
	if err != nil {
		bb.log.Err(err).Str("guid", guid).Msg("Failed to get attachment from BlueBubbles")
		return nil, err
	}

	downloadPath := fmt.Sprintf("/api/v1/attachment/%s/download", guid)
	reqURL, err := bb.apiURL(downloadPath, nil)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(bb.context(), http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, bb.sanitizeError(err)
	}

	response, err := bb.getHTTPClient(clientMedia).Do(req)
	if err != nil {
		err = bb.sanitizeError(err)
		bb.log.Error().Err(err).Str("guid", guid).Msg("Error downloading attachment from BlueBubbles")
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		bodySnippet, _ := io.ReadAll(io.LimitReader(response.Body, 256))
		err = bb.sanitizeError(checkHTTPStatus(http.MethodGet, downloadPath, response, bodySnippet))
		bb.log.Error().Err(err).Str("guid", guid).Msg("BlueBubbles attachment download failed")
		return nil, err
	}

	tempFile, err := os.CreateTemp(os.TempDir(), "bb-attachment-*")
	if err != nil {
		bb.log.Error().Err(err).Msg("Error creating temp file")
		return nil, err
	}
	tempPath := tempFile.Name()

	_, copyErr := io.Copy(tempFile, response.Body)
	closeErr := tempFile.Close()
	if copyErr != nil {
		_ = os.Remove(tempPath)
		bb.log.Error().Err(copyErr).Msg("Error copying response body to temp file")
		return nil, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tempPath)
		bb.log.Error().Err(closeErr).Msg("Error closing temp file")
		return nil, closeErr
	}

	return &imessage.Attachment{
		GUID:       guid,
		PathOnDisk: tempPath,
		FileName:   attachmentResponse.Data.TransferName,
		MimeType:   attachmentResponse.Data.MimeType,
		IsTemp:     true,
	}, nil
}

func convertPhones(phoneNumbers []PhoneNumber) []string {
	var phones []string
	for _, phone := range phoneNumbers {
		// Convert the phone number format as needed
		phones = append(phones, phone.Address)
	}
	return phones
}

func convertEmails(emails []Email) []string {
	var emailAddresses []string
	for _, email := range emails {
		// Convert the email address format as needed
		emailAddresses = append(emailAddresses, email.Address)
	}
	return emailAddresses
}

func timeToFloat(time time.Time) float64 {
	if time.IsZero() {
		return 0
	}
	return float64(time.Unix()) + float64(time.Nanosecond())/1e9
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func reverseList(input []*imessage.Message) []*imessage.Message {
	// Get the length of the slice
	length := len(input)

	// Create a new slice to store the reversed elements
	reversed := make([]*imessage.Message, length)

	// Iterate over the original slice in reverse order
	for i, value := range input {
		reversed[length-i-1] = value
	}

	return reversed
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func containsInt(slice []int, num int) bool {
	for _, n := range slice {
		if n == num {
			return true
		}
	}
	return false
}

func numericOnly(s string) string {
	var result strings.Builder
	for _, char := range s {
		if unicode.IsDigit(char) {
			result.WriteRune(char)
		}
	}
	return result.String()
}

// These functions are probably not necessary

func (bb *blueBubbles) SendMessageBridgeResult(chatID, messageID string, eventID id.EventID, success bool) {
	if messageID == "" {
		return
	}
	bb.editedLock.Lock()
	var editTS int64
	var hasEdit bool
	if queue := bb.inflightEdits[messageID]; len(queue) > 0 {
		editTS = queue[0]
		hasEdit = true
		if len(queue) == 1 {
			delete(bb.inflightEdits, messageID)
		} else {
			bb.inflightEdits[messageID] = queue[1:]
		}
	}
	_, hasRetraction := bb.inflightRetractions[messageID]
	if hasRetraction {
		delete(bb.inflightRetractions, messageID)
	}
	if success && hasRetraction {
		delete(bb.lastEdited, messageID)
		delete(bb.inflightEdits, messageID)
	}
	bb.editedLock.Unlock()

	if !success {
		return
	}
	if hasRetraction {
		bb.deletePersistedEditTS(messageID)
		return
	}
	if hasEdit {
		bb.markEdited(messageID, editTS)
	}
}
func (bb *blueBubbles) SendBackfillResult(chatID, backfillID string, success bool, idMap map[string][]id.EventID) {
}
func (bb *blueBubbles) SendChatBridgeResult(guid string, mxid id.RoomID) {
}
func (bb *blueBubbles) NotifyUpcomingMessage(eventID id.EventID) {
}
func (bb *blueBubbles) PreStartupSyncHook() (resp imessage.StartupSyncHookResponse, err error) {
	return imessage.StartupSyncHookResponse{
		SkipSync: false,
	}, nil
}
func (bb *blueBubbles) PostStartupSyncHook() {
}

func (bb *blueBubbles) Capabilities() imessage.ConnectorCapabilities {
	privateAPI := bb.usingPrivateAPI.Load()
	return imessage.ConnectorCapabilities{
		MessageSendResponses:     true,
		SendTapbacks:             privateAPI,
		SendCustomEmojiTapbacks:  privateAPI && bb.supportsCustomEmojiTapbacks.Load(),
		UnsendMessages:           privateAPI,
		EditMessages:             privateAPI,
		GroupAdmin:               privateAPI,
		SendReadReceipts:         privateAPI,
		SendTypingNotifications:  privateAPI,
		SendCaptions:             true,
		BridgeState:              true,
		MessageStatusCheckpoints: false,
		DeliveredStatus:          privateAPI,
		ContactChatMerging:       false,
		RichLinks:                false,
		ChatBridgeResult:         false,
	}
}

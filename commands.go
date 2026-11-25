// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2022 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/mautrix-imessage/imessage"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridge/commands"
)

var (
	HelpSectionManagingPortals = commands.HelpSection{Name: "Managing portals", Order: 15}
)

type WrappedCommandEvent struct {
	*commands.Event
	Bridge *IMBridge
	User   *User
	Portal *Portal
}

func (br *IMBridge) RegisterCommands() {
	proc := br.CommandProcessor.(*commands.Processor)
	proc.AddHandlers(
		commands.CommandLoginMatrix,
		commands.CommandLogoutMatrix,
		commands.CommandDiscardMegolmSession,
		commands.CommandSetPowerLevel,
		cmdHealth,
		cmdPM,
		cmdCreateGroup,
		cmdGroupRename,
		cmdGroupAdd,
		cmdGroupRemove,
		cmdGroupLeave,
		cmdSync,
		cmdSearchContacts,
		cmdRefreshContacts,
	)
}

func wrapCommand(handler func(*WrappedCommandEvent)) func(*commands.Event) {
	return func(ce *commands.Event) {
		user := ce.User.(*User)
		var portal *Portal
		if ce.Portal != nil {
			portal = ce.Portal.(*Portal)
		}
		br := ce.Bridge.Child.(*IMBridge)
		handler(&WrappedCommandEvent{ce, br, user, portal})
	}
}

var cmdPM = &commands.FullHandler{
	Func: wrapCommand(fnPM),
	Name: "pm",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Creates a new PM with the specified number or address.",
	},
	RequiresPortal: false,
	RequiresLogin:  false,
}

func fnPM(ce *WrappedCommandEvent) {
	ce.Bridge.ZLog.Trace().Interface("args", ce.Args).Str("cmd", ce.Command).Msg("fnPM")

	if len(ce.Args) == 0 {
		ce.Reply("**Usage:** `pm <international phone number>` OR `pm <apple id email address>`")
		return
	}

	startedDm, err := ce.Bridge.WebsocketHandler.StartChat(*&StartDMRequest{
		Identifier:    ce.RawArgs,
		Force:         false,
		ActuallyStart: true,
	})

	if err != nil {
		ce.Reply("Failed to start PM: %s", err)
	} else {
		ce.Reply("Created portal room [%s](%s) and invited you to it.", startedDm.RoomID, startedDm.RoomID.URI(ce.Bridge.Config.Homeserver.Domain).MatrixToURL())
	}
}

var cmdSearchContacts = &commands.FullHandler{
	Func: wrapCommand(fnSearchContacts),
	Name: "search-contacts",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Searches contacts based on name, phone, and email (only for BlueBubbles mode).",
	},
	RequiresPortal: false,
	RequiresLogin:  false,
}

func fnSearchContacts(ce *WrappedCommandEvent) {
	ce.Bridge.ZLog.Trace().Interface("args", ce.Args).Str("cmd", ce.Command).Msg("fnSearchContacts")

	if len(ce.Args) == 0 {
		ce.Reply("**Usage:** `search-contacts <search terms>`")
		return
	}

	searchTerm := strings.TrimSpace(ce.RawArgs)
	if searchTerm == "" {
		ce.Reply("**Usage:** `search-contacts <search terms>`")
		return
	}

	contacts, err := ce.Bridge.IM.SearchContactList(searchTerm)
	if err != nil {
		ce.Reply("Failed to search contacts: %s", err)
	} else {
		if len(contacts) == 0 {
			ce.Reply("No contacts found for search `%s`", searchTerm)
		} else {
			replyMessage := fmt.Sprintf("Found %d contacts:\n", len(contacts))

			for _, contact := range contacts {
				markdownString := buildContactString(contact)
				replyMessage += markdownString
				replyMessage += strings.Repeat("-", 40) + "\n"
			}

			ce.Reply(replyMessage)
		}
	}
}

func buildContactString(contact *imessage.Contact) string {
	name := contact.Nickname
	if name == "" {
		name = fmt.Sprintf("%s %s", contact.FirstName, contact.LastName)
	}

	contactInfo := fmt.Sprintf("**%s**\n", name)

	if len(contact.Phones) > 0 {
		contactInfo += "- **Phones:**\n"
		for _, phone := range contact.Phones {
			contactInfo += fmt.Sprintf("  - %s\n", phone)
		}
	}

	if len(contact.Emails) > 0 {
		contactInfo += "- **Emails:**\n"
		for _, email := range contact.Emails {
			contactInfo += fmt.Sprintf("  - %s\n", email)
		}
	}

	return contactInfo
}

var cmdRefreshContacts = &commands.FullHandler{
	Func: wrapCommand(fnRefreshContacts),
	Name: "refresh-contacts",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Request that the bridge reload cached contacts (only for BlueBubbles mode).",
	},
	RequiresPortal: false,
	RequiresLogin:  false,
}

func fnRefreshContacts(ce *WrappedCommandEvent) {
	ce.Bridge.ZLog.Trace().Interface("args", ce.Args).Str("cmd", ce.Command).Msg("fnRefreshContacts")

	err := ce.Bridge.IM.RefreshContactList()
	if err != nil {
		ce.Reply("Failed to refresh contacts: %s", err)
	} else {
		ce.Reply("Contacts list updated!")
	}
}

var cmdCreateGroup = &commands.FullHandler{
	Func: wrapCommand(fnCreateGroup),
	Name: "create-group",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Creates a new group chat with the specified phone numbers or email addresses (and initial message for BlueBubbles).",
		Args:        "<_address1_> <_address2_> [...] [-- <_initial message_>]",
	},
	RequiresPortal: false,
	RequiresLogin:  false,
}

func parseCreateGroupArgs(rawArgs string) (addresses []string, initialMessage string) {
	trimmed := strings.TrimSpace(rawArgs)
	if trimmed == "" {
		return nil, ""
	}
	// Split on standalone "--" separator to preserve spacing in the initial message.
	var addrPart string
	if idx := strings.Index(trimmed, " -- "); idx >= 0 {
		addrPart = trimmed[:idx]
		initialMessage = strings.TrimSpace(trimmed[idx+4:])
	} else if strings.HasSuffix(trimmed, " --") {
		addrPart = strings.TrimSuffix(trimmed, " --")
	} else if trimmed == "--" {
		return nil, ""
	} else {
		addrPart = trimmed
	}
	addresses = strings.Fields(addrPart)
	return addresses, initialMessage
}

func fnCreateGroup(ce *WrappedCommandEvent) {
	ce.Bridge.ZLog.Trace().Interface("args", ce.Args).Str("cmd", ce.Command).Msg("fnCreateGroup")

	addresses, initialMessage := parseCreateGroupArgs(ce.RawArgs)
	withMsgAPI, needsInitialMessage := ce.Bridge.IM.(imessage.GroupCreationWithMessageAPI)

	if len(addresses) < 2 || (needsInitialMessage && initialMessage == "") {
		if needsInitialMessage {
			ce.Reply("**Usage:** `create-group <address1> <address2> [...] -- <initial message>`")
		} else {
			ce.Reply("**Usage:** `create-group <address1> <address2> [...]`")
		}
		return
	}

	guids := make([]string, len(addresses))
	for i, identifier := range addresses {
		if imessage.IsResolvedChatGUID(identifier) {
			guids[i] = identifier
		} else {
			resolved, err := ce.Bridge.IM.ResolveIdentifier(identifier)
			if err != nil {
				ce.Reply("Failed to resolve identifier `%s`: %s", identifier, err)
				return
			}
			guids[i] = resolved
		}
	}

	var createResp *imessage.CreateGroupResponse
	var err error
	if needsInitialMessage {
		createResp, err = withMsgAPI.CreateGroupWithMessage(guids, initialMessage)
	} else {
		createResp, err = ce.Bridge.IM.CreateGroup(guids)
	}
	if err != nil {
		ce.Reply("Failed to create group: %s", err)
		return
	}

	var initialMsgWarning string
	if !needsInitialMessage && initialMessage != "" {
		if _, sendErr := ce.Bridge.IM.SendMessage(createResp.GUID, initialMessage, "", 0, nil, nil); sendErr != nil {
			initialMsgWarning = fmt.Sprintf(" (failed to send initial message: %s)", sendErr)
		}
	}

	portal := ce.Bridge.GetPortalByGUID(createResp.GUID)
	portal.roomCreateLock.Lock()
	alreadyExists := len(portal.MXID) > 0
	if !alreadyExists && createResp.ThreadID != "" {
		portal.ThreadID = createResp.ThreadID
	}
	roomID := portal.MXID
	portal.roomCreateLock.Unlock()

	if !alreadyExists {
		if err = portal.CreateMatrixRoom(nil, nil); err != nil {
			ce.Reply("Created iMessage group `%s`, but failed to create Matrix room: %s", createResp.GUID, err)
			return
		}
		roomID = portal.MXID
		ce.Reply("Group portal room [%s](%s) is ready.%s", roomID, roomID.URI(ce.Bridge.Config.Homeserver.Domain).MatrixToURL(), initialMsgWarning)
	} else {
		ce.Reply("Group portal room [%s](%s) already exists.%s", roomID, roomID.URI(ce.Bridge.Config.Homeserver.Domain).MatrixToURL(), initialMsgWarning)
	}
}

var cmdSync = &commands.FullHandler{
	Func: wrapCommand(fnSync),
	Name: "sync",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Refreshes contacts and attempts to resync existing portal rooms and metadata.",
	},
	RequiresPortal: false,
	RequiresLogin:  false,
}

func fnSync(ce *WrappedCommandEvent) {
	ce.Bridge.ZLog.Trace().Str("cmd", ce.Command).Msg("fnSync")

	if !ce.Bridge.syncInProgress.CompareAndSwap(false, true) {
		ce.Reply("A sync is already in progress.")
		return
	}

	ce.Reply("Syncing contacts and portals…")

	go func() {
		defer ce.Bridge.syncInProgress.Store(false)

		var contactStatus string
		if err := ce.Bridge.IM.RefreshContactList(); err != nil {
			if errors.Is(err, errors.ErrUnsupported) {
				contactStatus = "Contact list refresh is not supported by this connector."
			} else {
				ce.Bridge.ZLog.Warn().Err(err).Msg("Failed to refresh contact list during manual sync")
				contactStatus = fmt.Sprintf("Failed to refresh contact list: %s.", err)
			}
		} else {
			contactStatus = "Contact list refreshed."
		}

		portals := ce.Bridge.GetAllPortals()
		attempted := 0
		for _, portal := range portals {
			if len(portal.MXID) > 0 {
				portal.Sync(false)
				attempted++
			}
		}
		ce.Reply("%s Attempted sync for %d portal rooms.", contactStatus, attempted)
	}()
}

var cmdHealth = &commands.FullHandler{
	Func:    wrapCommand(fnHealth),
	Name:    "health",
	Aliases: []string{"status"},
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Shows connector health, server version, Private API/helper status, websocket state, and last-event age.",
	},
	RequiresPortal: false,
	RequiresLogin:  false,
}

func formatDurationShort(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return "<1s"
	}
	d = d.Round(time.Second)
	days := d / (24 * time.Hour)
	d %= 24 * time.Hour
	hours := d / time.Hour
	d %= time.Hour
	mins := d / time.Minute
	secs := (d % time.Minute) / time.Second

	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if mins > 0 {
		parts = append(parts, fmt.Sprintf("%dm", mins))
	}
	if secs > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", secs))
	}
	return strings.Join(parts, "")
}

func formatHealthReport(platform string, h *imessage.ConnectorHealth, now time.Time) string {
	overall := "Healthy"
	if !h.WSConnected {
		overall = "Disconnected"
	} else if h.ServerInfoErr != "" {
		overall = "Degraded (server info unreachable)"
	} else if h.PrivateAPI && h.HelperConnected != nil && !*h.HelperConnected {
		overall = "Degraded (Private API helper disconnected from Messages.app)"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "**Bridge Health (%s):** %s\n", platform, overall)

	if h.ServerVersion != "" || h.OSVersion != "" {
		serverStr := h.ServerVersion
		if serverStr == "" {
			serverStr = "unknown"
		}
		if h.OSVersion != "" {
			fmt.Fprintf(&b, "- **Server:** %s (macOS %s)\n", serverStr, h.OSVersion)
		} else {
			fmt.Fprintf(&b, "- **Server:** %s\n", serverStr)
		}
	}
	if h.ProxyService != "" {
		fmt.Fprintf(&b, "- **Proxy:** %s\n", h.ProxyService)
	}

	privateStr := "disabled"
	if h.PrivateAPI {
		privateStr = "enabled"
	}
	helperStr := "not reported"
	if h.HelperConnected != nil {
		if *h.HelperConnected {
			helperStr = "connected"
		} else {
			helperStr = "DISCONNECTED"
		}
	}
	fmt.Fprintf(&b, "- **Private API:** %s (helper: %s)\n", privateStr, helperStr)

	if h.WSConnected {
		uptime := ""
		if !h.ConnectedSince.IsZero() {
			uptime = fmt.Sprintf(" (up %s)", formatDurationShort(now.Sub(h.ConnectedSince)))
		}
		fmt.Fprintf(&b, "- **WebSocket:** connected%s, reconnects: %d\n", uptime, h.ReconnectCount)
	} else {
		downtime := ""
		if !h.DisconnectedSince.IsZero() {
			downtime = fmt.Sprintf(" (down %s)", formatDurationShort(now.Sub(h.DisconnectedSince)))
		}
		fmt.Fprintf(&b, "- **WebSocket:** disconnected%s, reconnects: %d\n", downtime, h.ReconnectCount)
	}
	if h.LastDisconnectErr != "" {
		fmt.Fprintf(&b, "- **Last disconnect error:** `%s`\n", h.LastDisconnectErr)
	}

	if h.LastEventAt.IsZero() {
		fmt.Fprintf(&b, "- **Last event:** none yet\n")
	} else {
		fmt.Fprintf(&b, "- **Last event:** %s ago\n", formatDurationShort(now.Sub(h.LastEventAt)))
	}

	if !h.LastCatchUpAt.IsZero() {
		catchUpLine := fmt.Sprintf("%s ago (%d chats, %d messages)", formatDurationShort(now.Sub(h.LastCatchUpAt)), h.LastCatchUpChats, h.LastCatchUpMsgs)
		if h.LastCatchUpErr != "" {
			catchUpLine += fmt.Sprintf(" — error: `%s`", h.LastCatchUpErr)
		}
		fmt.Fprintf(&b, "- **Last catch-up sync:** %s\n", catchUpLine)
	}

	if h.ServerInfoErr != "" {
		fmt.Fprintf(&b, "- **Server info error:** `%s`\n", h.ServerInfoErr)
	}
	return strings.TrimRight(b.String(), "\n")
}

func fnHealth(ce *WrappedCommandEvent) {
	ce.Bridge.ZLog.Trace().Str("cmd", ce.Command).Msg("fnHealth")

	healthAPI, ok := ce.Bridge.IM.(imessage.HealthAPI)
	if !ok {
		ce.Reply("Health reporting is not supported by connector `%s`.", ce.Bridge.Config.IMessage.Platform)
		return
	}
	health := healthAPI.GetHealth()
	if health == nil {
		ce.Reply("Connector `%s` did not return health status.", ce.Bridge.Config.IMessage.Platform)
		return
	}
	ce.Reply("%s", formatHealthReport(ce.Bridge.Config.IMessage.Platform, health, time.Now()))
}

func requirePortalGroupAdmin(ce *WrappedCommandEvent) (imessage.GroupAdminAPI, bool) {
	if ce.Portal == nil || ce.Portal.IsPrivateChat() {
		ce.Reply("This command must be run inside a bridged group chat room.")
		return nil, false
	}
	groupAdmin, ok := ce.Bridge.IM.(imessage.GroupAdminAPI)
	if !ok || !ce.Bridge.IM.Capabilities().GroupAdmin {
		ce.Reply("Group management is not supported (requires BlueBubbles Private API).")
		return nil, false
	}
	if ce.Portal.knownService() != "iMessage" {
		if ce.Portal.knownService() == "SMS" {
			ce.Reply("SMS/MMS group chats cannot be modified.")
		} else {
			ce.Reply("This group's service is not yet confirmed as iMessage; wait for a message or run `sync` first.")
		}
		return nil, false
	}
	return groupAdmin, true
}

func resolveParticipantLocalID(ce *WrappedCommandEvent, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("empty participant identifier")
	}
	if imessage.IsResolvedChatGUID(trimmed) {
		return imessage.ParseIdentifier(trimmed).LocalID, nil
	}
	if resolved, err := ce.Bridge.IM.ResolveIdentifier(trimmed); err == nil {
		if localID := imessage.ParseIdentifier(resolved).LocalID; localID != "" {
			return localID, nil
		}
	}
	if !strings.Contains(trimmed, "@") {
		var digits strings.Builder
		for _, r := range trimmed {
			if r >= '0' && r <= '9' {
				digits.WriteRune(r)
			}
		}
		if digits.Len() > 0 {
			return "+" + digits.String(), nil
		}
	}
	return trimmed, nil
}

var cmdGroupRename = &commands.FullHandler{
	Func: wrapCommand(fnGroupRename),
	Name: "group-rename",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Renames the current iMessage group chat (requires Private API).",
		Args:        "<_new name_>",
	},
	RequiresPortal: true,
	RequiresLogin:  false,
}

func fnGroupRename(ce *WrappedCommandEvent) {
	groupAdmin, ok := requirePortalGroupAdmin(ce)
	if !ok {
		return
	}
	newName := strings.TrimSpace(ce.RawArgs)
	if newName == "" {
		ce.Reply("**Usage:** `group-rename <new name>`")
		return
	}
	if err := groupAdmin.SetGroupName(ce.Portal.GUID, newName); err != nil {
		ce.Reply("Failed to rename group: %s", err)
		return
	}
	ce.Portal.UpdateName(newName, ce.Portal.MainIntent())
	ce.Portal.Update(nil)
	ce.Reply("Renamed group to **%s**.", newName)
}

var cmdGroupAdd = &commands.FullHandler{
	Func: wrapCommand(fnGroupAdd),
	Name: "group-add",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Adds a participant (phone number or Apple ID email) to the current iMessage group chat.",
		Args:        "<_phone or email_>",
	},
	RequiresPortal: true,
	RequiresLogin:  false,
}

func fnGroupAdd(ce *WrappedCommandEvent) {
	groupAdmin, ok := requirePortalGroupAdmin(ce)
	if !ok {
		return
	}
	if len(ce.Args) == 0 {
		ce.Reply("**Usage:** `group-add <phone number or Apple ID email>`")
		return
	}
	localID, err := resolveParticipantLocalID(ce, ce.RawArgs)
	if err != nil {
		ce.Reply("Invalid participant: %s", err)
		return
	}
	if err := groupAdmin.AddGroupParticipant(ce.Portal.GUID, localID); err != nil {
		ce.Reply("Failed to add `%s` to group: %s", localID, err)
		return
	}
	puppet := ce.Bridge.GetPuppetByLocalID(localID)
	puppet.Sync()
	if err := puppet.Intent.EnsureJoined(ce.Portal.MXID); err != nil {
		ce.Bridge.ZLog.Warn().Err(err).Str("puppet", puppet.MXID.String()).Msg("Failed to join added participant puppet")
	}
	ce.Reply("Added `%s` to the group.", localID)
}

var cmdGroupRemove = &commands.FullHandler{
	Func: wrapCommand(fnGroupRemove),
	Name: "group-remove",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Removes a participant (phone number or Apple ID email) from the current iMessage group chat.",
		Args:        "<_phone or email_>",
	},
	RequiresPortal: true,
	RequiresLogin:  false,
}

func fnGroupRemove(ce *WrappedCommandEvent) {
	groupAdmin, ok := requirePortalGroupAdmin(ce)
	if !ok {
		return
	}
	if len(ce.Args) == 0 {
		ce.Reply("**Usage:** `group-remove <phone number or Apple ID email>`")
		return
	}
	localID, err := resolveParticipantLocalID(ce, ce.RawArgs)
	if err != nil {
		ce.Reply("Invalid participant: %s", err)
		return
	}
	if err := groupAdmin.RemoveGroupParticipant(ce.Portal.GUID, localID); err != nil {
		ce.Reply("Failed to remove `%s` from group: %s", localID, err)
		return
	}
	puppet := ce.Bridge.GetPuppetByLocalID(localID)
	_, _ = ce.Portal.MainIntent().KickUser(ce.Portal.MXID, &mautrix.ReqKickUser{
		UserID: puppet.MXID,
		Reason: "Removed from iMessage group",
	})
	ce.Reply("Removed `%s` from the group.", localID)
}

var cmdGroupLeave = &commands.FullHandler{
	Func: wrapCommand(fnGroupLeave),
	Name: "group-leave",
	Help: commands.HelpMeta{
		Section:     HelpSectionManagingPortals,
		Description: "Leaves the current iMessage group chat.",
	},
	RequiresPortal: true,
	RequiresLogin:  false,
}

func fnGroupLeave(ce *WrappedCommandEvent) {
	groupAdmin, ok := requirePortalGroupAdmin(ce)
	if !ok {
		return
	}
	if err := groupAdmin.LeaveGroup(ce.Portal.GUID); err != nil {
		ce.Reply("Failed to leave group: %s", err)
		return
	}
	ce.Reply("Left the iMessage group.")
}

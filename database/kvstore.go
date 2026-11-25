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

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "maunium.net/go/maulogger/v2"
)

type KeyValueQuery struct {
	db  *Database
	log log.Logger
}

const (
	KVSendStatusStart       = "com.beeper.send_status_start"
	KVBridgeWasConnected    = "bridge_was_connected"
	KVBridgeFirstConnect    = "bridge_first_connect"
	KVBridgeInfoVersion     = "bridge_info_version"
	KVLookedForPortals      = "looked_for_portals"
	KVDoublePuppetLoggedOut = "double_puppet_logged_out"
	KVPortalServicePrefix   = "portal_service:"

	ExpectedBridgeInfoVersion = "1"
)

func (kvq *KeyValueQuery) Get(key string) (value string) {
	err := kvq.db.QueryRow("SELECT value FROM kv_store WHERE key=$1", key).Scan(&value)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		panic(fmt.Errorf("failed to scan value for %s: %w", key, err))
	}
	return
}

func (kvq *KeyValueQuery) Set(key, value string) {
	_, err := kvq.db.Exec(`
		INSERT INTO kv_store (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value=excluded.value
	`, key, value)
	if err != nil {
		panic(fmt.Errorf("failed to insert %s: %w", key, err))
	}
}

func (kvq *KeyValueQuery) Delete(key string) {
	_, err := kvq.db.Exec("DELETE FROM kv_store WHERE key=$1", key)
	if err != nil {
		panic(fmt.Errorf("failed to delete %s: %w", key, err))
	}
}

func escapeLikePrefix(prefix string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(prefix) + "%"
}

// DeletePrefixOlderThan deletes all kv_store rows whose key starts with prefix
// and whose value parses as a base-10 int64 strictly less than cutoff.
func (kvq *KeyValueQuery) DeletePrefixOlderThan(prefix string, cutoff int64) (int64, error) {
	if prefix == "" {
		return 0, errors.New("prefix must not be empty")
	}

	rows, err := kvq.db.Query("SELECT key, value FROM kv_store WHERE key LIKE $1 ESCAPE '\\'", escapeLikePrefix(prefix))
	if err != nil {
		return 0, fmt.Errorf("failed to query kv_store for prefix %s: %w", prefix, err)
	}

	var expiredKeys []string
	for rows.Next() {
		var key, raw string
		if err := rows.Scan(&key, &raw); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("failed to scan kv_store row for prefix %s: %w", prefix, err)
		}
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		ts, parseErr := strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			continue
		}
		if ts < cutoff {
			expiredKeys = append(expiredKeys, key)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("failed iterating kv_store rows for prefix %s: %w", prefix, err)
	}
	_ = rows.Close()

	if len(expiredKeys) == 0 {
		return 0, nil
	}

	const batchSize = 200
	var deleted int64
	for start := 0; start < len(expiredKeys); start += batchSize {
		end := start + batchSize
		if end > len(expiredKeys) {
			end = len(expiredKeys)
		}
		batch := expiredKeys[start:end]
		placeholders := make([]string, len(batch))
		args := make([]any, len(batch))
		for i, key := range batch {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = key
		}
		res, execErr := kvq.db.Exec(
			fmt.Sprintf("DELETE FROM kv_store WHERE key IN (%s)", strings.Join(placeholders, ", ")),
			args...,
		)
		if execErr != nil {
			return deleted, fmt.Errorf("failed to delete expired kv_store rows for prefix %s: %w", prefix, execErr)
		}
		if n, raErr := res.RowsAffected(); raErr == nil {
			deleted += n
		} else {
			deleted += int64(len(batch))
		}
	}
	return deleted, nil
}

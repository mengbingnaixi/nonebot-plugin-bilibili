from __future__ import annotations

import json
import time
from typing import Any
from pathlib import Path
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aiosqlite

from .models import AtAllKind, Subscription
from .paths import DATABASE_PATH, ensure_data_dirs
from .rules import validate_regex, normalize_atall_switches

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    avatar TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    group_id INTEGER NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    uid INTEGER NOT NULL,
    uname TEXT NOT NULL DEFAULT '',
    avatar TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, uid)
);

CREATE TABLE IF NOT EXISTS filters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    pattern TEXT NOT NULL,
    UNIQUE (group_id, pattern)
);

CREATE TABLE IF NOT EXISTS atall (
    group_id INTEGER NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    uid INTEGER NOT NULL,
    kind TEXT NOT NULL,
    PRIMARY KEY (group_id, uid, kind)
);

CREATE TABLE IF NOT EXISTS overrides (
    group_id INTEGER NOT NULL REFERENCES groups(group_id) ON DELETE CASCADE,
    uid INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL,
    template_name TEXT,
    font_name TEXT,
    color TEXT,
    gradient_color TEXT,
    PRIMARY KEY (group_id, uid, kind)
);

CREATE TABLE IF NOT EXISTS states (
    namespace TEXT NOT NULL,
    state_key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (namespace, state_key)
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_receipts (
    namespace TEXT NOT NULL,
    uid INTEGER NOT NULL,
    content_key TEXT NOT NULL,
    group_id INTEGER NOT NULL,
    delivered_at INTEGER NOT NULL,
    PRIMARY KEY (namespace, uid, content_key, group_id)
);

CREATE TABLE IF NOT EXISTS delivery_claims (
    namespace TEXT NOT NULL,
    uid INTEGER NOT NULL,
    content_key TEXT NOT NULL,
    group_id INTEGER NOT NULL,
    claimed_at INTEGER NOT NULL,
    PRIMARY KEY (namespace, uid, content_key, group_id)
);
"""

DELIVERY_CLAIM_TTL = 300


class Repository:
    def __init__(self, path: Path = DATABASE_PATH) -> None:
        self.path = path

    async def initialize(self) -> None:
        if self.path == DATABASE_PATH:
            ensure_data_dirs()
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            columns = {
                str(row[1])
                for row in await (await db.execute("PRAGMA table_info(overrides)")).fetchall()
            }
            if "color" not in columns:
                await db.execute("ALTER TABLE overrides ADD COLUMN color TEXT")
            if "gradient_color" not in columns:
                await db.execute("ALTER TABLE overrides ADD COLUMN gradient_color TEXT")
            await self._migrate_render_kinds(db)
            await db.execute("DELETE FROM overrides WHERE uid<>0")
            await db.commit()

    @staticmethod
    async def _migrate_render_kinds(db: aiosqlite.Connection) -> None:
        rows = await (
            await db.execute("""SELECT group_id, uid, kind, template_name, font_name, color,
                gradient_color
                FROM overrides WHERE kind IN ('all', 'music', 'article')""")
        ).fetchall()
        for row in rows:
            targets = ("dynamic", "video", "live") if row[2] == "all" else ("dynamic",)
            for target in targets:
                await db.execute(
                    """INSERT INTO overrides(
                        group_id, uid, kind, template_name, font_name, color,
                        gradient_color
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(group_id, uid, kind) DO UPDATE SET
                        template_name=COALESCE(overrides.template_name, excluded.template_name),
                        font_name=COALESCE(overrides.font_name, excluded.font_name),
                        color=COALESCE(overrides.color, excluded.color),
                        gradient_color=COALESCE(
                            overrides.gradient_color, excluded.gradient_color
                        )""",
                    (row[0], row[1], target, row[3], row[4], row[5], row[6]),
                )
        await db.execute("DELETE FROM overrides WHERE kind IN ('all', 'music', 'article')")

    @asynccontextmanager
    async def _connect(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(self.path)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys=ON")
        try:
            yield db
        finally:
            await db.close()

    async def upsert_group(self, group_id: int, name: str = "", avatar: str = "") -> None:
        now = int(time.time())
        async with self._connect() as db:
            await db.execute(
                """INSERT INTO groups(group_id, name, avatar, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    name=CASE WHEN excluded.name='' THEN groups.name ELSE excluded.name END,
                    avatar=CASE WHEN excluded.avatar='' THEN groups.avatar
                                ELSE excluded.avatar END""",
                (group_id, name, avatar, now),
            )
            await db.commit()

    async def add_subscription(
        self,
        group_id: int,
        uid: int,
        uname: str = "",
        avatar: str = "",
        group_name: str = "",
    ) -> bool:
        await self.upsert_group(
            group_id,
            group_name,
            f"https://p.qlogo.cn/gh/{group_id}/{group_id}/100",
        )
        async with self._connect() as db:
            cursor = await db.execute(
                "INSERT OR IGNORE INTO subscriptions VALUES (?, ?, ?, ?, ?)",
                (group_id, uid, uname, avatar, int(time.time())),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def remove_subscription(self, group_id: int, uid: int) -> bool:
        async with self._connect() as db:
            cursor = await db.execute(
                "DELETE FROM subscriptions WHERE group_id=? AND uid=?", (group_id, uid)
            )
            await db.execute("DELETE FROM atall WHERE group_id=? AND uid=?", (group_id, uid))
            await db.execute("DELETE FROM overrides WHERE group_id=? AND uid=?", (group_id, uid))
            await self._clear_orphaned_uid_state(db, {uid})
            await db.commit()
            return cursor.rowcount > 0

    async def clear_group(self, group_id: int) -> int:
        async with self._connect() as db:
            rows = await (
                await db.execute("SELECT uid FROM subscriptions WHERE group_id=?", (group_id,))
            ).fetchall()
            affected_uids = {int(row[0]) for row in rows}
            cursor = await db.execute("DELETE FROM subscriptions WHERE group_id=?", (group_id,))
            await db.execute("DELETE FROM atall WHERE group_id=?", (group_id,))
            await db.execute("DELETE FROM overrides WHERE group_id=?", (group_id,))
            await self._clear_orphaned_uid_state(db, affected_uids)
            await db.commit()
            return cursor.rowcount

    async def clear_all(self) -> int:
        async with self._connect() as db:
            cursor = await db.execute("DELETE FROM subscriptions")
            await db.execute("DELETE FROM atall")
            await db.execute("DELETE FROM overrides")
            await db.execute("DELETE FROM states WHERE namespace IN ('dynamic', 'live')")
            await db.execute("DELETE FROM delivery_receipts")
            await db.execute("DELETE FROM delivery_claims")
            await db.commit()
            return cursor.rowcount

    @staticmethod
    async def _clear_orphaned_uid_state(db: aiosqlite.Connection, uids: set[int]) -> None:
        for uid in uids:
            remaining = await (
                await db.execute("SELECT 1 FROM subscriptions WHERE uid=? LIMIT 1", (uid,))
            ).fetchone()
            if remaining is not None:
                continue
            await db.execute(
                "DELETE FROM states WHERE namespace IN ('dynamic', 'live') AND state_key=?",
                (str(uid),),
            )
            await db.execute("DELETE FROM delivery_receipts WHERE uid=?", (uid,))
            await db.execute("DELETE FROM delivery_claims WHERE uid=?", (uid,))

    async def list_subscriptions(self, group_id: int | None = None) -> list[Subscription]:
        query = """SELECT s.group_id, s.uid, s.uname, s.avatar,
                   g.name AS group_name, g.avatar AS group_avatar
                   FROM subscriptions s JOIN groups g USING(group_id)"""
        params: tuple[int, ...] = ()
        if group_id is not None:
            query += " WHERE s.group_id=?"
            params = (group_id,)
        query += " ORDER BY s.group_id, s.uid"
        async with self._connect() as db:
            rows = await (await db.execute(query, params)).fetchall()
        return [Subscription(**dict(row)) for row in rows]

    async def list_uids(self) -> list[int]:
        async with self._connect() as db:
            rows = await (
                await db.execute("SELECT DISTINCT uid FROM subscriptions ORDER BY uid")
            ).fetchall()
        return [int(row[0]) for row in rows]

    async def get_uname(self, uid: int) -> str:
        # Return the UP name saved in subscriptions, empty string if not found
        async with self._connect() as db:
            row = await (
                await db.execute(
                    "SELECT uname FROM subscriptions "
                    "WHERE uid=? AND uname != '' LIMIT 1",
                    (uid,),
                )
            ).fetchone()
        return str(row[0]) if row else ""

    async def groups_for_uid(self, uid: int) -> list[Subscription]:
        subscriptions = await self.list_subscriptions()
        return [item for item in subscriptions if item.uid == uid]

    async def add_filter(self, group_id: int, pattern: str) -> bool:
        validate_regex(pattern)
        await self.upsert_group(group_id)
        async with self._connect() as db:
            cursor = await db.execute(
                "INSERT OR IGNORE INTO filters(group_id, pattern) VALUES (?, ?)",
                (group_id, pattern),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def list_filters(self, group_id: int) -> list[tuple[int, str]]:
        async with self._connect() as db:
            rows = await (
                await db.execute(
                    "SELECT id, pattern FROM filters WHERE group_id=? ORDER BY id", (group_id,)
                )
            ).fetchall()
        return [(int(row["id"]), str(row["pattern"])) for row in rows]

    async def remove_filter(self, group_id: int, selector: str) -> bool:
        async with self._connect() as db:
            if selector.isdigit():
                cursor = await db.execute(
                    "SELECT id FROM filters WHERE group_id=? ORDER BY id", (group_id,)
                )
                rows = list(await cursor.fetchall())
                index = int(selector) - 1
                if index < 0 or index >= len(rows):
                    return False
                cursor = await db.execute(
                    "DELETE FROM filters WHERE group_id=? AND id=?",
                    (group_id, int(rows[index]["id"])),
                )
            else:
                cursor = await db.execute(
                    "DELETE FROM filters WHERE group_id=? AND pattern=?", (group_id, selector)
                )
            await db.commit()
            return cursor.rowcount > 0

    async def remove_filter_exact(self, group_id: int, pattern: str) -> bool:
        async with self._connect() as db:
            cursor = await db.execute(
                "DELETE FROM filters WHERE group_id=? AND pattern=?", (group_id, pattern)
            )
            await db.commit()
            return cursor.rowcount > 0

    async def set_atall(self, group_id: int, uid: int, kind: AtAllKind, enabled: bool) -> set[str]:
        await self.upsert_group(group_id)
        current = await self.get_atall(group_id, uid)
        updated = normalize_atall_switches(current, kind, enabled)
        async with self._connect() as db:
            await db.execute("DELETE FROM atall WHERE group_id=? AND uid=?", (group_id, uid))
            await db.executemany(
                "INSERT INTO atall(group_id, uid, kind) VALUES (?, ?, ?)",
                [(group_id, uid, item) for item in sorted(updated)],
            )
            await db.commit()
        return updated

    async def get_atall(self, group_id: int, uid: int) -> set[str]:
        async with self._connect() as db:
            rows = await (
                await db.execute(
                    "SELECT kind FROM atall WHERE group_id=? AND uid=?", (group_id, uid)
                )
            ).fetchall()
        return {str(row[0]) for row in rows}

    async def list_atall(self, group_id: int) -> dict[int, set[str]]:
        async with self._connect() as db:
            rows = await (
                await db.execute(
                    "SELECT uid, kind FROM atall WHERE group_id=? ORDER BY uid, kind", (group_id,)
                )
            ).fetchall()
        result: dict[int, set[str]] = {}
        for row in rows:
            result.setdefault(int(row["uid"]), set()).add(str(row["kind"]))
        return result

    async def set_override(
        self,
        group_id: int,
        kind: str,
        template_name: str | None = None,
        font_name: str | None = None,
        color: str | None = None,
        gradient_color: str | None = None,
    ) -> None:
        await self.upsert_group(group_id)
        async with self._connect() as db:
            if (
                template_name is None
                and font_name is None
                and color is None
                and gradient_color is None
            ):
                await db.execute(
                    "DELETE FROM overrides WHERE group_id=? AND uid=? AND kind=?",
                    (group_id, 0, kind),
                )
            else:
                await db.execute(
                    """INSERT INTO overrides(
                        group_id, uid, kind, template_name, font_name, color,
                        gradient_color
                    ) VALUES (?, 0, ?, ?, ?, ?, ?)
                    ON CONFLICT(group_id, uid, kind) DO UPDATE SET
                        template_name=excluded.template_name,
                        font_name=excluded.font_name,
                        color=excluded.color,
                        gradient_color=excluded.gradient_color""",
                    (
                        group_id,
                        kind,
                        template_name,
                        font_name,
                        color,
                        gradient_color,
                    ),
                )
            await db.commit()

    async def resolve_override(
        self, group_id: int, kind: str
    ) -> tuple[str | None, str | None, str | None, str | None]:
        async with self._connect() as db:
            row = await (
                await db.execute(
                    """SELECT template_name, font_name, color, gradient_color
                    FROM overrides WHERE group_id=? AND uid=0 AND kind=?""",
                    (group_id, kind),
                )
            ).fetchone()
        if not row:
            return None, None, None, None
        return row["template_name"], row["font_name"], row["color"], row["gradient_color"]

    async def list_overrides(self, group_id: int) -> list[dict[str, object]]:
        async with self._connect() as db:
            rows = await (
                await db.execute(
                    """SELECT uid, kind, template_name, font_name, color, gradient_color
                    FROM overrides
                    WHERE group_id=? ORDER BY uid, kind""",
                    (group_id,),
                )
            ).fetchall()
        return [dict(row) for row in rows]

    async def get_state(self, namespace: str, state_key: str) -> str | None:
        async with self._connect() as db:
            row = await (
                await db.execute(
                    "SELECT value FROM states WHERE namespace=? AND state_key=?",
                    (namespace, state_key),
                )
            ).fetchone()
        return str(row[0]) if row else None

    async def set_state(self, namespace: str, state_key: str, value: str) -> None:
        async with self._connect() as db:
            await db.execute(
                """INSERT INTO states(namespace, state_key, value, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(namespace, state_key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at""",
                (namespace, state_key, value, int(time.time())),
            )
            await db.commit()

    async def get_setting(self, key: str, default: str = "") -> str:
        async with self._connect() as db:
            row = await (
                await db.execute("SELECT value FROM settings WHERE key=?", (key,))
            ).fetchone()
        return str(row[0]) if row else default

    async def set_setting(self, key: str, value: str) -> None:
        async with self._connect() as db:
            await db.execute(
                """INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at""",
                (key, value, int(time.time())),
            )
            await db.commit()

    async def has_delivery_receipt(
        self, namespace: str, uid: int, content_key: str, group_id: int
    ) -> bool:
        async with self._connect() as db:
            row = await (
                await db.execute(
                    """SELECT 1 FROM delivery_receipts
                    WHERE namespace=? AND uid=? AND content_key=? AND group_id=?""",
                    (namespace, uid, content_key, group_id),
                )
            ).fetchone()
        return row is not None

    async def set_delivery_receipt(
        self, namespace: str, uid: int, content_key: str, group_id: int
    ) -> None:
        async with self._connect() as db:
            await db.execute(
                """INSERT INTO delivery_receipts(
                    namespace, uid, content_key, group_id, delivered_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(namespace, uid, content_key, group_id) DO UPDATE SET
                    delivered_at=excluded.delivered_at""",
                (namespace, uid, content_key, group_id, int(time.time())),
            )
            await db.commit()

    async def claim_delivery(
        self, namespace: str, uid: int, content_key: str, group_id: int
    ) -> bool:
        """Atomically reserve one delivery across concurrent jobs or processes."""
        now = int(time.time())
        async with self._connect() as db:
            await db.execute(
                """DELETE FROM delivery_claims
                WHERE namespace=? AND uid=? AND content_key=? AND group_id=?
                  AND claimed_at<?""",
                (
                    namespace,
                    uid,
                    content_key,
                    group_id,
                    now - DELIVERY_CLAIM_TTL,
                ),
            )
            cursor = await db.execute(
                """INSERT OR IGNORE INTO delivery_claims(
                    namespace, uid, content_key, group_id, claimed_at
                ) VALUES (?, ?, ?, ?, ?)""",
                (namespace, uid, content_key, group_id, now),
            )
            await db.commit()
            return cursor.rowcount == 1

    async def release_delivery_claim(
        self, namespace: str, uid: int, content_key: str, group_id: int
    ) -> None:
        async with self._connect() as db:
            await db.execute(
                """DELETE FROM delivery_claims
                WHERE namespace=? AND uid=? AND content_key=? AND group_id=?""",
                (namespace, uid, content_key, group_id),
            )
            await db.commit()

    async def prune_delivery_receipts(
        self, namespace: str, uid: int, keep_content_key: str
    ) -> None:
        async with self._connect() as db:
            await db.execute(
                """DELETE FROM delivery_receipts
                WHERE namespace=? AND uid=? AND content_key<>?""",
                (namespace, uid, keep_content_key),
            )
            await db.commit()

    async def export_snapshot(self) -> dict[str, object]:
        subscriptions = await self.list_subscriptions()
        groups: dict[int, dict[str, Any]] = {}
        for item in subscriptions:
            group = groups.setdefault(
                item.group_id,
                {
                    "group_id": item.group_id,
                    "name": item.group_name,
                    "avatar": item.group_avatar,
                    "subscriptions": [],
                },
            )
            group["subscriptions"].append(
                {"uid": item.uid, "uname": item.uname, "avatar": item.avatar}
            )
        return {"groups": list(groups.values())}

    async def set_json_state(self, namespace: str, state_key: str, value: object) -> None:
        await self.set_state(namespace, state_key, json.dumps(value, ensure_ascii=False))

    async def get_json_state(self, namespace: str, state_key: str) -> object | None:
        raw = await self.get_state(namespace, state_key)
        return json.loads(raw) if raw is not None else None


repository = Repository()

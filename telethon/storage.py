import inspect
import sqlite3
import logging
import os
import time
from pathlib import Path
from typing import Any, List, Tuple

from pyrogram import Client, raw, utils
from pyrogram.storage import Storage

log = logging.getLogger(__name__)

# language=SQLite
SCHEMA = """
CREATE TABLE sessions
(
    dc_id           INTEGER PRIMARY KEY,
    server_address  TEXT,
    port            INTEGER,
    auth_key        BLOB,
    takeout_id      INTEGER
);

CREATE TABLE entities
(
    id             INTEGER PRIMARY KEY,
    hash           INTEGER NOT NULL,
    username       TEXT,
    phone          INTEGER,
    name           TEXT,
    date           INTEGER
);

CREATE TABLE sent_files
(
    md5_digest  BLOB,
    file_size   INTEGER,
    type        INTEGER,
    id          INTEGER,
    hash        INTEGER,
    PRIMARY KEY(md5_digest, file_size, type)
);

CREATE TABLE update_state
(
    id      INTEGER PRIMARY KEY,
    pts     INTEGER,
    qts     INTEGER,
    date    INTEGER,
    seq     INTEGER
);

CREATE TABLE version
(
    version INTEGER PRIMARY KEY
);
"""


def get_input_peer(peer_id: int, access_hash: int):
    if peer_id >= 0:
        return raw.types.InputPeerUser(
            user_id=peer_id,
            access_hash=access_hash
        )

    if peer_id <= -1000000000000:
        return raw.types.InputPeerChannel(
            channel_id=utils.get_channel_id(peer_id),
            access_hash=access_hash
        )

    if peer_id < 0:
        return raw.types.InputPeerChat(
            chat_id=-peer_id
        )

    raise ValueError("Invalid peer type")


class TelethonStorage(Storage):
    FILE_EXTENSION = ".session"
    VERSION = 7
    USERNAME_TTL = 8 * 60 * 60

    def __init__(self, *, client: Client):
        super().__init__(client.name)

        self._api_id = client.api_id
        self._test_mode = client.test_mode
        self._is_bot = client.bot_token is not None

        self.database = client.workdir / (client.name + self.FILE_EXTENSION)

    def create(self):
        with self.conn:
            self.conn.executescript(SCHEMA)

            self.conn.execute(
                "INSERT INTO version VALUES (?)",
                (self.VERSION,)
            )

            self.conn.execute(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
                (2, "149.154.167.51", 443, None, 0)
            )

    def update(self):
        version = self.version()

        if version == 1:
            version += 1
            # version == 1 doesn't have the old sent_files so no need to drop

        if version == 2:
            version += 1
            # Old cache from old sent_files lasts then a day anyway, drop

            with self.conn:
                self.conn.execute("ALTER TABLE sessions ADD api_id INTEGER")
                self.conn.execute("""CREATE TABLE sent_files (
                    md5_digest  BLOB,
                    file_size   INTEGER,
                    type        INTEGER,
                    id          INTEGER,
                    hash        INTEGER,
                    PRIMARY KEY(md5_digest, file_size, type)
                )""")

        if version == 3:
            version += 1

            with self.conn:
                self.conn.execute("""CREATE TABLE update_state (
                    id      INTEGER PRIMARY KEY,
                    pts     INTEGER,
                    qts     INTEGER,
                    date    INTEGER,
                    seq     INTEGER
                )""")

        if version == 4:
            version += 1

            with self.conn:
                self.conn.execute("ALTER TABLE sessions ADD COLUMN takeout_id integer")

        if version == 5:
            version += 1
            # Not really any schema upgrade, but potentially all access
            # hashes for User and Channel are wrong, so drop them off.

            with self.conn:
                self.conn.execute("DELETE FROM entities")

        if version == 6:
            version += 1

            with self.conn:
                self.conn.execute("ALTER TABLE entities ADD COLUMN date integer")

        self.version(version)

    async def open(self):
        path = self.database
        file_exists = path.is_file()

        self.conn = sqlite3.connect(str(path), timeout=1, check_same_thread=False)

        if not file_exists:
            self.create()
        else:
            self.update()

        with self.conn:
            self.conn.execute("VACUUM")

    async def save(self):
        await self.date(int(time.time()))
        self.conn.commit()

    async def close(self):
        self.conn.close()

    async def delete(self):
        os.remove(self.database)

    async def update_peers(self, peers: List[Tuple[int, int, str, str]]):
        values = []

        for peer_data in peers:
            id, hash, type, phone = peer_data
            values.append((id, hash, phone, None, int(time.time())))

        self.conn.executemany(
            "REPLACE INTO entities (id, hash, phone, name, date)"
            "VALUES (?, ?, ?, ?, ?)",
            values
        )

    async def update_usernames(self, usernames: List[Tuple[int, List[str]]]):
        self.conn.executemany(
            "REPLACE INTO entities (id, username) VALUES (?, ?)",
            [(id, username) for id, usernames in usernames for username in usernames]
        )

    async def update_state(self, value: Tuple[int, int, int, int, int] = object):
        # TODO
        return None

    async def get_peer_by_id(self, peer_id: int):
        r = self.conn.execute(
            "SELECT id, hash FROM entities WHERE id = ?",
            (peer_id,)
        ).fetchone()

        if r is None:
            raise KeyError(f"ID not found: {peer_id}")

        return get_input_peer(*r)

    async def get_peer_by_username(self, username: str):
        r = self.conn.execute(
            "SELECT id, hash, date FROM entities WHERE username = ?"
            "ORDER BY date DESC",
            (username,)
        ).fetchone()

        if r is None:
            raise KeyError(f"Username not found: {username}")

        if abs(time.time() - r[2]) > self.USERNAME_TTL:
            raise KeyError(f"Username expired: {username}")

        return get_input_peer(*r[:2])

    async def get_peer_by_phone_number(self, phone_number: str):
        r = self.conn.execute(
            "SELECT id, hash FROM entities WHERE phone = ?",
            (phone_number,)
        ).fetchone()

        if r is None:
            raise KeyError(f"Phone number not found: {phone_number}")

        return get_input_peer(*r)

    def _get(self):
        attr = inspect.stack()[2].function

        return self.conn.execute(
            f"SELECT {attr} FROM sessions"
        ).fetchone()[0]

    def _set(self, value: Any):
        attr = inspect.stack()[2].function

        with self.conn:
            self.conn.execute(
                f"UPDATE sessions SET {attr} = ?",
                (value,)
            )

    def _accessor(self, value: Any = object):
        return self._get() if value == object else self._set(value)

    async def dc_id(self, value: int = object):
        return self._accessor(value)

    async def api_id(self, value: int = object):
        if value != object:
            self._api_id = value

        return self._api_id

    async def test_mode(self, value: bool = object):
        if value != object:
            self._test_mode = value

        return self._test_mode

    async def auth_key(self, value: bytes = object):
        return self._accessor(value)

    async def date(self, value: int = object):
        if value == object:
            cur = self.conn.execute(
                "SELECT date FROM entities WHERE id=0"
            )

            res = cur.fetchone()

            return None if res is None else res[0]
        else:
            with self.conn:
                self.conn.execute(
                    "UPDATE entities SET date = ? WHERE id=0",
                    (value,)
                )

    async def user_id(self, value: int = object):
        if value == object:
            cur = self.conn.execute(
                "SELECT hash FROM entities WHERE id=0"
            )

            res = cur.fetchone()

            return 0 if res is None else res[0]
        else:
            if value is None:
                return

            with self.conn:
                self.conn.execute(
                    "REPLACE INTO entities VALUES (?, ?, ?, ?, ?, ?)",
                    (0, value, None, None, None, int(time.time()))
                )

    async def is_bot(self, value: bool = object):
        if value != object:
            self._is_bot = value

        return self._is_bot

    def version(self, value: int = object):
        if value == object:
            return self.conn.execute(
                "SELECT version FROM version"
            ).fetchone()[0]
        else:
            with self.conn:
                self.conn.execute(
                    "UPDATE version SET version = ?",
                    (value,)
                )

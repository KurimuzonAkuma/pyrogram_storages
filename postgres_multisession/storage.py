import asyncio
import inspect
import time
from typing import List, Tuple, Any

from asyncpg.pool import Pool
from pyrogram import Client
from pyrogram.storage import Storage
from pyrogram.storage.sqlite_storage import get_input_peer

# language=SQLite
SCHEMA = """
CREATE TABLE IF NOT EXISTS public.sessions (
    session_name TEXT PRIMARY KEY,
    dc_id        INTEGER,
    api_id       INTEGER,
    test_mode    INTEGER,
    auth_key     BYTEA,
    date         INTEGER NOT NULL,
    user_id      BIGINT,
    is_bot       BOOL
);

CREATE TABLE IF NOT EXISTS public.peers (
    session_name TEXT NOT NULL,
    id           BIGINT NOT NULL,
    access_hash  BIGINT NOT NULL,
    type         TEXT NOT NULL,
    phone_number TEXT,
    last_update_on BIGINT,
    PRIMARY KEY(session_name, id),
    FOREIGN KEY(session_name) REFERENCES public.sessions(session_name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.usernames (
    session_name TEXT NOT NULL,
    id           BIGINT NOT NULL,
    username     TEXT NOT NULL,
    FOREIGN KEY(session_name, id) REFERENCES public.peers(session_name, id) ON DELETE CASCADE,
    UNIQUE (session_name, id, username)
);

CREATE TABLE IF NOT EXISTS public.version (
    number INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS public.update_state (
    id           INTEGER PRIMARY KEY,
    session_name TEXT NOT NULL,
    pts          INTEGER,
    qts          INTEGER,
    date         INTEGER,
    seq          INTEGER,
    FOREIGN KEY(session_name) REFERENCES public.sessions(session_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_update_state_session_name ON public.update_state (session_name);
CREATE INDEX IF NOT EXISTS idx_usernames_id ON public.usernames (id);
CREATE INDEX IF NOT EXISTS idx_usernames_username ON public.usernames (username);
CREATE INDEX IF NOT EXISTS idx_peers_phone_number ON public.peers(phone_number);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'trg_peers_last_update_on'
    ) THEN
        CREATE TRIGGER trg_peers_last_update_on
        BEFORE UPDATE ON public.peers
        FOR EACH ROW
        EXECUTE FUNCTION update_last_update_on();
    END IF;
END $$;
"""


class MultiPostgresStorage(Storage):
    VERSION = 1
    USERNAME_TTL = 8 * 60 * 60

    def __init__(self, client: Client, pool: Pool):
        super().__init__(client.name)

        self.pool = pool
        self.lock = asyncio.Lock()
        self.name = client.name

    async def create(self):
        async with self.lock, self.pool.acquire() as con:
            session_exists = await con.fetchval(
                "SELECT 1 FROM public.sessions WHERE session_name = $1",
                self.name
            )

            if session_exists:
                pass
            else:
                await con.execute(
                    """INSERT INTO public.sessions
                       (session_name, dc_id, api_id, test_mode, auth_key, date, user_id, is_bot)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
                    self.name, 2, None, None, None, int(time.time()), None, None,
                )

    async def open(self):
        async with self.pool.acquire() as con:
            await con.execute(SCHEMA)
            session_exists = await con.fetchval(
                "SELECT 1 FROM public.sessions WHERE session_name = $1",
                self.name
            )
            if session_exists:
                pass
            else:
                await self.create()

    async def save(self):
        await self.date(int(time.time()))

    async def close(self):
        pass

    async def delete(self):
        async with self.pool.acquire() as con:
            await con.execute('DROP TABLE IF EXISTS public.sessions')
            await con.execute('DROP TABLE IF EXISTS public.peers')
            await con.execute('DROP TABLE IF EXISTS public.version')

    async def update_peers(self, peers: List[Tuple[int, int, str, str]]):
        async with self.pool.acquire() as con:
            await con.executemany(
                """
                INSERT INTO public.peers (session_name, id, access_hash, type, phone_number)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (session_name, id)
                DO UPDATE SET
                    access_hash = EXCLUDED.access_hash,
                    type = EXCLUDED.type,
                    phone_number = EXCLUDED.phone_number
                """,
                [(self.name, peer[0], peer[1], str(peer[2]), peer[3]) for peer in peers]
            )

    async def update_usernames(self, usernames: List[Tuple[int, List[str]]]):
        async with self.pool.acquire() as con:
            ids_to_delete = [(self.name, id) for id, _ in usernames]

            if ids_to_delete:
                await con.executemany(
                    """DELETE FROM public.usernames
                       WHERE session_name = $1 AND id = $2""",
                    ids_to_delete
                )

            for id, user_list in usernames:
                await con.executemany(
                    """INSERT INTO public.usernames (session_name, id, username)
                       VALUES ($1, $2, $3)
                       ON CONFLICT (session_name, id, username) DO NOTHING""",
                    [(self.name, id, username) for username in user_list]
                )

    async def get_peer_by_id(self, peer_id: int):
        if not isinstance(peer_id, int):
            raise KeyError(f"ID not int: {peer_id}")

        async with self.pool.acquire() as con:
            r = await con.fetchrow(
                """SELECT id, access_hash, type
                   FROM public.peers
                   WHERE session_name = $1 AND id = $2""",
                self.name, peer_id,
            )

        if r is None:
            raise KeyError(f"ID not found: {peer_id}")

        return get_input_peer(r['id'], r['access_hash'], r['type'])

    async def get_peer_by_username(self, username: str):
        async with self.pool.acquire() as con:
            r = await con.fetchrow(
                """SELECT p.id, p.access_hash, p.type, p.last_update_on
                   FROM public.peers p
                   JOIN public.usernames u ON p.id = u.id
                   WHERE u.username = $1 AND u.session_name = $2
                   ORDER BY p.last_update_on DESC""",
                username, self.name
            )

        if r is None:
            raise KeyError(f"Username not found: {username}")

        if abs(time.time() - r['last_update_on']) > self.USERNAME_TTL:
            raise KeyError(f"Username expired: {username}")

        return get_input_peer(r['id'], r['access_hash'], r['type'])

    async def update_state(self, value: Tuple[int, int, int, int, int] = object):
        async with self.pool.acquire() as con:
            if value == object:
                r = await con.fetch(
                    "SELECT id, pts, qts, date, seq FROM public.update_state "
                    "WHERE session_name = $1 ORDER BY date ASC",
                    self.name
                )
                return r
            else:
                if isinstance(value, int):
                    await con.execute(
                        "DELETE FROM public.update_state WHERE session_name = $1 AND id = $2",
                        self.name, value
                    )
                else:
                    await con.execute(
                        """REPLACE INTO public.update_state (id, session_name, pts, qts, date, seq)
                           VALUES ($1, $2, $3, $4, $5, $6)""",
                        value[0], self.name, value[1], value[2], value[3], value[4]
                    )

    async def get_peer_by_phone_number(self, phone_number: str):
        async with self.pool.acquire() as con:
            r = await con.fetchrow(
                """SELECT id, access_hash, type
                   FROM public.peers
                   WHERE session_name = $1 AND phone_number = $2""",
                self.name, phone_number,
            )

        if r is None:
            raise KeyError(f"Phone number not found: {phone_number}")

        return get_input_peer(*r)

    async def _get(self):
        attr = inspect.stack()[2].function

        async with self.pool.acquire() as con:
            return await con.fetchval(
                f'SELECT {attr} FROM public.sessions WHERE session_name = $1',
                self.name
            )

    async def _set(self, value: Any):
        attr = inspect.stack()[2].function

        async with self.lock, self.pool.acquire() as con:
            await con.execute(
                f'UPDATE public.sessions SET {attr} = $1 WHERE session_name = $2',
                value, self.name
            )

    async def _accessor(self, value: Any = object):
        return await self._get() if value == object else await self._set(value)

    async def dc_id(self, value: int = object):
        return await self._accessor(value)

    async def api_id(self, value: int = object):
        return await self._accessor(value)

    async def test_mode(self, value: bool = object):
        return await self._accessor(value)

    async def auth_key(self, value: bytes = object):
        return await self._accessor(value)

    async def date(self, value: int = object):
        return await self._accessor(value)

    async def user_id(self, value: int = object):
        return await self._accessor(value)

    async def is_bot(self, value: bool = object):
        return await self._accessor(value)

    async def version(self, value: int = object):
        if value == object:
            async with self.pool.acquire() as con:
                return await con.fetchval(
                    'SELECT number FROM public.version'
                )
        else:
            async with self.lock, self.pool.acquire() as con:
                await con.execute(
                    """UPDATE public.version
                       SET number = $1""",
                    value,
                )

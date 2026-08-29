import time
from typing import Any, Iterable, List, Optional, Tuple, Union

from pyrogram import Client, raw, utils
from pyrogram.storage import Storage
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    delete,
    event,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import aliased, relationship, sessionmaker

Base = declarative_base()


class SessionModel(Base):
    __tablename__ = "sessions"

    session_name = Column(String, primary_key=True)
    dc_id = Column(Integer, nullable=False)
    api_id = Column(Integer, nullable=True)
    test_mode = Column(Boolean, nullable=True)
    auth_key = Column(LargeBinary)
    date = Column(Integer, nullable=False)
    user_id = Column(BigInteger, nullable=True)
    is_bot = Column(Boolean, nullable=True)


class PeerModel(Base):
    __tablename__ = 'peers'

    session_name = Column(String, ForeignKey('sessions.session_name'), primary_key=True)
    id = Column(BigInteger, primary_key=True)
    access_hash = Column(BigInteger)
    type = Column(String)
    phone_number = Column(String)
    last_update_on = Column(BigInteger)

    session = relationship("SessionModel", back_populates="peers")


@event.listens_for(PeerModel, 'before_update')
def update_last_update_on(mapper, connection, target):
    if not target.last_update_on:
        target.last_update_on = int(time.time())


class UsernameModel(Base):
    __tablename__ = 'usernames'

    session_name = Column(String, ForeignKey('peers.session_name'), primary_key=True)
    id = Column(BigInteger, ForeignKey('peers.id'), primary_key=True)
    username = Column(String, primary_key=True)


class UpdateStateModel(Base):
    __tablename__ = 'update_state'

    id = Column(Integer, primary_key=True)
    session_name = Column(String, ForeignKey('sessions.session_name'))
    pts = Column(Integer)
    qts = Column(Integer)
    date = Column(Integer)
    seq = Column(Integer)


SessionModel.peers = relationship("PeerModel", back_populates="session")


class VersionModel(Base):
    __tablename__ = 'version'
    number = Column(Integer, primary_key=True)


def get_input_peer(peer_id: int, access_hash: int, peer_type: str):
    if peer_type in {"user", "bot"}:
        return raw.types.InputPeerUser(user_id=peer_id, access_hash=access_hash)

    if peer_type == "group":
        return raw.types.InputPeerChat(chat_id=-peer_id)

    if peer_type in {"direct", "channel", "forum", "supergroup", "community"}:
        return raw.types.InputPeerChannel(
            channel_id=utils.get_channel_id(peer_id), access_hash=access_hash
        )

    raise ValueError(f"Invalid peer type: {peer_type}")


class MultiPostgresStorage(Storage):
    VERSION = 1
    USERNAME_TTL = 8 * 60 * 60

    def __init__(self, client: Client, database: dict):
        db_url = f"postgresql+asyncpg://{database['db_user']}:{database['db_pass']}@{database['db_host']}:{database['db_port']}/{database['db_name']}"
        self.engine = create_async_engine(db_url, echo=False)
        self.session_maker = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        self.name = client.name

    async def create(self):
        async with self.session_maker() as session:
            session_exists = await session.execute(select(SessionModel).filter_by(session_name=self.name))
            session_exists = session_exists.scalar()

            if not session_exists:
                new_session = SessionModel(
                    session_name=self.name,
                    dc_id=None,
                    api_id=None,
                    test_mode=None,
                    auth_key=None,
                    date=int(time.time()),
                    user_id=None,
                    is_bot=None
                )
                session.add(new_session)
                await session.commit()

    async def open(self):
        async with self.session_maker() as session:
            session_exists = await session.execute(select(SessionModel).filter_by(session_name=self.name))
            session_exists = session_exists.scalar()
            if not session_exists:
                await self.create()

    async def save(self):
        async with self.session_maker() as session:
            await self.date(int(time.time()))
            await session.commit()

    async def close(self):
        async with self.session_maker() as session:
            await session.close()
        await self.engine.dispose()

    async def delete(self):
        async with self.session_maker() as session:
            await session.execute(
                delete(UpdateStateModel).where(UpdateStateModel.session_name == self.name)
            )
            await session.execute(
                delete(UsernameModel).where(UsernameModel.session_name == self.name)
            )
            await session.execute(
                delete(PeerModel).where(PeerModel.session_name == self.name)
            )
            await session.execute(
                delete(SessionModel).where(SessionModel.session_name == self.name)
            )

            await session.commit()

    async def update_peers(self, peers: List[Tuple[int, int, str, str]]):
        async with self.session_maker() as session:
            for peer in peers:
                stmt = select(PeerModel).filter_by(session_name=self.name, id=peer[0])
                result = await session.execute(stmt)
                existing_peer = result.scalar_one_or_none()

                if existing_peer:
                    existing_peer.access_hash = peer[1]
                    existing_peer.type = peer[2]
                    existing_peer.phone_number = peer[3]
                else:
                    new_peer = PeerModel(
                        session_name=self.name,
                        id=peer[0],
                        access_hash=peer[1],
                        type=peer[2],
                        phone_number=peer[3]
                    )
                    session.add(new_peer)

            await session.commit()

    async def update_usernames(self, usernames: List[Tuple[int, List[str]]]):
        async with self.session_maker() as session:
            for telegram_id, _ in usernames:
                await session.execute(
                    delete(UsernameModel).where(UsernameModel.session_name == self.name,
                                                UsernameModel.id == telegram_id)
                )

            for telegram_id, user_list in usernames:
                for username in user_list:
                    new_username = UsernameModel(session_name=self.name, id=telegram_id, username=username)
                    session.add(new_username)

            await session.commit()

    async def get_peer_by_id(self, peer_id_or_username):
        async with self.session_maker() as session:
            if isinstance(peer_id_or_username, int):
                peer = await session.execute(
                    select(PeerModel).filter_by(session_name=self.name, id=peer_id_or_username)
                )
                peer = peer.scalar_one_or_none()
                if peer is None:
                    raise KeyError(f"ID not found: {peer_id_or_username}")
                return get_input_peer(peer.id, peer.access_hash, peer.type)
            elif isinstance(peer_id_or_username, str):
                r = await session.execute(
                    select(
                        PeerModel.id,
                        PeerModel.access_hash,
                        PeerModel.type,
                        PeerModel.last_update_on
                    )
                    .join(UsernameModel, UsernameModel.id == PeerModel.id)
                    .filter(UsernameModel.username == peer_id_or_username,
                            UsernameModel.session_name == self.name,
                            PeerModel.session_name == self.name)
                    .order_by(PeerModel.last_update_on.desc())
                )
                r = r.fetchone()
                if r is None:
                    raise KeyError(f"Username not found: {peer_id_or_username}")
                if len(r) == 4:
                    peer_id, access_hash, peer_type, last_update_on = r
                else:
                    raise ValueError(f"The result does not contain the expected tuple of values. Received: {r}")
                if last_update_on:
                    if abs(time.time() - last_update_on) > self.USERNAME_TTL:
                        raise KeyError(f"Username expired: {peer_id_or_username}")
                return get_input_peer(peer_id, access_hash, peer_type)

            else:
                raise ValueError("peer_id_or_username must be an integer (ID) or string (Username).")

    async def get_peer_by_username(self, username: str):
        async with self.session_maker() as session:
            peer_alias = aliased(PeerModel)
            username_alias = aliased(UsernameModel)
            r = await session.execute(
                select(peer_alias.id, peer_alias.access_hash, peer_alias.type, peer_alias.last_update_on)
                .join(username_alias, username_alias.id == peer_alias.id)
                .filter(username_alias.username == username, username_alias.session_name == self.name)
                .order_by(peer_alias.last_update_on.desc())
            )
            r = r.fetchone()
            if r is None:
                raise KeyError(f"Username not found: {username}")

            peer_id, access_hash, peer_type, last_update_on = r
            return get_input_peer(peer_id, access_hash, peer_type)

    async def get_update_states(
        self,
        ids: Optional[Union[int, Iterable[int]]] = None
    ):
        async with self.session_maker() as session:
            query = select(
                UpdateStateModel.id,
                UpdateStateModel.pts,
                UpdateStateModel.qts,
                UpdateStateModel.date,
                UpdateStateModel.seq,
            ).where(
                UpdateStateModel.session_name == self.name
            )

            if ids is not None:
                state_ids = (ids,) if isinstance(ids, int) else tuple(ids)

                if not state_ids:
                    return []

                query = query.where(
                    UpdateStateModel.id.in_(state_ids)
                )

            query = query.order_by(UpdateStateModel.date.asc())

            result = await session.execute(query)
            rows = result.all()

            return [
                UpdateState(
                    id=row.id,
                    pts=row.pts,
                    qts=row.qts,
                    date=row.date,
                    seq=row.seq,
                )
                for row in rows
            ]

    async def set_update_state(
        self,
        update_state: Union[UpdateState, Iterable[UpdateState]]
    ):
        states = (
            [update_state]
            if isinstance(update_state, UpdateState)
            else list(update_state)
        )

        if not states:
            return

        async with self.session_maker() as session:
            values = [
                {
                    "id": state.id,
                    "session_name": self.name,
                    "pts": state.pts,
                    "qts": state.qts,
                    "date": state.date,
                    "seq": state.seq,
                }
                for state in states
            ]

            stmt = insert(UpdateStateModel).values(values)

            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    UpdateStateModel.id
                ],
                set_={
                    "pts": stmt.excluded.pts,
                    "qts": stmt.excluded.qts,
                    "date": stmt.excluded.date,
                    "seq": stmt.excluded.seq,
                },
            )

            await session.execute(stmt)
            await session.commit()

    async def delete_update_state(
        self,
        state_id: Union[int, Iterable[int]]
    ):
        async with self.session_maker() as session:
            if isinstance(state_id, int):
                state_ids = (state_id,)
            else:
                state_ids = tuple(state_id)

            if not state_ids:
                return

            await session.execute(
                delete(UpdateStateModel).where(
                    UpdateStateModel.session_name == self.name,
                    UpdateStateModel.id.in_(state_ids),
                )
            )

            await session.commit()

    async def get_peer_by_phone_number(self, phone_number: str):
        async with self.session_maker() as session:
            r = await session.execute(
                select(PeerModel.id, PeerModel.access_hash, PeerModel.type)
                .filter_by(session_name=self.name, phone_number=phone_number)
            )
            r = r.scalar_one_or_none()

            if r is None:
                raise KeyError(f"Phone number not found: {phone_number}")

            return get_input_peer(r.id, r.access_hash, r.type)

    async def _get(self, attr: str):
        async with self.session_maker() as session:
            result = await session.execute(select(getattr(SessionModel, attr)).filter_by(session_name=self.name))
            return result.scalar_one_or_none()

    async def _set(self, attr: str, value: Any):
        async with self.session_maker() as session:
            session_instance = await session.execute(
                select(SessionModel).filter_by(session_name=self.name)
            )
            session_instance = session_instance.scalar_one_or_none()

            if session_instance:
                setattr(session_instance, attr, value)
                await session.commit()
            else:
                raise ValueError(f"Session with name {self.name} not found.")

    async def _accessor(self, attr: str, value: Any = object):
        if value == object:
            return await self._get(attr)
        else:
            await self._set(attr, value)

    async def dc_id(self, value: int = object):
        return await self._accessor('dc_id', value)

    async def api_id(self, value: int = object):
        return await self._accessor('api_id', value)

    async def test_mode(self, value: bool = object):
        return await self._accessor('test_mode', value)

    async def auth_key(self, value: bytes = object):
        return await self._accessor('auth_key', value)

    async def date(self, value: int = object):
        return await self._accessor('date', value)

    async def user_id(self, value: int = object):
        return await self._accessor('user_id', value)

    async def is_bot(self, value: bool = object):
        return await self._accessor('is_bot', value)

    async def version(self, value: int = object):
        async with self.session_maker() as session:
            if value == object:
                result = await session.execute(select(VersionModel.number))
                return result.scalar_one_or_none()
            else:
                version_instance = await session.execute(select(VersionModel))
                version_instance = version_instance.scalar_one_or_none()

                if version_instance:
                    version_instance.number = value
                    await session.commit()

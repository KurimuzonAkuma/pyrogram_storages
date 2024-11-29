import time
from typing import Tuple, List, Any

from pyrogram import Client, raw, utils
from pyrogram.storage import Storage
from sqlalchemy import (
    Column, Integer, String, BigInteger, Boolean, ForeignKey, delete, LargeBinary, event
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker, relationship, aliased

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
    if peer_type in ["user", "bot"]:
        return raw.types.InputPeerUser(
            user_id=peer_id,
            access_hash=access_hash
        )

    if peer_type == "group":
        return raw.types.InputPeerChat(
            chat_id=-peer_id
        )

    if peer_type in ["channel", "supergroup"]:
        return raw.types.InputPeerChannel(
            channel_id=utils.get_channel_id(peer_id),
            access_hash=access_hash
        )

    raise ValueError(f"Invalid peer type: {peer_type}")


class MultiPostgresStorage(Storage):
    VERSION = 1
    USERNAME_TTL = 8 * 60 * 60

    def __init__(self, client: Client, database: dict):
        super().__init__(client.name)
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

    async def update_state(self, value: Tuple[int, int, int, int, int] = object):
        async with self.session_maker() as session:
            if value == object:
                result = await session.execute(
                    select(UpdateStateModel).filter_by(session_name=self.name)
                )
                return result.scalars().all()
            else:
                if isinstance(value, int):
                    await session.execute(
                        delete(UpdateStateModel)
                        .where(UpdateStateModel.session_name == self.name, UpdateStateModel.id == value)
                    )
                else:
                    state = await session.execute(
                        select(UpdateStateModel).filter_by(session_name=self.name, id=value[0])
                    )
                    state_instance = state.scalar_one_or_none()

                    if state_instance:
                        state_instance.pts = value[1]
                        state_instance.qts = value[2]
                        state_instance.date = value[3]
                        state_instance.seq = value[4]
                    else:
                        state_instance = UpdateStateModel(
                            id=value[0],
                            session_name=self.name,
                            pts=value[1],
                            qts=value[2],
                            date=value[3],
                            seq=value[4]
                        )
                        session.add(state_instance)

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

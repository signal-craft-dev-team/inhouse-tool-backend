from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from .config import settings

# MongoDB
mongo_client: AsyncIOMotorClient | None = None


def get_mongo_db():
    return mongo_client[settings.mongodb_db]


# PostgreSQL
engine = create_async_engine(
    settings.postgres_uri,
    echo=settings.debug,
    connect_args={"server_settings": {"search_path": "service"}},
)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_pg_session():
    async with AsyncSessionLocal() as session:
        yield session


async def connect_db():
    global mongo_client
    mongo_client = AsyncIOMotorClient(settings.mongodb_uri)


async def disconnect_db():
    if mongo_client:
        mongo_client.close()

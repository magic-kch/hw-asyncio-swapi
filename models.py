import os

from sqlalchemy import Integer, String, DateTime
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret_of_swapi")
POSTGRES_USER = os.getenv("POSTGRES_USER", "swapi")
POSTGRES_DB = os.getenv("POSTGRES_DB", "swapi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5431")

PG_DSN = (
    f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase, AsyncAttrs):
    pass


class SwapiPeople(Base):
    __tablename__ = "swapi"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    birth_year: Mapped[str] = mapped_column(String, nullable=False)
    eye_color: Mapped[str] = mapped_column(String, nullable=False)
    films: Mapped[str] = mapped_column(String, nullable=True) # строка с названиями фильмов через запятую
    gender: Mapped[str] = mapped_column(String, nullable=False)
    hair_color: Mapped[str] = mapped_column(String, nullable=False)
    height: Mapped[str] = mapped_column(String, nullable=False)
    homeworld: Mapped[str] = mapped_column(String, nullable=False)
    mass: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    skin_color: Mapped[str] = mapped_column(String, nullable=False)
    species: Mapped[str] = mapped_column(String, nullable=True) # строка с названиями типов через запятую
    starships: Mapped[str] = mapped_column(String, nullable=True) # строка с названиями кораблей через запятую
    vehicles: Mapped[str] = mapped_column(String, nullable=True) # строка с названиями транспорта через запятую


async def init_orm():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_orm():
    await engine.dispose()

from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Entity(Base):
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.now())
    created_by = Column(String, nullable=False)
    data = Column(JSONB)


class EntityCreate(BaseModel):
    created_by: str
    data: dict


class EntityRead(BaseModel):
    id: int
    created_at: datetime
    created_by: str
    data: dict

    class Config:
        from_attributes = True

from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Entity(Base):
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String, nullable=False)
    data = Column(
        JSONB
    )  # Changed from JSON to JSONB for better performance and operator support


class EntityCreate(BaseModel):
    created_by: str
    data: dict


class EntityRead(BaseModel):
    id: int
    created_at: datetime
    created_by: str
    data: dict

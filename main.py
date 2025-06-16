import uvicorn
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, and_, delete
from models import Base, Entity, EntityCreate, EntityRead
from datetime import datetime
import os
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator
import json
import re

DATABASE_URL = os.getenv("DATABASE_URL")

async_engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=20,
    max_overflow=0,
    pool_pre_ping=True,
    pool_recycle=300,
)

AsyncSessionLocal = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)

# Security validation functions
JSON_PATH_PATTERN = re.compile(r"^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*$")
MAX_JSON_PATH_DEPTH = 5
MAX_JSON_VALUE_LENGTH = 1000
MAX_JSON_CONTAINS_LENGTH = 5000


def validate_json_path(path: str) -> bool:
    """Validate JSON path to prevent injection attacks"""
    if not path or len(path) > 100:
        return False
    if not JSON_PATH_PATTERN.match(path):
        return False
    if path.count(".") > MAX_JSON_PATH_DEPTH:
        return False
    return True


def validate_json_string(
    json_str: str, max_length: int = MAX_JSON_CONTAINS_LENGTH
) -> bool:
    """Validate JSON string"""
    if not json_str or len(json_str) > max_length:
        return False
    try:
        json.loads(json_str)
        return True
    except json.JSONDecodeError:
        return False


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await async_engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.post("/entities", response_model=EntityRead)
async def create_entity(entity: EntityCreate):
    async with AsyncSessionLocal() as db:
        try:
            db_entity = Entity(
                created_at=datetime.now(),
                created_by=entity.created_by,
                data=entity.data,
            )
            db.add(db_entity)
            await db.commit()
            await db.refresh(db_entity)
            return db_entity
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/entities/{entity_id}", response_model=EntityRead)
async def read_entity(entity_id: int):
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(select(Entity).where(Entity.id == entity_id))
            entity = result.scalar_one_or_none()

            if entity is None:
                raise HTTPException(status_code=404, detail="Entity not found")
            return entity
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/entities", response_model=list[EntityRead])
async def read_entities(
    skip: int = Query(0, ge=0, le=10000),
    limit: int = Query(100, ge=1, le=1000),
    json_path: Optional[str] = Query(
        None, max_length=100, description="JSON path to filter by"
    ),
    json_value: Optional[str] = Query(
        None, max_length=1000, description="Value to match at the JSON path"
    ),
    json_contains: Optional[str] = Query(
        None, max_length=5000, description="JSON object that must be contained"
    ),
    json_key_exists: Optional[str] = Query(
        None, max_length=100, description="Check if a key exists in JSON"
    ),
):
    async with AsyncSessionLocal() as db:
        try:
            stmt = select(Entity)
            filters = []

            if json_path and json_value:
                if not validate_json_path(json_path):
                    raise HTTPException(
                        status_code=400, detail="Invalid JSON path format"
                    )

                if len(json_value) > MAX_JSON_VALUE_LENGTH:
                    raise HTTPException(status_code=400, detail="JSON value too long")

                if "." in json_path:
                    path_parts = json_path.split(".")
                    json_column = Entity.data
                    for part in path_parts[:-1]:
                        json_column = json_column[part]
                    filters.append(json_column[path_parts[-1]].astext == json_value)
                else:
                    filters.append(Entity.data[json_path].astext == json_value)

            if json_contains:
                if not validate_json_string(json_contains):
                    raise HTTPException(
                        status_code=400, detail="Invalid JSON format or too long"
                    )

                try:
                    json_obj = json.loads(json_contains)
                    filters.append(Entity.data.contains(json_obj))
                except json.JSONDecodeError:
                    raise HTTPException(
                        status_code=400, detail="Invalid JSON in json_contains"
                    )

            if json_key_exists:
                if not validate_json_path(json_key_exists):
                    raise HTTPException(
                        status_code=400, detail="Invalid JSON key path format"
                    )

                if "." in json_key_exists:
                    path_parts = json_key_exists.split(".")
                    if len(path_parts) == 2:
                        filters.append(
                            Entity.data[path_parts[0]].has_key(path_parts[1])
                        )
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail="Nested key check supports only one level",
                        )
                else:
                    filters.append(Entity.data.has_key(json_key_exists))

            if filters:
                stmt = stmt.where(and_(*filters))

            stmt = stmt.offset(skip).limit(limit)

            result = await db.execute(stmt)
            entities = result.scalars().all()

            return list(entities)

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.put("/entities/{entity_id}", response_model=EntityRead)
async def update_entity(entity_id: int, entity_update: EntityCreate):
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(select(Entity).where(Entity.id == entity_id))
            db_entity = result.scalar_one_or_none()

            if db_entity is None:
                raise HTTPException(status_code=404, detail="Entity not found")

            db_entity.created_by = entity_update.created_by
            db_entity.data = entity_update.data

            await db.commit()
            await db.refresh(db_entity)
            return db_entity

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.delete("/entities/{entity_id}")
async def delete_entity(entity_id: int):
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(select(Entity).where(Entity.id == entity_id))
            entity = result.scalar_one_or_none()

            if entity is None:
                raise HTTPException(status_code=404, detail="Entity not found")

            await db.execute(delete(Entity).where(Entity.id == entity_id))
            await db.commit()

            return {"ok": True}

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(select(1))
            return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=503, detail=f"Database connection failed: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

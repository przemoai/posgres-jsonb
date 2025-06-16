import uvicorn
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from models import Base, Entity, EntityCreate, EntityRead
from datetime import datetime
import os
from contextlib import asynccontextmanager
from typing import Optional
import json
import re

DATABASE_URL = os.getenv(
    "DATABASE_URL"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

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


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield
    engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.post("/entities", response_model=EntityRead)
def create_entity(entity: EntityCreate):
    db = SessionLocal()
    try:
        db_entity = Entity(
            created_at=datetime.now(),
            created_by=entity.created_by,
            data=entity.data
        )
        db.add(db_entity)
        db.commit()
        db.refresh(db_entity)
        return db_entity
    finally:
        db.close()


@app.get("/entities/{entity_id}", response_model=EntityRead)
def read_entity(entity_id: int):
    db = SessionLocal()
    try:
        entity = db.query(Entity).filter(Entity.id == entity_id).first()
        if entity is None:
            raise HTTPException(status_code=404, detail="Entity not found")
        return entity
    finally:
        db.close()


@app.get("/entities", response_model=list[EntityRead])
def read_entities(
    skip: int = Query(0, ge=0, le=10000),
    limit: int = Query(100, ge=1, le=1000),
    json_path: Optional[str] = Query(
        None,
        max_length=100,
        description="JSON path to filter by (e.g., 'name', 'user_age')",
    ),
    json_value: Optional[str] = Query(
        None, max_length=1000, description="Value to match at the JSON path"
    ),
    json_contains: Optional[str] = Query(
        None,
        max_length=5000,
        description="JSON object that must be contained in data field",
    ),
    json_key_exists: Optional[str] = Query(
        None, max_length=100, description="Check if a key exists in JSON"
    ),
):
    db = SessionLocal()
    try:
        query = db.query(Entity)
        filters = []

        if json_path and json_value:
            if not validate_json_path(json_path):
                raise HTTPException(status_code=400, detail="Invalid JSON path format")

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
                    filters.append(Entity.data[path_parts[0]].has_key(path_parts[1]))
                else:
                    raise HTTPException(
                        status_code=400,
                        detail="Nested key check supports only one level",
                    )
            else:
                filters.append(Entity.data.has_key(json_key_exists))

        if filters:
            query = query.filter(and_(*filters))

        entities = query.offset(skip).limit(limit).all()
        return entities

    finally:
        db.close()


@app.delete("/entities/{entity_id}")
def delete_entity(entity_id: int):
    db = SessionLocal()
    try:
        entity = db.query(Entity).filter(Entity.id == entity_id).first()
        if entity is None:
            raise HTTPException(status_code=404, detail="Entity not found")
        db.delete(entity)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


if __name__ == "__main__":
    uvicorn.run(app)

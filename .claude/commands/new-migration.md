# Create new migration: $ARGUMENTS

Create an Alembic migration for the specified schema change. Follow these steps:

## 1. Create or modify the domain entity

Edit or create in `src/app/domain/entities/`:

```python
from dataclasses import dataclass, field
from src.app.domain.entities.base import BaseEntity

@dataclass
class NewEntity(BaseEntity):
    field1: str = ""
    field2: int = 0
    # BaseEntity already provides: id (UUID), created_at, updated_at
```

Reference: `src/app/domain/entities/dataset.py`, `src/app/domain/entities/user_query.py`.

## 2. Create the SQLAlchemy mapping

Create or edit in `src/app/infrastructure/persistence_sqla/mappings/`:

```python
from sqlalchemy import Table, Column, String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID

new_table = Table(
    "new_table",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("field1", String(255), nullable=False),
    Column("field2", Integer, default=0),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
)

# Mapper registration in start_mappers()
mapper_registry.map_imperatively(NewEntity, new_table)
```

Reference: check existing mappings in `src/app/infrastructure/persistence_sqla/mappings/`.

## 3. Generate the migration

```bash
make db.revision msg="$ARGUMENTS"
```

This runs `alembic revision --autogenerate -m "$ARGUMENTS"`.

The file is created in `src/app/infrastructure/persistence_sqla/alembic/versions/`.

## 4. Review and adjust the migration

Open the generated file and verify:
- [ ] `upgrade()`: operations are correct (CREATE TABLE, ADD COLUMN, etc.)
- [ ] `downgrade()`: rollback is correct (DROP TABLE, DROP COLUMN, etc.)
- [ ] Necessary indexes are included
- [ ] If using pgvector: `op.execute('CREATE EXTENSION IF NOT EXISTS vector')` in upgrade

## 5. Apply the migration

```bash
make db.migrate
```

## 6. Create the repository (if new table)

If it's a new table, create the port + adapter:
- Port: `src/app/domain/ports/{category}/{name}_repository.py`
- Adapter: `src/app/infrastructure/adapters/{category}/{name}_repository_sqla.py`
- Register in Dishka: `src/app/setup/ioc/provider_registry.py`

Reference: `src/app/domain/ports/dataset/dataset_repository.py` → `src/app/infrastructure/adapters/dataset/dataset_repository_sqla.py`.

## Checklist
- [ ] Domain entity created/modified
- [ ] SQLAlchemy mapping updated
- [ ] Migration generated with `make db.revision`
- [ ] Migration reviewed (upgrade + downgrade correct)
- [ ] Migration applied with `make db.migrate`
- [ ] Repository created and registered (if new table)

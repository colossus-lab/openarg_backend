# Domain Layer

The domain layer contains business entities, abstract port interfaces, and domain exceptions. It has **zero framework dependencies** — only Python stdlib and dataclasses.

Location: `src/app/domain/`

## Entities

All entities inherit from `BaseEntity` which provides UUID primary key and timestamps.

### BaseEntity (`entities/base.py`)

```python
@dataclass
class BaseEntity:
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)
```

All timestamps are UTC-aware via `datetime.now(UTC)`.

### User (`entities/user/user.py`)

Represents a user synced from Google OAuth.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `email` | str | `""` | Google email (unique in DB) |
| `name` | str | `""` | Display name |
| `image_url` | str | `""` | Google profile image |

### Conversation (`entities/chat/conversation.py`)

A chat conversation owned by a user.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `user_id` | UUID | `uuid4()` | FK to User |
| `title` | str | `""` | Conversation title |

### Message (`entities/chat/message.py`)

A single message within a conversation.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `conversation_id` | UUID | `uuid4()` | FK to Conversation |
| `role` | str | `""` | `"user"` or `"assistant"` |
| `content` | str | `""` | Message text |
| `sources` | list[dict] | `[]` | Source attributions (JSONB) |

### Dataset (`entities/dataset/dataset.py`)

Metadata for an indexed public dataset.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source_id` | str | `""` | Portal-specific ID |
| `title` | str | `""` | Dataset title |
| `description` | str | `""` | Full description |
| `organization` | str | `""` | Publishing org |
| `portal` | str | `""` | Source portal (datos_gob_ar, caba) |
| `url` | str | `""` | Dataset page URL |
| `download_url` | str | `""` | Direct download link |
| `format` | str | `""` | File format (csv, json, xlsx) |
| `columns` | str | `""` | JSON string with column names |
| `sample_rows` | str | `""` | JSON string with sample data |
| `tags` | str | `""` | Comma-separated tags |
| `last_updated_at` | datetime | `_utcnow` | Last update on portal |
| `is_cached` | bool | `False` | Whether data is cached locally |
| `row_count` | int \| None | `None` | Number of rows |

### DatasetChunk (`entities/dataset/dataset.py`)

A vector-embedded chunk of dataset metadata used for semantic search.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dataset_id` | str | `""` | Reference to parent dataset |
| `content` | str | `""` | Combined text for embedding |
| `embedding` | list[float] | `[]` | 1024-dim vector (Cohere Embed Multilingual v3) |

### CachedDataset (`entities/dataset/cached_data.py`)

Tracks the download and caching status of a dataset.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dataset_id` | str | `""` | Reference to parent dataset |
| `table_name` | str | `""` | PostgreSQL table name |
| `row_count` | int | `0` | |
| `columns_json` | str | `""` | JSON column metadata |
| `size_bytes` | int | `0` | |
| `status` | str | `"pending"` | pending/downloading/ready/error |
| `error_message` | str | `""` | Error details |

### UserQuery (`entities/query/query.py`)

Analytics record for a user query.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `user_id` | str | `""` | Session ID or email |
| `question` | str | `""` | Original question |
| `status` | str | `"pending"` | Query lifecycle status |
| `plan_json` | str | `""` | LLM execution plan |
| `datasets_used` | str | `""` | JSON array of dataset IDs |
| `analysis_result` | str | `""` | Final analysis |
| `sources_json` | str | `""` | JSON source attributions |
| `error_message` | str | `""` | |
| `tokens_used` | int | `0` | |
| `duration_ms` | int | `0` | |

### AgentTask (`entities/agent/agent_task.py`)

Execution log for an individual agent step.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query_id` | str | `""` | Parent query |
| `agent_type` | str | `""` | planner/collector/analyst |
| `status` | str | `"pending"` | pending/running/completed/error |
| `input_json` | str | `""` | Input data |
| `output_json` | str | `""` | Output data |
| `error_message` | str | `""` | |
| `tokens_used` | int | `0` | |
| `duration_ms` | int | `0` | |

---

## Ports (Interfaces)

All ports are abstract base classes (ABC) defining the contracts that infrastructure adapters must implement.

### IUserRepository (`ports/user/user_repository.py`)

```python
class IUserRepository(ABC):
    async def upsert_by_email(self, user: User) -> User: ...
    async def get_by_email(self, email: str) -> User | None: ...
    async def get_by_id(self, user_id: UUID) -> User | None: ...
```

### IChatRepository (`ports/chat/chat_repository.py`)

```python
class IChatRepository(ABC):
    async def create_conversation(self, conversation: Conversation) -> Conversation: ...
    async def list_conversations(self, user_id: UUID, limit=20, offset=0) -> list[Conversation]: ...
    async def get_conversation(self, conversation_id: UUID) -> Conversation | None: ...
    async def delete_conversation(self, conversation_id: UUID) -> bool: ...
    async def update_conversation_title(self, conversation_id: UUID, title: str) -> Conversation | None: ...
    async def add_message(self, message: Message) -> Message: ...
    async def get_messages(self, conversation_id: UUID, limit=100, offset=0) -> list[Message]: ...
```

### IDatasetRepository (`ports/dataset/dataset_repository.py`)

```python
class IDatasetRepository(ABC):
    async def save(self, dataset: Dataset) -> Dataset: ...
    async def get_by_id(self, dataset_id: UUID) -> Dataset | None: ...
    async def get_by_source_id(self, source_id: str, portal: str) -> Dataset | None: ...
    async def list_by_portal(self, portal: str, limit=100, offset=0) -> list[Dataset]: ...
    async def count_by_portal(self, portal: str) -> int: ...
    async def upsert(self, dataset: Dataset) -> Dataset: ...
```

### ILLMProvider (`ports/llm/llm_provider.py`)

```python
@dataclass
class LLMMessage:
    role: str   # system, user, assistant
    content: str

@dataclass
class LLMResponse:
    content: str
    tokens_used: int
    model: str

class ILLMProvider(ABC):
    async def chat(self, messages: list[LLMMessage], temperature=0.0, max_tokens=4096) -> LLMResponse: ...
    async def chat_stream(self, messages: list[LLMMessage], temperature=0.0, max_tokens=4096) -> AsyncIterator[str]: ...
```

### IEmbeddingProvider (`ports/llm/llm_provider.py`)

```python
class IEmbeddingProvider(ABC):
    async def embed(self, text: str) -> list[float]: ...
    async def embed_batch(self, texts: list[str]) -> list[list[float]]: ...
```

### IVectorSearch (`ports/search/vector_search.py`)

```python
@dataclass
class SearchResult:
    dataset_id: str
    title: str
    description: str
    portal: str
    columns: str
    score: float

class IVectorSearch(ABC):
    async def search_datasets(self, query_embedding: list[float], limit=5) -> list[SearchResult]: ...
    async def index_dataset(self, dataset_id: str, chunks: list[DatasetChunk]) -> None: ...
    async def delete_dataset_chunks(self, dataset_id: str) -> None: ...
```

### ISQLSandbox (`ports/sandbox/sql_sandbox.py`)

```python
@dataclass
class SandboxResult:
    columns: list[str]
    rows: list[dict]
    row_count: int
    truncated: bool
    error: str | None

@dataclass
class CachedTableInfo:
    table_name: str
    dataset_id: str
    row_count: int | None
    columns: list[str]

class ISQLSandbox(ABC):
    async def execute_readonly(self, sql: str, timeout_seconds: int = 30) -> SandboxResult: ...
    async def list_cached_tables(self) -> list[CachedTableInfo]: ...
```

### IDataSource (`ports/source/data_source.py`)

```python
@dataclass
class CatalogEntry:
    source_id: str
    title: str
    description: str
    organization: str
    url: str
    resources: list[dict]

@dataclass
class DownloadedData:
    content: bytes
    format: str
    filename: str

class IDataSource(ABC):
    async def fetch_catalog(self, limit=100, offset=0) -> list[CatalogEntry]: ...
    async def fetch_catalog_count(self) -> int: ...
    async def download_dataset(self, resource_url: str) -> DownloadedData: ...
```

### ICacheService (`ports/cache/cache_port.py`)

```python
class ICacheService(ABC):
    async def get(self, key: str) -> dict | None: ...
    async def set(self, key: str, value: dict, ttl_seconds: int = 3600) -> None: ...
    async def delete(self, key: str) -> None: ...
    async def exists(self, key: str) -> bool: ...
```

---

## Exceptions

### Error Hierarchy

```
Exception
  └── ApplicationError (base, with error_code + http_status)
        ├── DomainError
        └── InfrastructureError
```

### ErrorCode Enum (`exceptions/error_codes.py`)

Each code maps to an `ErrorDefinition` with: code string, i18n key, default message, HTTP status.

| Prefix | Domain | Examples |
|--------|--------|----------|
| `DS_*` | Datasets | DS_001 Not found, DS_002 Download failed, DS_003 Parse error |
| `QR_*` | Queries | QR_001 Empty query, QR_002 Too long, QR_003 Processing failed |
| `AG_*` | Agents | AG_001 Timeout, AG_002 LLM error, AG_003 No datasets found |
| `SC_*` | Scraper | SC_001 Portal unreachable, SC_002 Rate limited |
| `SR_*` | Search | SR_001 Embedding failed |

### Usage

```python
from app.domain.exceptions.base import DomainError
from app.domain.exceptions.error_codes import ErrorCode

raise DomainError(
    ErrorCode.DS_NOT_FOUND,
    details={"dataset_id": "abc123"},
)
```

The exception handler converts this to:
```json
{
  "code": "DS_001",
  "message": "Dataset not found",
  "details": { "dataset_id": "abc123" }
}
```

# Create new data connector: $ARGUMENTS

Add a new open data portal to the scraping/indexing system. Follow these steps:

## 1. Create the IDataSource adapter

Create `src/app/infrastructure/adapters/source/{name}_adapter.py` implementing `IDataSource`:

```python
from src.app.domain.ports.source.data_source import IDataSource
from src.app.domain.entities.dataset import CatalogEntry, DownloadedData

class {Name}Adapter(IDataSource):
    def __init__(self, base_url: str = "https://..."):
        self._base_url = base_url
        self._client = httpx.AsyncClient(timeout=30.0)

    async def fetch_catalog(self, offset: int = 0, limit: int = 100) -> list[CatalogEntry]:
        # Paginate through the portal catalog
        # Return metadata: title, description, organization, tags, columns, download_url, format
        ...

    async def fetch_catalog_count(self) -> int:
        # Total datasets in the portal
        ...

    async def download_dataset(self, entry: CatalogEntry) -> DownloadedData:
        # Download the file (CSV/JSON/XLSX)
        ...
```

Primary reference: `src/app/infrastructure/adapters/source/datos_gob_ar_adapter.py` (CKAN), `src/app/infrastructure/adapters/source/caba_adapter.py`.

## 2. Register in Dishka

Edit `src/app/setup/ioc/provider_registry.py`:
- Add the adapter to `DataSourceProvider` or create a new one
- Expose as provider with `scope=Scope.APP`

## 3. Add portal configuration

Edit `src/app/setup/config/settings.py`:
- Add the base URL for the new portal in `ScraperSettings`

Edit `config/local/config.toml` with default values.

## 4. Wire into the scraper worker

Edit `src/app/infrastructure/celery/tasks/scraper_tasks.py`:
- Add the new portal as an option in `scrape_catalog(portal: str)`
- The worker should use the adapter to iterate the catalog and save to `datasets`
- Dispatch `index_dataset_embedding` for each inserted dataset

## 5. Update task routing (if needed)

If the portal needs a dedicated queue, add it in `src/app/infrastructure/celery/app.py` → `task_routes`.

## Checklist
- [ ] Adapter implements full `IDataSource` (fetch_catalog, fetch_catalog_count, download_dataset)
- [ ] Registered in Dishka IoC
- [ ] URL configured in settings
- [ ] Scraper worker recognizes it as a valid portal
- [ ] Tested: `POST /api/v1/datasets/scrape/{portal}` indexes datasets

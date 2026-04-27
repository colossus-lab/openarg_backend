# Spec 013 — Ingestion Validation (WS0)

**Type**: Forward-engineered (collector_plan.md WS0)
**Status**: Implemented (code only — pending operator review and rollout)
**Hexagonal scope**: Application (Strategy) + Infrastructure (hooks + sweep)
**Related plan**: [./plan.md](./plan.md)
**Implements**: WS0 of [collector_plan.md](../../../collector_plan.md)

---

## 1. Context & Purpose

A first-class validation component that runs before, during and after every ingestion. Replaces ad-hoc, downloader-internal sniff checks with a pluggable detector suite that:

- **bloquea** that HTML pages, GDrive scan warnings, `.rar` disguised as `.zip`, etc. ever reach pandas (Modo 1, pre-parse);
- **bloquea** that single-column garbage tables get marked `ready` (Modo 2, post-parse gate);
- **registra retroactivamente** the corruption already in production (Modo 3, retrospective sweep);
- **persiste un audit trail versionado** in `ingestion_findings` so reruns of the same detector against the same input don't duplicate rows.

This is the foundation for the WS5 rebuild — without WS0 the rebuild reintroduces the same garbage with different names.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Detector** | A `Strategy` that inspects a `ResourceContext` and returns 0..1 `Finding`. Declares a `name`, `version`, `severity` and `applicable_to(ctx)` predicate. |
| **Finding** | A single detector outcome — `severity ∈ {info, warn, critical}`, JSONB payload, `should_redownload` flag. |
| **Mode** | One of `pre_parse`, `post_parse`, `retrospective`, `state_invariant`. The same detector can run in any of them. |
| **Soft-flip** | Retrospective mode that registers `severity=warn` findings without flipping `materialization_status`. Default during the 1-week soak. |
| **Auto-flip** | After soak, `OPENARG_SWEEP_AUTOFLIP=1` lets retrospective findings flip `materialization_status='materialization_corrupted'`. |

## 3. Detector Suite (14 detectors)

### Content (need raw_bytes or materialized columns)
1. **`HtmlAsDataDetector`** — first bytes look like HTML/XML when CSV/Excel was expected. **39 cases in prod**. *Critical, should_redownload.*
2. **`SingleColumnDetector`** — materialized table has 1 column whose name starts with `<` / `<!DOCTYPE` / `<html`. **38 confirmed.** *Critical.*
3. **`HeaderFlattenDetector`** — column names like `col_0..N` or `Foo.1`/`Foo.2`. **127 INDEC tables in prod.** *Warn.*
4. **`GDriveScanWarningDetector`** — payload contains the GDrive scan interstitial. *Critical.*
5. **`EncodingMismatchDetector`** — mojibake (`Ã±` for `ñ`) in column names or raw bytes. **3 cases in prod.** *Warn.*

### Metadata + integrity (need materialized state)
6. **`RowCountDetector`** — 0 rows or divergence > threshold from declared row count. **12 row_count=0 cases.** *Critical when paired with size_bytes=0.*
7. **`MissingKeyColumnDetector`** — domain-specific (e.g. `cueanexo` for educacion). *Warn.*
8. **`MetadataIntegrityDetector`** — `cached_datasets.size_bytes`/`row_count`/`columns_json` inconsistent with the actual table. **553 ready+size_bytes=0** in prod (mostly ingester bug). *Warn (separates bug from corruption).*

### Pre-ingest (cheap, network or metadata only)
9. **`MissingDownloadUrlDetector`** — empty/invalid URL, non-HTTP scheme. **8 cases in prod.** *Warn.*
10. **`HttpErrorDetector`** — 4xx/5xx, redirect-loop, dead-portal hostname. **65+ cases.** *Critical for 5xx/404/410.*
11. **`FileTooLargeDetector`** — per-file 500 MB cap (per WS5), raw download 5 GB cap. *Warn / critical for 5 GB.*

### Naming + storage
12. **`TableNameCollisionDetector`** — pre-CREATE TABLE check; the candidate name truncated to 63 chars collides with an existing table owned by a different `resource_id`. **10 cases in prod.** *Critical.*
13. **`NonTabularZipDetector`** — ZIP only contains PDFs/images. **2 ACUMAR Actas cases.** *Info — classify as `document_bundle`.*
14. **`UnsupportedArchiveDetector`** — file declared `.zip` but magic bytes are `.rar`/`.7z`/`.gz`. **4 cases.** *Warn.*

## 4. Functional Requirements

- **FR-001**: The validator MUST run the same detector list across all three modes; the only difference is the inputs available in `ResourceContext`.
- **FR-002**: Each detector MUST declare `version`. Bumping the version invalidates findings under the old version (sweep re-evaluates).
- **FR-003**: Findings MUST be UPSERTed by `(resource_id, detector_name, detector_version, mode, input_hash)`. Repeat runs bump `found_at`, do not duplicate.
- **FR-004**: A detector that crashes MUST NOT break the validator — it logs and the validator continues with the next detector.
- **FR-005**: Pre-parse and post-parse hooks MUST be **fail-open** — a crash in the validator infrastructure cannot block ingestion.
- **FR-006**: The retrospective sweep MUST be paginated in bounded batches so a single run never exceeds the soft time limit. Current implementation uses offset pagination with optional portal filtering.
- **FR-007**: Critical findings in pre-parse MUST short-circuit the S3 upload — we do not store HTML/`.rar` garbage.
- **FR-008**: `OPENARG_DISABLE_INGESTION_VALIDATOR=1` MUST disable the runtime hooks (Modo 1+2) without disabling Modo 3.
- **FR-009**: Specialized collector tasks that materialize directly to `cache_*` MUST finalize through a shared post-parse helper before writing `cached_datasets.status='ready'`. The generic collector path and the specialized collectors must share the same WS0 Modo 2 gate.
- **FR-010**: ZIP ingestion MUST classify members entry-by-entry using `MultiFileExpander`; the 500MB policy applies per expanded entry, not to the aggregate decompressed size of the archive.
- **FR-011**: Retrospective sweeps MUST propagate both `materialized_row_count` and `declared_row_count` into `ResourceContext` so metadata integrity detectors can compare runtime table state against stored collector metadata without the hook itself crashing.

## 5. Success Criteria

- **SC-001**: Zero new HTML-as-data tables (the 38 in staging) reach `status='ready'` after deploy.
- **SC-002**: The `ingestion_findings` table populates with the 14 detector_names within the first sweep.
- **SC-003**: A repeated sweep against unchanged data does NOT increase row count in `ingestion_findings` (idempotency).
- **SC-004**: Pre-parse rejection latency adds <50ms p95 to the collector path.

## 6. Out of Scope

- Re-downloading rejected resources (handled by Modo 3 + `should_redownload` flag, but the actual re-collect dispatch is in collector_tasks).
- Live HTTP probing in Modo 1 (relegated to the curated_sources CI step).
- Per-portal detector tuning (deferred until a real false-positive shows up).

## 7. Tech Debt

- **DEBT-013-001**: `MissingKeyColumnDetector` rules are hard-coded. A second iteration should pull them from `taxonomy.yml`.
- **DEBT-013-002**: `findings_repository.persist_findings` opens one transaction per finding. Batch UPSERT would be cheaper.

# Spec: Scrape Córdoba Legislatura

**Type**: Reverse-engineered
**Status**: Draft
**Hexagonal scope**: Infrastructure (task only)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

A **WordPress** crawler that extracts download links (CSV/XLS) from the open data portal of the **Legislatura de Córdoba**. Since the portal does not expose a structured API (neither CKAN nor DKAN), HTML scraping is required.

## 2. User Stories

- **US-001 (P1)**: As a user, I want to access provincial legislative datasets from Córdoba.

## 3. Functional Requirements

- **FR-001**: MUST crawl WordPress pages on legislaturacordoba.gob.ar.
- **FR-002**: MUST extract links to CSV/XLS files using BeautifulSoup or similar.
- **FR-003**: MUST download files and cache them.

## 4. Tech Debt Discovered

- **[DEBT-001]** — **Fragile HTML scraping** — breaks if the WordPress template changes.
- **[DEBT-002]** — **No tests** against HTML fixtures.
- **[DEBT-003]** — **No domain port**.

---

**End of spec.md**

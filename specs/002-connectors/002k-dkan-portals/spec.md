# Spec: Scrape DKAN Portals (Rosario, Jujuy)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Infrastructure (task only)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Scraper for **DKAN** portals (a CKAN fork used by some Argentine cities). It currently covers **Rosario** and **Jujuy**. DKAN does not implement the standard CKAN `/api/3/action/package_search`, so it requires portal-specific scraping. It is the only mechanism by which OpenArg accesses data from these cities.

## 2. User Stories

- **US-001 (P1)**: As a user, I want to search for Rosario or Jujuy datasets.
- **US-002 (P2)**: As the system, I need to scrape the catalogs weekly.

## 3. Functional Requirements

- **FR-001**: MUST implement portal-specific scraping for DKAN (Rosario and Jujuy have different endpoints).
- **FR-002**: MUST extract dataset metadata (title, description, resources, download URLs).
- **FR-003**: MUST download CSVs when the resource is small (<2MB).
- **FR-004**: MUST register datasets in the `datasets` table.

## 4. Success Criteria

- **SC-001**: Full weekly scrape in **<5 minutes** per portal.
- **SC-002**: Schema changes detected (warning logged).

## 5. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — Is there a plan to support more DKAN portals? Or keep only Rosario/Jujuy?
- **[NEEDS CLARIFICATION CL-002]** — Does the DKAN API have stable endpoints, or does it require HTML scraping?

## 6. Tech Debt Discovered

- **[DEBT-001]** — **Two portals hardcoded** — there is no dynamic registry.
- **[DEBT-002]** — **No specific tests** against DKAN response fixtures.
- **[DEBT-003]** — **The "rosario" portal migrated from CKAN to DKAN** (see `002d-ckan-search/plan.md`) — semantic duplication between both connectors.

---

**End of spec.md**

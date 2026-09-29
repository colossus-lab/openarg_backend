# Spec 029 — MCP público de datos (mcp.openarg.org)

**Status**: Draft · **Owner**: backend
**Last synced with code**: 2026-09-29
**Extends**: 007-public-api, 008-developers-keys

---

## 1. Context & Purpose

OpenArg ya tiene una API pública (`POST /api/v1/ask`, spec 007) con claves por
usuario (`oarg_sk_`, spec 008). Casi nadie la usa, porque integrarla exige
programar. Un servidor MCP pone esa misma API adentro de los asistentes que la
gente ya usa (Claude Code, Claude Desktop, Cursor, VS Code). La web
`mcp.openarg.org` lo documenta y es la página que se promociona.

No confundir con el MCP de operaciones (spec 028). Ese corre en la máquina de
un operador y lee los servidores por SSH. Éste es público y no ve nada que la
API pública no muestre ya.

## 2. Ubiquitous Language

- **Clave**: `oarg_sk_…`, la misma de la API pública. Viaja en
  `Authorization: Bearer`.
- **Cupo**: 10 preguntas por día por clave (plan free, spec 007 FR-005), más
  el tope global diario que funciona como techo de gasto de Bedrock.
- **Herramienta de catálogo**: la que no pasa por el LLM (`listar_fuentes`).
  No descuenta preguntas.

## 3. User Stories

- Como periodista o investigador, quiero preguntarle a mi asistente por un
  dato oficial y recibirlo con la fuente, sin programar.
- Como desarrollador, quiero configurar el servidor copiando un bloque y mi
  clave.
- Como operador, quiero que el gasto tenga techo y que el MCP no pueda tirar
  abajo el chat de openarg.org.

## 4. Functional Requirements

- **FR-001**: Contenedor propio (`openarg_mcp`, imagen `openarg-mcp`). No
  importa nada de `src/` y habla con el backend sólo por HTTP (`BACKEND_URL`).
- **FR-002**: No decide nada de autenticación ni de cuotas. Reenvía la clave
  del usuario a `/api/v1/ask` y `/api/v1/fuentes`, y el backend valida y
  cobra.
- **FR-003**: Listar las herramientas no pide clave, para que los directorios
  de MCP puedan inspeccionar el servidor. Llamarlas sí la pide. Sin clave, la
  respuesta es un error de herramienta que dice dónde conseguirla.
- **FR-004**: Reenvía la IP del usuario (el primer valor de `X-Forwarded-For`
  que pone Caddy, validado como IP) para el límite por IP del backend.
- **FR-005**: La clave nunca se escribe en un log (`core.redact`). El
  contenedor corre sin access log.
- **FR-006**: Los errores del backend (401, 408, 429, 503) se traducen a
  mensajes en castellano que el asistente puede mostrar tal cual. El detalle
  de un 5xx no se reenvía.
- **FR-007**: Transporte Streamable HTTP en modo stateless con respuestas
  JSON. No hay sesiones que perder en un reinicio.
- **FR-008**: Protección contra DNS rebinding: sólo se aceptan los `Host`
  de `MCP_ALLOWED_HOSTS`.
- **FR-009**: La web estática (`mcp_publico/site/`) se sirve desde el mismo
  contenedor en `/`. La CSP no permite nada de otro origen, así que las
  fuentes tipográficas están en el repo.

### Herramientas

| Herramienta | Backend | Cupo |
|---|---|---|
| `consultar_datos_publicos(pregunta)` | `POST /api/v1/ask` | 1 de las 10 diarias |
| `listar_fuentes()` | `GET /api/v1/fuentes` | no descuenta; 60/día propio |

## 5. Success Criteria

- **SC-001**: Una pregunta desde Claude Code contra `mcp.openarg.org/mcp`
  vuelve con respuesta y fuentes.
- **SC-002**: La consulta 11 del día vuelve con el mensaje de cupo en
  castellano.
- **SC-003**: Con el contenedor del MCP detenido, el chat de openarg.org
  sigue funcionando igual.
- **SC-004**: `docker logs openarg_mcp` no contiene ninguna `oarg_sk_`.

## 6. Out of Scope

- **OAuth** (conectores de claude.ai web y ChatGPT). Es la fase 2 y necesita
  spec propia.
- **Modo profundo**: la API pública no lo expone (spec 007).
- `chart_data` / `map_data` como contenido estructurado. La v1 devuelve texto.

## 7. Tech Debt Discovered

- **[DEBT-001]** — `mcp` no está en `pyproject.toml`, así que CI saltea
  `tests/unit/test_mcp_publico_server.py`. Los tests de `core.py` sí corren.
  Para correr el de punta a punta:
  `uv pip install -r mcp_publico/requirements.txt`.
- **[DEBT-002]** — Cualquier cambio en `mcp_publico/**` reconstruye las 10
  imágenes (`build.yml` es una sola matriz sin filtro por servicio).

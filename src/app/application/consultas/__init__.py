"""Constructores de consultas compartidos por el modo datos del MCP y el agente.

El modo datos (``public_catalog`` → ``catalogo_router``) y las herramientas del
agente (``obtener_datos``, ``calcular``) arman SQL a partir de una tabla que
existe, columnas que existen y filtros con forma validada. Hasta el 04-oct
cada uno tenía su versión de las mismas piezas y las dos tenían los mismos
bugs (auditoría del 02/03-oct, verificada contra el código):

- ``numeros``: "12.500" se leía como 12,5 (Pauta CABA: 119.968 contra
  242.635.557). El formato se decide por columna, con una muestra.
- ``fechas``: el filtro de período era lexicográfico y sólo entendía ISO; una
  columna "1/10/2017" o "Junio de 2026" daba 0 filas sin aviso.
- ``filtros``: igualdad byte a byte ("Educacion" no encontraba "Educación"),
  sin ``en``, y los valores iban interpolados en el SQL.
- ``sugerencias``: un filtro sin coincidencias devolvía 0 filas (o un
  ``valor: 0`` citable) sin ninguna pista; el legacy sí recuperaba esos casos.

Todo lo de acá es puro salvo ``sugerencias`` y ``perfiles``, que leen del
sandbox; los valores de usuario viajan siempre como parámetros ligados.
"""

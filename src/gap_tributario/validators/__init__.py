"""Schemas Pandera para validação fail-fast de dados.

Cada schema valida um DataFrame Polars proveniente de um extrator específico.
Falhas de validação geram SchemaError imediatamente (modo fail-fast).
"""

from __future__ import annotations

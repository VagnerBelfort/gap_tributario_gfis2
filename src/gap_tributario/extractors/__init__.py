"""Módulos de extração de dados por fonte.

Cada extrator é responsável por acessar uma fonte de dados específica
e retornar um DataFrame Polars validado.

Módulos:
- base: Protocol Extractor (interface base)
- arrecadacao: Leitor GFIS2 Parquet (g_arrecadacao ouro)
- comex: Leitor MDIC ComEx CSVs (exportações/importações)
- ibge: Cliente IBGE SIDRA API (VAB via sidrapy)
- ptax: Cliente BCB PTAX API (cotação dólar)
- siscomex: Cliente Oracle Siscomex (opcional)
"""

from __future__ import annotations

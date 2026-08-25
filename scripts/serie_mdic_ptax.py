#!/usr/bin/env python3
"""Reconstrói a série de importações do MA pelo MDIC, para auditar o corredor.

O corredor de controle aplicado em `engine/comparacao.py` (FAIXA_OBSERVADA e
CORREDOR_CONTROLE) nasceu da comparação ano a ano entre o snapshot do Siscomex
e o dado público do MDIC. Este script refaz essa comparação do zero, a partir
das duas APIs oficiais, para que a constante não dependa de números digitados
à mão em documento.

Não precisa da rede da SEFAZ: ComexStat e BCB são públicos. O único insumo
local é o snapshot já versionado em `bases/siscomex_importacoes.csv`.

Uso:
  uv run python scripts/serie_mdic_ptax.py
  uv run python scripts/serie_mdic_ptax.py --de 2019 --ate 2025 --formato markdown
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Dict

COMEXSTAT_URL = "https://api-comexstat.mdic.gov.br/general"
PTAX_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    "?@dataInicial='01-01-{ano}'&@dataFinalCotacao='12-31-{ano}'"
    "&$format=json&$select=cotacaoCompra,cotacaoVenda"
)
SNAPSHOT_PADRAO = Path("bases/siscomex_importacoes.csv")
UF = "Maranhão"


def _get(url: str, corpo: bytes = None) -> dict:
    """GET/POST JSON com cabeçalho mínimo."""
    req = urllib.request.Request(url, data=corpo)
    if corpo is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310 — URLs fixas
        return json.loads(resp.read())


def importacoes_fob_usd(de: int, ate: int) -> Dict[int, Decimal]:
    """US$ FOB de importação do MA por ano, do ComexStat (UF do produto)."""
    corpo = json.dumps(
        {
            "flow": "import",
            "monthDetail": False,
            "yearDetail": True,
            "period": {"from": f"{de}-01", "to": f"{ate}-12"},
            "filters": [],
            "details": ["state"],
            "metrics": ["metricFOB"],
        }
    ).encode()
    lista = _get(COMEXSTAT_URL, corpo)["data"]["list"]
    return {
        int(r["year"]): Decimal(r["metricFOB"]) for r in lista if r["state"] == UF
    }


def ptax_media_venda(ano: int) -> Decimal:
    """Média anual das cotações de venda da PTAX — a ponta que o pipeline usa."""
    valores = _get(PTAX_URL.format(ano=ano))["value"]
    total = sum(Decimal(str(v["cotacaoVenda"])) for v in valores)
    return total / Decimal(len(valores))


def importacoes_siscomex(snapshot: Path) -> Dict[int, Dict[str, Decimal]]:
    """Soma o snapshot por ano, separando domicílio fiscal de local de despacho."""
    domicilio: Dict[int, Decimal] = defaultdict(Decimal)
    despacho: Dict[int, Decimal] = defaultdict(Decimal)
    with snapshot.open(encoding="utf-8") as f:
        for linha in csv.DictReader(f, delimiter=";"):
            ano = linha["ano"]
            if len(ano) != 4:  # anos digitados errado na origem (18, 202, 203...)
                continue
            valor = Decimal(linha["cif_brl"])
            if linha["uf_importador"] == "MA":
                domicilio[int(ano)] += valor
            if linha["uf_despacho"] == "MA":
                despacho[int(ano)] += valor
    return {
        ano: {"domicilio": domicilio[ano], "despacho": despacho[ano]}
        for ano in sorted(set(domicilio) | set(despacho))
    }


def _br(valor: Decimal, casas: int = 2) -> str:
    """Formata número no padrão brasileiro."""
    return f"{valor:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _pct(valor: Decimal) -> str:
    """Formata percentual com sinal, no padrão brasileiro: +2,4%."""
    return f"{valor:+.1f}%".replace(".", ",")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--de", type=int, default=2019, help="Ano inicial")
    parser.add_argument("--ate", type=int, default=2025, help="Ano final")
    parser.add_argument(
        "--snapshot", type=Path, default=SNAPSHOT_PADRAO, help="CSV do Siscomex"
    )
    parser.add_argument(
        "--formato", choices=("markdown", "csv"), default="markdown", help="Saída"
    )
    args = parser.parse_args()

    if not args.snapshot.exists():
        print(f"Snapshot não encontrado: {args.snapshot}", file=sys.stderr)
        return 2

    fob = importacoes_fob_usd(args.de, args.ate)
    siscomex = importacoes_siscomex(args.snapshot)

    if args.formato == "markdown":
        print(
            "| Ano | MDIC US$ bi FOB | PTAX média | MDIC R$ bi | "
            "Siscomex R$ bi (domicílio) | Δ vs MDIC | Siscomex R$ bi (despacho) | Trânsito |"
        )
        print("|---|---:|---:|---:|---:|---:|---:|---:|")

    desvios = []
    for ano in range(args.de, args.ate + 1):
        if ano not in fob or ano not in siscomex:
            continue
        ptax = ptax_media_venda(ano)
        mdic = fob[ano] * ptax / Decimal("1e9")
        dom = siscomex[ano]["domicilio"] / Decimal("1e9")
        desp = siscomex[ano]["despacho"] / Decimal("1e9")
        desvio = (dom - mdic) / mdic * Decimal("100")
        transito = (desp - dom) / dom * Decimal("100")
        desvios.append(desvio)

        if args.formato == "markdown":
            print(
                f"| {ano} | {_br(fob[ano] / Decimal('1e9'), 3)} | {_br(ptax, 4)} | "
                f"{_br(mdic)} | {_br(dom)} | {_pct(desvio)} | {_br(desp)} | "
                f"{_pct(transito)} |"
            )
        else:
            print(f"{ano};{fob[ano]};{ptax};{mdic};{dom};{desvio};{desp};{transito}")

    if desvios and args.formato == "markdown":
        ordenados = sorted(desvios)
        mediana = ordenados[len(ordenados) // 2]
        print(
            f"\nFaixa observada: {_pct(min(desvios))} a {_pct(max(desvios))} "
            f"(mediana {_pct(mediana)}) — conferir contra FAIXA_OBSERVADA "
            "em src/gap_tributario/engine/comparacao.py"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

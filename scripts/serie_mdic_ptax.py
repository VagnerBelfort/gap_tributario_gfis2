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
  uv run python scripts/serie_mdic_ptax.py --de 2025 --ate 2026 --trimestral

O snapshot soma DI e DUIMP (coluna `fonte`); a tabela mostra as duas parcelas.
O ano ainda aberto só é comparável por trimestre: `--trimestral` usa apenas os
trimestres que o MDIC já publicou por inteiro.
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
from typing import Dict, Tuple

COMEXSTAT_URL = "https://api-comexstat.mdic.gov.br/general"
PTAX_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    "?@dataInicial='{inicio}'&@dataFinalCotacao='{fim}'"
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


def _fob_ma(de: int, ate: int, mensal: bool) -> list:
    corpo = json.dumps(
        {
            "flow": "import",
            "monthDetail": mensal,
            "yearDetail": True,
            "period": {"from": f"{de}-01", "to": f"{ate}-12"},
            "filters": [],
            "details": ["state"],
            "metrics": ["metricFOB"],
        }
    ).encode()
    return [r for r in _get(COMEXSTAT_URL, corpo)["data"]["list"] if r["state"] == UF]


def importacoes_fob_usd(de: int, ate: int) -> Dict[int, Decimal]:
    """US$ FOB de importação do MA por ano, do ComexStat (UF do produto)."""
    return {int(r["year"]): Decimal(r["metricFOB"]) for r in _fob_ma(de, ate, mensal=False)}


def importacoes_fob_usd_trimestral(de: int, ate: int) -> Dict[Tuple[int, int], Decimal]:
    """US$ FOB por trimestre, só dos trimestres com os três meses publicados."""
    fob: Dict[Tuple[int, int], Decimal] = defaultdict(Decimal)
    meses: Dict[Tuple[int, int], set] = defaultdict(set)
    for r in _fob_ma(de, ate, mensal=True):
        mes = int(r["monthNumber"])
        chave = (int(r["year"]), (mes - 1) // 3 + 1)
        fob[chave] += Decimal(r["metricFOB"])
        meses[chave].add(mes)
    return {k: v for k, v in fob.items() if len(meses[k]) == 3}


def _ptax_media(inicio: str, fim: str) -> Decimal:
    valores = _get(PTAX_URL.format(inicio=inicio, fim=fim))["value"]
    total = sum(Decimal(str(v["cotacaoVenda"])) for v in valores)
    return total / Decimal(len(valores))


def ptax_media_venda(ano: int) -> Decimal:
    """Média anual das cotações de venda da PTAX — a ponta que o pipeline usa."""
    return _ptax_media(f"01-01-{ano}", f"12-31-{ano}")


def ptax_media_venda_trimestre(ano: int, trimestre: int) -> Decimal:
    """Média das cotações de venda da PTAX no trimestre."""
    inicio, fim = {1: ("01-01", "03-31"), 2: ("04-01", "06-30"),
                   3: ("07-01", "09-30"), 4: ("10-01", "12-31")}[trimestre]
    return _ptax_media(f"{inicio}-{ano}", f"{fim}-{ano}")


def importacoes_siscomex(snapshot: Path, trimestral: bool = False) -> Dict:
    """Soma o snapshot por ano (ou ano × trimestre), por fonte e critério de UF.

    `DI` e `DUIMP` são o domicílio fiscal do importador em cada declaração.
    `despacho` só existe para a DI: a DUIMP não traz UF de despacho.
    """
    somas: Dict = defaultdict(lambda: {"DI": Decimal(0), "DUIMP": Decimal(0), "despacho": Decimal(0)})
    with snapshot.open(encoding="utf-8") as f:
        for linha in csv.DictReader(f, delimiter=";"):
            ano = linha["ano"]
            if len(ano) != 4:  # anos digitados errado na origem (18, 202, 203...)
                continue
            chave = (int(ano), int(linha["trimestre"])) if trimestral else int(ano)
            valor = Decimal(linha["cif_brl"])
            fonte = linha.get("fonte") or "DI"
            if linha["uf_importador"] == "MA":
                somas[chave][fonte] += valor
            if linha["uf_despacho"] == "MA":
                somas[chave]["despacho"] += valor
    return dict(somas)


def _br(valor: Decimal, casas: int = 2) -> str:
    """Formata número no padrão brasileiro."""
    return f"{valor:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _pct(valor: Decimal) -> str:
    """Formata percentual com sinal, no padrão brasileiro: +2,4%."""
    return f"{valor:+.1f}%".replace(".", ",")


def _mediana(valores: list) -> Decimal:
    ordenados = sorted(valores)
    meio = len(ordenados) // 2
    if len(ordenados) % 2:
        return ordenados[meio]
    return (ordenados[meio - 1] + ordenados[meio]) / 2


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
    parser.add_argument(
        "--trimestral",
        action="store_true",
        help="Compara por trimestre (só os trimestres fechados no MDIC)",
    )
    args = parser.parse_args()

    if not args.snapshot.exists():
        print(f"Snapshot não encontrado: {args.snapshot}", file=sys.stderr)
        return 2

    if args.trimestral:
        fob = importacoes_fob_usd_trimestral(args.de, args.ate)
        ptax_de = lambda k: ptax_media_venda_trimestre(*k)  # noqa: E731
        rotulo = "Trimestre"
    else:
        fob = importacoes_fob_usd(args.de, args.ate)
        ptax_de = ptax_media_venda
        rotulo = "Ano"
    siscomex = importacoes_siscomex(args.snapshot, trimestral=args.trimestral)

    if args.formato == "markdown":
        print(
            f"| {rotulo} | MDIC US$ bi FOB | PTAX média | MDIC R$ bi | DI R$ bi | "
            "DUIMP R$ bi | Siscomex R$ bi (domicílio) | Δ vs MDIC | "
            "DI R$ bi (despacho) | Trânsito DI |"
        )
        print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    desvios = []
    for chave in sorted(fob):
        ano = chave[0] if args.trimestral else chave
        if not args.de <= ano <= args.ate or chave not in siscomex:
            continue
        rot = f"{chave[0]} T{chave[1]}" if args.trimestral else str(chave)
        ptax = ptax_de(chave)
        mdic = fob[chave] * ptax / Decimal("1e9")
        di = siscomex[chave]["DI"] / Decimal("1e9")
        duimp = siscomex[chave]["DUIMP"] / Decimal("1e9")
        dom = di + duimp
        desp = siscomex[chave]["despacho"] / Decimal("1e9")
        desvio = (dom - mdic) / mdic * Decimal("100")
        transito = (desp - di) / di * Decimal("100")
        desvios.append(desvio)

        if args.formato == "markdown":
            print(
                f"| {rot} | {_br(fob[chave] / Decimal('1e9'), 3)} | {_br(ptax, 4)} | "
                f"{_br(mdic)} | {_br(di)} | {_br(duimp)} | {_br(dom)} | {_pct(desvio)} | "
                f"{_br(desp)} | {_pct(transito)} |"
            )
        else:
            print(f"{rot};{fob[chave]};{ptax};{mdic};{di};{duimp};{dom};{desvio};{desp};{transito}")

    if desvios and args.formato == "markdown":
        print(
            f"\nFaixa observada: {_pct(min(desvios))} a {_pct(max(desvios))} "
            f"(mediana {_pct(_mediana(desvios))}) — conferir contra FAIXA_OBSERVADA "
            "em src/gap_tributario/engine/comparacao.py"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

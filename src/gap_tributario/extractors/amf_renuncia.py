"""Parser e reader da renúncia fiscal de ICMS — AMF Tabela 7 (LDO/MA).

Fonte do **policy gap** (gasto tributário legal) usado na decomposição do gap
(issue #4). Os 3 arquivos `docs/AMF-Tabela 7 *.xlsx` (vintages LDO-2022/2025/2026,
cobertura combinada 2022–2029) listam a renúncia de receita por TRIBUTO ×
MODALIDADE. Layout do export (calamine, header=None):

- col 0: TRIBUTO (célula mesclada — só na 1ª linha do bloco; precisa de ffill);
- col 1: MODALIDADE (Crédito Presumido, Isenção, Redução de Base de Cálculo...);
- col 3+: valores por ano (col 3 = base_year, col 4 = base_year+1, ...).

Fluxo de duas etapas (mesmo padrão do SIGDEF):

1. ``AmfRenunciaParser`` — lê os xlsx via calamine (dep lazy), soma as
   modalidades de **ICMS** (IPVA fora), escolhe a vintage por ano e materializa
   um CSV limpo versionado.
2. ``AmfRenunciaReader`` — lê o CSV limpo e devolve ``RenunciaFiscal`` por ano
   (total em R$ milhões + detalhe por modalidade). Ano sem renúncia → ``None``.
"""

from __future__ import annotations

import csv
import logging
from decimal import Decimal
from pathlib import Path
from typing import Optional

import polars as pl

from gap_tributario.models import Proveniencia, RenunciaFiscal

logger = logging.getLogger(__name__)

# Posições das colunas no export (lê-se por índice).
_POS_TRIBUTO = 0
_POS_MODALIDADE = 1
_POS_PRIMEIRO_ANO = 3  # col 3 = base_year

_TRIBUTO_ICMS = "ICMS"

# CSV limpo versionado (materializado uma única vez a partir dos xlsx).
_DADOS_PADRAO = Path(__file__).resolve().parent.parent / "data" / "amf_renuncia_icms_ma.csv"

_FATOR_MILHOES = Decimal("1000000")

# Mapa de vintages: ano → (arquivo xlsx, aba, base_year, rótulo da vintage).
# Cobertura combinada 2022–2029; nos overlaps (2026–2029) prefere-se a vintage
# mais recente (LDO-2026 sobre LDO-2025).
_ARQ_LDO2022 = "AMF-Tabela 7-LDO-2022 COM DETALHAMENTO-(estimat-22-23-24).xlsx"
_ARQ_LDO2025 = "AMF-Tabela 7 - para LDO-2025-COM DETALHAMENTO-a.xlsx"
_ARQ_LDO2026 = "AMF-Tabela 7 - para LDO-2026-COM DETALHAMENTO-i.xlsx"
_ABA_PLDO = "Renúncia Fiscal-PLDO-2026"

_VINTAGES: dict[int, tuple[str, str, int, str]] = {
    2022: (_ARQ_LDO2022, "Planilha1", 2022, "LDO-2022"),
    2023: (_ARQ_LDO2022, "Planilha1", 2022, "LDO-2022"),
    2024: (_ARQ_LDO2022, "Planilha1", 2022, "LDO-2022"),
    2025: (_ARQ_LDO2025, _ABA_PLDO, 2025, "LDO-2025"),
    2026: (_ARQ_LDO2026, _ABA_PLDO, 2026, "LDO-2026"),
    2027: (_ARQ_LDO2026, _ABA_PLDO, 2026, "LDO-2026"),
    2028: (_ARQ_LDO2026, _ABA_PLDO, 2026, "LDO-2026"),
    2029: (_ARQ_LDO2026, _ABA_PLDO, 2026, "LDO-2026"),
}


class AmfRenunciaParser:
    """Transform dos xlsx da AMF Tabela 7 para um CSV limpo de renúncia de ICMS."""

    def parse_planilha(self, raw: pl.DataFrame, *, base_year: int, ano: int) -> pl.DataFrame:
        """Extrai as modalidades de ICMS de um ano a partir do raw posicional.

        Aplica forward-fill no TRIBUTO (col 0, mesclada), filtra ``TRIBUTO==ICMS``
        e lê o valor do ano na coluna ``3 + (ano - base_year)``.

        Args:
            raw: DataFrame posicional como devolvido pelo calamine (header=None).
            base_year: ano da primeira coluna de valores (col 3).
            ano: ano desejado (deve estar dentro das colunas disponíveis).

        Returns:
            DataFrame com colunas modalidade (str) e valor_brl (float, R$ unitário).
        """
        cols = raw.columns
        col_valor = cols[_POS_PRIMEIRO_ANO + (ano - base_year)]

        return (
            raw.select(
                pl.col(cols[_POS_TRIBUTO]).cast(pl.Utf8).forward_fill().alias("tributo"),
                pl.col(cols[_POS_MODALIDADE]).cast(pl.Utf8).alias("modalidade"),
                pl.col(col_valor).cast(pl.Float64, strict=False).alias("valor_brl"),
            )
            .filter((pl.col("tributo") == _TRIBUTO_ICMS) & pl.col("valor_brl").is_not_null())
            .select("modalidade", "valor_brl")
        )

    def from_xlsx(self, xlsx_path: str | Path, *, sheet: str, base_year: int, ano: int) -> pl.DataFrame:
        """Lê uma aba do xlsx da AMF via calamine e devolve as modalidades de ICMS do ano.

        O calamine não é dependência de runtime (só usado nesta materialização
        única); por isso é importado de forma lazy.
        """
        import math  # noqa: PLC0415

        import pandas as pd  # noqa: PLC0415 — dependência opcional de materialização

        bruto = pd.ExcelFile(xlsx_path, engine="calamine").parse(sheet, header=None)
        schema = [str(i) for i in range(bruto.shape[1])]
        # Converte NaN → None para que o forward-fill do TRIBUTO (col mesclada)
        # funcione: cast(Utf8) de um NaN vira a string "NaN", que não é nula.
        rows = [
            [None if isinstance(v, float) and math.isnan(v) else v for v in row]
            for row in bruto.values.tolist()
        ]
        raw = pl.DataFrame(rows, schema=schema, orient="row")
        return self.parse_planilha(raw, base_year=base_year, ano=ano)

    def materializar_csv(
        self,
        docs_dir: str | Path,
        csv_path: str | Path = _DADOS_PADRAO,
    ) -> Path:
        """Materializa o CSV limpo de renúncia de ICMS varrendo todas as vintages.

        Para cada ano coberto (2022–2029) escolhe a vintage apropriada, soma as
        modalidades de ICMS e grava (ano, vintage, modalidade, valor_brl). Roda
        uma única vez para gerar o asset versionado consumido pelo reader.

        Returns:
            Path do CSV gravado.
        """
        docs = Path(docs_dir)
        registros: list[tuple[int, str, str, int]] = []

        for ano in sorted(_VINTAGES):
            arquivo, aba, base_year, vintage = _VINTAGES[ano]
            modalidades = self.from_xlsx(docs / arquivo, sheet=aba, base_year=base_year, ano=ano)
            for modalidade, valor in zip(modalidades["modalidade"], modalidades["valor_brl"]):
                registros.append((ano, vintage, str(modalidade), int(round(valor))))
            total = sum(r[3] for r in registros if r[0] == ano)
            logger.info(
                "AMF %d (%s): %d modalidades de ICMS, total R$ %.1f mi",
                ano,
                vintage,
                len(modalidades),
                total / 1e6,
            )

        destino = Path(csv_path)
        destino.parent.mkdir(parents=True, exist_ok=True)
        with destino.open("w", encoding="utf-8", newline="") as f:
            f.write("# Renúncia fiscal de ICMS-MA — AMF Tabela 7 (LDO/MA, fonte BI-Oracle-SEFAZ).\n")
            f.write("# Materializado dos 3 xlsx da AMF; vintage por ano (LDO-2022/2025/2026).\n")
            f.write("# valor_brl em R$ unitário; modalidades: Créd.Presumido, Isenção, Redução BC.\n")
            writer = csv.writer(f)
            writer.writerow(["ano", "vintage", "modalidade", "valor_brl"])
            writer.writerows(registros)

        logger.info("AMF: CSV limpo materializado (%d linhas) em %s", len(registros), destino)
        return destino


class AmfRenunciaReader:
    """Lê o CSV limpo da AMF e devolve a renúncia de ICMS por ano (R$ milhões)."""

    def __init__(self, data_path: Optional[Path] = None) -> None:
        """Inicializa o reader.

        Args:
            data_path: Caminho do CSV limpo (ano, vintage, modalidade, valor_brl).
                       Default: asset versionado empacotado com o código.
        """
        self.data_path = data_path or _DADOS_PADRAO

    def ler(self, ano: int) -> Optional[RenunciaFiscal]:
        """Lê a renúncia fiscal de ICMS do ano.

        Args:
            ano: ano de referência.

        Returns:
            RenunciaFiscal com total (R$ milhões), detalhe por modalidade e
            vintage; ou ``None`` se o ano não tem renúncia no asset (degrada
            para "só gap total" sem quebrar).
        """
        por_modalidade: dict[str, Decimal] = {}
        vintage = ""
        with self.data_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(filter(lambda ln: not ln.startswith("#"), f))
            for row in reader:
                if int(row["ano"]) != ano:
                    continue
                vintage = row["vintage"]
                valor_mi = Decimal(row["valor_brl"]) / _FATOR_MILHOES
                por_modalidade[row["modalidade"]] = valor_mi

        if not por_modalidade:
            logger.info("AMF: sem renúncia para %d — decomposição degrada p/ só gap total.", ano)
            return None

        total = sum(por_modalidade.values(), Decimal("0"))
        logger.info("Renúncia ICMS %d (%s): R$ %s milhões", ano, vintage, total)
        return RenunciaFiscal(ano=ano, total=total, por_modalidade=por_modalidade, vintage=vintage)

    def proveniencia(self, renuncia: RenunciaFiscal, data_extracao: str) -> Proveniencia:
        """Retorna a proveniência da renúncia fiscal usada na decomposição.

        Args:
            renuncia: RenunciaFiscal lida (carrega a vintage da LDO aplicada).
            data_extracao: data da extração no formato ISO (YYYY-MM-DD), stampada
                           pelo orquestrador (CLI).
        """
        return Proveniencia(
            variavel="Renúncia Fiscal (ICMS)",
            origem="AMF Tabela 7 (LDO/MA) — fonte BI-Oracle-SEFAZ-MA",
            fonte=f"Anexo de Metas Fiscais, Tabela 7 — vintage {renuncia.vintage}",
            data_extracao=data_extracao,
            observacoes="Estimativa prospectiva da LDO. Soma das modalidades de ICMS "
            "(Crédito Presumido + Isenção + Redução de Base de Cálculo); IPVA fora.",
        )

"""Parser e extrator SIGDEF — ICMS arrecadado por setor (CONFAZ/SIGDEF).

Fonte primária de ICMS arrecadado da cascata, à frente do GFIS2. O export
SIGDEF (`docs/20260204_por-setor.xls`) tem um **desalinhamento de colunas**: o
ICMS total real está na coluna de **posição 21** (rotulada erroneamente
`va_icms_outras` no cabeçalho), enquanto a coluna rotulada `va_icms_total`
(posição 22) está errada. Por isso lemos por POSIÇÃO, não por rótulo.

Fluxo de duas etapas:

1. ``SigdefParser`` (transform puro) — lê o `.xls`, corrige o offset, valida a
   sanidade nacional e materializa um **parquet limpo** versionado. Roda uma
   única vez (não a cada execução do pipeline).
2. ``SigdefIcmsExtractor`` — lê o parquet limpo, agrega mês→período e devolve o
   ICMS do Maranhão em milhões de R$.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path

import polars as pl

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo, Proveniencia

logger = logging.getLogger(__name__)

# Posições das colunas no export (lê-se por índice, ignorando os rótulos).
_POS_UF = 1
_POS_ANO = 3
_POS_MES = 4
_POS_ICMS_TOTAL = 21  # rotulada "va_icms_outras" — é o ICMS total REAL

# Faixa plausível da soma NACIONAL de ICMS num ano (R$ unitário). O ICMS Brasil
# é da ordem de centenas de bilhões; ler a coluna errada (offset) cai para
# dezenas de bilhões e dispara este guard. 2022 nacional real ≈ R$ 691 bi.
_SANIDADE_MIN_BRL = 400_000_000_000
_SANIDADE_MAX_BRL = 1_500_000_000_000

# UF de domicílio fiscal do projeto e fator de conversão R$ unitário → R$ milhões.
_UF_MA = "MA"
_FATOR_MILHOES = Decimal("1000000")

# Parquet limpo versionado (materializado uma única vez a partir do .xls SIGDEF).
_DADOS_PADRAO = Path(__file__).resolve().parent.parent / "data" / "sigdef_icms_ma.parquet"

# Meses de cada trimestre.
_MESES_TRIMESTRE = {1: (1, 2, 3), 2: (4, 5, 6), 3: (7, 8, 9), 4: (10, 11, 12)}


class SigdefParser:
    """Transform puro do export SIGDEF para um DataFrame limpo de ICMS."""

    def parse(
        self,
        raw: pl.DataFrame,
        *,
        ano_referencia: int = 2022,
        sanidade: bool = True,
    ) -> pl.DataFrame:
        """Converte o raw posicional do SIGDEF em (uf, ano, mes, icms_total).

        Lê as colunas por POSIÇÃO (não por rótulo), aplicando o fix de offset:
        o ICMS total real está na posição 21. As linhas de cabeçalho e de
        agregado são descartadas naturalmente (ano/uf não-numéricos viram nulo).

        Args:
            raw: DataFrame posicional como devolvido pelo calamine (header=None).
            ano_referencia: ano usado na checagem de sanidade nacional.
            sanidade: se True, valida a soma nacional do ano de referência.

        Returns:
            DataFrame limpo com colunas uf (str), ano (int), mes (int),
            icms_total (float, em R$ unitário).
        """
        cols = raw.columns
        limpo = raw.select(
            pl.col(cols[_POS_UF]).cast(pl.Utf8).alias("uf"),
            pl.col(cols[_POS_ANO]).cast(pl.Int32, strict=False).alias("ano"),
            pl.col(cols[_POS_MES]).cast(pl.Int32, strict=False).alias("mes"),
            pl.col(cols[_POS_ICMS_TOTAL]).cast(pl.Float64, strict=False).alias("icms_total"),
        ).drop_nulls(["uf", "ano", "mes", "icms_total"])

        if sanidade:
            self._validar_sanidade_nacional(limpo, ano_referencia)

        return limpo

    def from_xls(self, xls_path: str | Path, *, ano_referencia: int = 2022) -> pl.DataFrame:
        """Lê o `.xls` SIGDEF via calamine e devolve o DataFrame limpo.

        O calamine não é dependência de runtime do projeto (só usado nesta
        materialização única); por isso é importado de forma lazy aqui.

        Args:
            xls_path: Caminho do export SIGDEF (`docs/20260204_por-setor.xls`).
            ano_referencia: ano usado na checagem de sanidade nacional.

        Returns:
            DataFrame limpo (uf, ano, mes, icms_total).
        """
        import pandas as pd  # noqa: PLC0415 — dependência opcional de materialização

        bruto = pd.ExcelFile(xls_path, engine="calamine").parse(0, header=None)
        # Constrói o polars a partir das linhas (evita exigir pyarrow para
        # converter colunas object com tipos mistos do export).
        schema = [str(i) for i in range(bruto.shape[1])]
        raw = pl.DataFrame(bruto.values.tolist(), schema=schema, orient="row")
        return self.parse(raw, ano_referencia=ano_referencia)

    def materializar_parquet(
        self,
        xls_path: str | Path,
        parquet_path: str | Path = _DADOS_PADRAO,
        *,
        uf: str = _UF_MA,
        ano_referencia: int = 2022,
    ) -> Path:
        """Materializa o parquet limpo (somente a UF de interesse) a partir do `.xls`.

        Roda uma única vez para gerar o asset versionado consumido pelo
        ``SigdefIcmsExtractor``. A sanidade nacional é validada sobre o dado
        completo antes do recorte por UF.

        Returns:
            Path do parquet gravado.
        """
        limpo = self.from_xls(xls_path, ano_referencia=ano_referencia)
        recorte = limpo.filter(pl.col("uf") == uf).sort(["ano", "mes"])
        destino = Path(parquet_path)
        destino.parent.mkdir(parents=True, exist_ok=True)
        recorte.write_parquet(destino)
        logger.info(
            "SIGDEF: parquet limpo materializado (%d linhas, UF=%s) em %s",
            recorte.height,
            uf,
            destino,
        )
        return destino

    @staticmethod
    def _validar_sanidade_nacional(limpo: pl.DataFrame, ano_referencia: int) -> None:
        """Confere se a soma nacional do ano de referência é plausível (guard de offset)."""
        do_ano = limpo.filter(pl.col("ano") == ano_referencia)
        if do_ano.height == 0:
            logger.warning(
                "SIGDEF: ano de referência %d ausente — sanidade nacional não verificada.",
                ano_referencia,
            )
            return

        total_nacional = do_ano["icms_total"].sum()
        if not (_SANIDADE_MIN_BRL <= total_nacional <= _SANIDADE_MAX_BRL):
            raise ValueError(
                f"SIGDEF falhou na sanidade nacional: ICMS total {ano_referencia} = "
                f"R$ {total_nacional / 1e9:,.1f} bi, fora da faixa esperada "
                f"[{_SANIDADE_MIN_BRL / 1e9:.0f}, {_SANIDADE_MAX_BRL / 1e9:.0f}] bi. "
                f"Provável desalinhamento de colunas (ler ICMS total na posição "
                f"{_POS_ICMS_TOTAL}, não pelo rótulo va_icms_total)."
            )


class SigdefIcmsExtractor:
    """Extrator do ICMS arrecadado do MA a partir do parquet limpo do SIGDEF."""

    def __init__(self, data_path: Path | None = None) -> None:
        """Inicializa o extrator.

        Args:
            data_path: Caminho do parquet limpo (uf, ano, mes, icms_total em R$).
                       Default: asset versionado empacotado com o código.
        """
        self.data_path = data_path or _DADOS_PADRAO

    def extract(self, periodo: PeriodoCalculo) -> Decimal:
        """Extrai o ICMS arrecadado do MA para o período, em R$ milhões.

        Agrega os meses do período (todos, se anual; só os do trimestre, se
        trimestral) e soma o ICMS total. Ano fora da cobertura do SIGDEF ou
        parquet ausente levantam ExtractionError, disparando o fallback p/ GFIS2.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual).

        Returns:
            Decimal com o ICMS arrecadado em milhões de R$.

        Raises:
            ExtractionError: parquet ausente/corrompido ou sem dados no período.
        """
        if not self.data_path.exists():
            raise ExtractionError(
                f"Parquet limpo do SIGDEF não encontrado: '{self.data_path}'. "
                f"Caindo para a próxima fonte da cascata (GFIS2)."
            )

        try:
            lf = pl.scan_parquet(self.data_path).filter(
                (pl.col("uf") == _UF_MA) & (pl.col("ano") == periodo.ano)
            )
            if not periodo.is_anual:
                meses = _MESES_TRIMESTRE[periodo.trimestre]  # type: ignore[index]
                lf = lf.filter(pl.col("mes").is_in(meses))

            resultado = lf.select(
                pl.col("icms_total").sum().alias("total"),
                pl.len().alias("n"),
            ).collect()
        except pl.exceptions.PolarsError as exc:
            raise ExtractionError(
                f"Erro ao ler o parquet SIGDEF em '{self.data_path}': {exc}."
            ) from exc

        total_reais = resultado["total"].item()
        if resultado["n"].item() == 0 or total_reais is None:
            raise ExtractionError(
                f"SIGDEF não cobre o período {periodo.label} (UF=MA). "
                f"Caindo para a próxima fonte da cascata (GFIS2)."
            )

        icms_milhoes = Decimal(str(total_reais)) / _FATOR_MILHOES
        logger.info("ICMS SIGDEF %s (MA): R$ %s milhões", periodo.label, icms_milhoes)
        return icms_milhoes

    def proveniencia(self, data_extracao: str) -> Proveniencia:
        """Retorna a proveniência do ICMS arrecadado extraído desta fonte.

        Args:
            data_extracao: data da extração no formato ISO (YYYY-MM-DD), stampada
                           pelo orquestrador (CLI).
        """
        return Proveniencia(
            variavel="ICMS Arrecadado",
            origem="SIGDEF — ICMS arrecadado por setor (CONFAZ/SIGDEF)",
            fonte="Export SIGDEF por setor (ICMS total, posição 21); "
            "parquet limpo versionado",
            data_extracao=data_extracao,
            observacoes="Cobertura 1997–2023; fonte nº1 da cascata de ICMS, à frente "
            "do GFIS2 (fallback). Agregação mês→período.",
        )

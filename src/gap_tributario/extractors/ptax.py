"""Extrator da cotação do dólar via API BCB PTAX.

Responsável por consultar a API REST do Banco Central do Brasil
e retornar a cotação média PTAX para o período especificado.

Endpoint:
    GET https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/
        CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)

Retorno: Média aritmética de cotacaoVenda de todos os dias úteis do período
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Tuple

import httpx

from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

logger = logging.getLogger(__name__)

# Mapeamento trimestre → (mês_início, dia_início, mês_fim, dia_fim)
_TRIMESTRE_DATAS = {
    1: ("01", "01", "03", "31"),
    2: ("04", "01", "06", "30"),
    3: ("07", "01", "09", "30"),
    4: ("10", "01", "12", "31"),
}


class PTAXExtractor:
    """Extrator da cotação do dólar via API BCB PTAX."""

    BCB_BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"

    def __init__(self, timeout: int = 15, max_retries: int = 3) -> None:
        """Inicializa o extrator com configurações de timeout e retry.

        Args:
            timeout: Timeout em segundos para requisições à API (default: 15)
            max_retries: Número máximo de tentativas (default: 3)
        """
        self.timeout = timeout
        self.max_retries = max_retries

    def _build_date_range(self, periodo: PeriodoCalculo) -> Tuple[str, str]:
        """Converte um PeriodoCalculo para par de datas no formato MM-DD-YYYY.

        Args:
            periodo: Período de cálculo (trimestral ou anual)

        Returns:
            Tupla (data_inicio, data_fim) no formato 'MM-DD-YYYY'
        """
        ano = str(periodo.ano)

        if periodo.is_anual:
            data_inicio = f"01-01-{ano}"
            data_fim = f"12-31-{ano}"
        else:
            mes_ini, dia_ini, mes_fim, dia_fim = _TRIMESTRE_DATAS[periodo.trimestre]
            data_inicio = f"{mes_ini}-{dia_ini}-{ano}"
            data_fim = f"{mes_fim}-{dia_fim}-{ano}"

        return data_inicio, data_fim

    def _build_url(self, data_inicio: str, data_fim: str) -> str:
        """Monta a URL completa OData da BCB PTAX.

        Args:
            data_inicio: Data de início no formato 'MM-DD-YYYY'
            data_fim: Data de fim no formato 'MM-DD-YYYY'

        Returns:
            URL completa para a requisição à API BCB PTAX
        """
        endpoint = "CotacaoDolarPeriodo(dataInicial=@di,dataFinalCotacao=@df)"
        params = (
            f"?@di='{data_inicio}'"
            f"&@df='{data_fim}'"
            f"&$format=json"
            f"&$select=cotacaoVenda,dataHoraCotacao"
        )
        return f"{self.BCB_BASE_URL}{endpoint}{params}"

    def extract(self, periodo: PeriodoCalculo) -> Decimal:
        """Extrai a cotação média PTAX para o período.

        Consulta a API BCB PTAX e calcula a média aritmética de cotacaoVenda
        de todos os dias úteis do período. Implementa retry com backoff
        exponencial (1s, 2s, 4s) em caso de falha de rede ou HTTP error.

        Args:
            periodo: PeriodoCalculo (trimestral ou anual)

        Returns:
            Decimal com a cotação média PTAX em R$/USD

        Raises:
            ExtractionError: Se a API BCB estiver indisponível após retries
                             ou se não houver cotações para o período
        """
        data_inicio, data_fim = self._build_date_range(periodo)
        url = self._build_url(data_inicio, data_fim)

        logger.info(
            "Consultando PTAX BCB para período %s: %s a %s",
            periodo.label,
            data_inicio,
            data_fim,
        )

        for tentativa in range(1, self.max_retries + 1):
            try:
                response = httpx.get(url, timeout=self.timeout)
                response.raise_for_status()
                dados = response.json()
                break
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                if tentativa < self.max_retries:
                    espera = 2 ** (tentativa - 1)  # 1s, 2s, 4s
                    logger.warning(
                        "Tentativa %d/%d falhou para PTAX %s. Aguardando %ds. Erro: %s",
                        tentativa,
                        self.max_retries,
                        periodo.label,
                        espera,
                        exc,
                    )
                    time.sleep(espera)
                else:
                    raise ExtractionError(
                        f"BCB PTAX API indisponível para {periodo.label} após {self.max_retries} "
                        f"tentativas. Use --ptax-manual para informar o câmbio manualmente. "
                        f"Erro: {exc}"
                    ) from exc

        cotacoes = dados.get("value", [])
        if not cotacoes:
            raise ExtractionError(
                f"API BCB PTAX retornou lista vazia de cotações para {periodo.label} "
                f"({data_inicio} a {data_fim}). O período pode não ter dias úteis "
                f"ou os dados podem estar indisponíveis."
            )

        soma = sum(Decimal(str(c["cotacaoVenda"])) for c in cotacoes)
        ptax_media = soma / Decimal(len(cotacoes))

        logger.info(
            "PTAX média %s: R$ %.4f (%d cotações)",
            periodo.label,
            float(ptax_media),
            len(cotacoes),
        )

        return ptax_media

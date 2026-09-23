"""Testes do extrator de arrecadação GFIS2 Parquet.

Testa o ArrecadacaoExtractor com fixtures Parquet para garantir:
- Leitura e agregação correta das colunas ICMS
- Filtragem por período anual e trimestral
- Conversão de unidade (R$ unitário → R$ milhões)
- Tipo de retorno Decimal
- Tratamento correto de erros (path inexistente, Parquet corrompido, sem dados)
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from gap_tributario.extractors.arrecadacao import ArrecadacaoExtractor
from gap_tributario.extractors.base import ExtractionError
from gap_tributario.models import PeriodoCalculo

# Caminho para o fixture Parquet estático
FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "parquet"
FIXTURE_PARQUET = FIXTURE_DIR / "arrecadacao_fixture.parquet"

# Valores esperados calculados com base nos dados da fixture. A soma cobre TODAS
# as parcelas de ICMS presentes nela (a fixture não traz fcp/fdi/idh/fruicao):
# 2022 anual: normal=7.000.000 + imp=300.000 + st_sda=2.500.000 + st_ent=1.000.000
#             + tvi=100.000 + da=26.000 = 10.926.000 R$ = 10,926 R$ milhões
ICMS_2022_ANUAL_MILHOES = Decimal("10.926")

# T1/2022: normal=1.000.000 + imp=100.000 + st_sda=500.000 + st_ent=100.000
#          + tvi=10.000 + da=5.000 = 1.715.000 R$ = 1,715 R$ milhões
ICMS_2022_T1_MILHOES = Decimal("1.715")


# ---- Fixtures pytest ----


@pytest.fixture
def extractor(tmp_path: Path) -> ArrecadacaoExtractor:
    """Copia a fixture Parquet para tmp_path e retorna extrator configurado."""
    parquet_dir = tmp_path / "g_arrecadacao"
    parquet_dir.mkdir()
    # Copia o arquivo de fixture para o diretório temporário
    conteudo = FIXTURE_PARQUET.read_bytes()
    (parquet_dir / "arrecadacao_fixture.parquet").write_bytes(conteudo)
    return ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))


@pytest.fixture
def extractor_sem_dados_2022(tmp_path: Path) -> ArrecadacaoExtractor:
    """Cria extrator com Parquet que contém apenas dados de 2021 (sem 2022)."""
    parquet_dir = tmp_path / "g_arrecadacao_sem_2022"
    parquet_dir.mkdir()

    # Apenas dados de 2021
    df = pl.DataFrame(
        {
            "per_aaaa": [2021, 2021],
            "per_nro_trimestre": [1, 2],
            "val_icms_normal": [500_000.0, 600_000.0],
            "val_icms_imp": [50_000.0, 60_000.0],
            "val_icms_st_sda": [250_000.0, 300_000.0],
        },
        schema={
            "per_aaaa": pl.Int32,
            "per_nro_trimestre": pl.Int32,
            "val_icms_normal": pl.Float64,
            "val_icms_imp": pl.Float64,
            "val_icms_st_sda": pl.Float64,
        },
    )
    df.write_parquet(parquet_dir / "dados.parquet")
    return ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))


@pytest.fixture
def extractor_valores_zero(tmp_path: Path) -> ArrecadacaoExtractor:
    """Cria extrator com Parquet onde todos os valores ICMS são zero."""
    parquet_dir = tmp_path / "g_arrecadacao_zeros"
    parquet_dir.mkdir()

    df = pl.DataFrame(
        {
            "per_aaaa": [2022],
            "per_nro_trimestre": [1],
            "val_icms_normal": [0.0],
            "val_icms_imp": [0.0],
            "val_icms_st_sda": [0.0],
        },
        schema={
            "per_aaaa": pl.Int32,
            "per_nro_trimestre": pl.Int32,
            "val_icms_normal": pl.Float64,
            "val_icms_imp": pl.Float64,
            "val_icms_st_sda": pl.Float64,
        },
    )
    df.write_parquet(parquet_dir / "dados.parquet")
    return ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))


# ---- Testes básicos de importabilidade e instanciação ----


def test_arrecadacao_extractor_importable():
    """Verifica que o extrator é importável."""
    assert ArrecadacaoExtractor is not None


def test_arrecadacao_extractor_instantiable():
    """Verifica que o extrator pode ser instanciado."""
    extractor = ArrecadacaoExtractor(parquet_base_path="./test_path")
    assert extractor.parquet_base_path == "./test_path"


# ---- Testes de happy path — período anual ----


def test_extract_periodo_anual_retorna_valor_correto(extractor):
    """Fixture com dados 2022 (T1-T4) → soma anual = 10,926 M.

    Dados da fixture:
    - val_icms_normal:  7.000.000 R$ = 7,000 M
    - val_icms_imp:       300.000 R$ = 0,300 M
    - val_icms_st_sda:  2.500.000 R$ = 2,500 M
    - val_icms_st_ent:  1.000.000 R$ = 1,000 M
    - val_icms_tvi:       100.000 R$ = 0,100 M
    - val_icms_da:         26.000 R$ = 0,026 M
    - Total 2022:      10.926.000 R$ = 10,926 M
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    assert resultado == ICMS_2022_ANUAL_MILHOES


def test_extract_retorna_decimal_nao_float(extractor):
    """Verifica que o tipo de retorno é Decimal, não float."""
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert not isinstance(resultado, float)


def test_extract_valor_em_milhoes_nao_reais(extractor):
    """Verifica que o resultado está em milhões, não em R$ unitário.

    Total 2022 = 10.926.000 R$ → resultado deve ser ~10,9, não ~10.926.000.
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    # Deve ser ~9.8 M, não ~9.800.000
    assert resultado < Decimal("1000"), f"Valor muito alto ({resultado}): provavelmente em R$ unitário"
    assert resultado > Decimal("1"), f"Valor muito baixo ({resultado}): verificar conversão"
    assert resultado == Decimal("10.926")


# ---- Testes de happy path — período trimestral ----


def test_extract_periodo_trimestral_t1_retorna_valor_correto(extractor):
    """Fixture com dados T1/2022 → soma T1 = 1,715 M.

    Dados da fixture para T1:
    - val_icms_normal: 1.000.000 R$ = 1,000 M
    - val_icms_imp:      100.000 R$ = 0,100 M
    - val_icms_st_sda:   500.000 R$ = 0,500 M
    - val_icms_st_ent:   100.000 R$ = 0,100 M
    - val_icms_tvi:       10.000 R$ = 0,010 M
    - val_icms_da:         5.000 R$ = 0,005 M
    - Total T1/2022:   1.715.000 R$ = 1,715 M
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1))

    assert resultado == ICMS_2022_T1_MILHOES


def test_extract_trimestral_retorna_apenas_dados_do_trimestre(extractor):
    """Verifica que a filtragem trimestral não inclui outros trimestres.

    T1/2022 (1,715 M) < total anual confirma que está filtrando corretamente.
    """
    resultado_t1 = extractor.extract(PeriodoCalculo(ano=2022, trimestre=1))
    resultado_anual = extractor.extract(PeriodoCalculo(ano=2022))

    # T1 deve ser menor que o total anual
    assert resultado_t1 < resultado_anual

    # T1 deve ser exatamente o valor do trimestre 1, não 1/4 do anual
    assert resultado_t1 == ICMS_2022_T1_MILHOES
    assert resultado_anual == ICMS_2022_ANUAL_MILHOES


def test_extract_filtragem_exclui_dados_de_outros_anos(extractor):
    """Verifica que dados de 2021 não interferem no resultado de 2022."""
    resultado_2022 = extractor.extract(PeriodoCalculo(ano=2022))

    # O fixture contém dados de 2021 também; se não filtrar corretamente,
    # o resultado seria maior que 9.8 M
    assert resultado_2022 == ICMS_2022_ANUAL_MILHOES


# ---- Testes de valores zero ----


def test_extract_valores_icms_zero_retorna_decimal_zero(extractor_valores_zero):
    """Fixture com todos os valores ICMS = 0 → deve retornar Decimal('0'), não erro."""
    resultado = extractor_valores_zero.extract(PeriodoCalculo(ano=2022))

    assert isinstance(resultado, Decimal)
    assert resultado == Decimal("0")


# ---- Testes de erro — path inexistente ----


def test_extract_path_inexistente_levanta_extraction_error():
    """ArrecadacaoExtractor com path que não existe → deve levantar ExtractionError."""
    extractor = ArrecadacaoExtractor(parquet_base_path="/caminho/que/nao/existe/parquet")

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022))

    mensagem = str(exc_info.value)
    assert "não encontrado" in mensagem or "não existe" in mensagem or "not found" in mensagem.lower()
    assert "/caminho/que/nao/existe/parquet" in mensagem


def test_extract_path_inexistente_menciona_configuracao():
    """Mensagem de erro deve orientar o usuário a verificar aliquotas.yaml."""
    extractor = ArrecadacaoExtractor(parquet_base_path="/path/invalido")

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022))

    # Mensagem deve orientar o usuário a verificar a configuração
    assert "aliquotas.yaml" in str(exc_info.value) or "parquet_base_path" in str(exc_info.value)


# ---- Testes de erro — sem dados para o período ----


def test_extract_sem_dados_para_periodo_levanta_extraction_error(extractor_sem_dados_2022):
    """Parquet com apenas dados de 2021 e consulta de 2022 → ExtractionError."""
    with pytest.raises(ExtractionError) as exc_info:
        extractor_sem_dados_2022.extract(PeriodoCalculo(ano=2022))

    mensagem = str(exc_info.value)
    assert "2022" in mensagem


def test_extract_sem_dados_para_trimestre_levanta_extraction_error(extractor_sem_dados_2022):
    """Parquet sem dados de 2022 e consulta T1/2022 → ExtractionError."""
    with pytest.raises(ExtractionError) as exc_info:
        extractor_sem_dados_2022.extract(PeriodoCalculo(ano=2022, trimestre=1))

    assert "2022" in str(exc_info.value)


# ---- Testes de Parquet corrompido ----


def test_extract_parquet_corrompido_levanta_extraction_error(tmp_path):
    """Arquivo com extensão .parquet mas conteúdo inválido → ExtractionError."""
    parquet_dir = tmp_path / "g_arrecadacao_corrompido"
    parquet_dir.mkdir()

    # Cria arquivo com conteúdo inválido (não é um Parquet válido)
    arquivo_invalido = parquet_dir / "corrompido.parquet"
    arquivo_invalido.write_bytes(b"conteudo invalido que nao eh parquet")

    extractor = ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))

    with pytest.raises(ExtractionError) as exc_info:
        extractor.extract(PeriodoCalculo(ano=2022))

    mensagem = str(exc_info.value)
    assert "Erro ao ler" in mensagem or "corrompido" in mensagem or "parquet" in mensagem.lower()


# ---- Testes de conversão de unidade ----


def test_extract_conversao_unidade_reais_para_milhoes(extractor):
    """Verificação explícita da conversão: soma bruta é 10.926.000 → resultado 10,926.

    Somando todas as parcelas de ICMS da fixture, trimestre a trimestre:
    - T1: 1M + 100k + 500k + 100k + 10k + 5k = 1,715M
    - T2: 2M + 200k + 1M   + 200k + 20k + 6k = 3,426M
    - T3: 3M + 0    + 500k + 300k + 30k + 7k = 3,837M
    - T4: 1M + 0    + 500k + 400k + 40k + 8k = 1,948M
    Total bruto: 10.926.000 R$ → 10,926 R$ milhões
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    # Verificação explícita: 10926000 / 1000000 = 10,926
    assert resultado == Decimal("10926000") / Decimal("1000000")
    assert resultado == Decimal("10.926")


# ---- Testes de composição do ICMS total ----
#
# O GFIS2 registra a arrecadação de ICMS repartida em várias colunas. Somar só
# `normal + imp + st_sda` deixa de fora FCP, dívida ativa, ST entrada e TVI, o
# que subestimava o total em 10-15% frente ao SIGDEF/CONFAZ. As colunas são
# mutuamente exclusivas por linha, então somá-las não duplica arrecadação.


@pytest.fixture
def extractor_todas_parcelas(tmp_path: Path) -> ArrecadacaoExtractor:
    """Parquet com todas as parcelas de ICMS mais tributos que NÃO são ICMS."""
    parquet_dir = tmp_path / "g_arrecadacao_completo"
    parquet_dir.mkdir()

    df = pl.DataFrame(
        {
            "per_aaaa": [2022],
            "per_nro_trimestre": [1],
            # Parcelas de ICMS — devem entrar na soma (total 5.500.000)
            "val_icms_normal": [1_000_000.0],
            "val_icms_imp": [100_000.0],
            "val_icms_st_sda": [200_000.0],
            "val_icms_st_ent": [300_000.0],
            "val_icms_da": [400_000.0],
            "val_icms_tvi": [500_000.0],
            "val_fcp": [600_000.0],
            "val_fdi": [700_000.0],
            "val_idh": [800_000.0],
            "val_fruicao_ben_fiscal": [900_000.0],
            # NÃO são ICMS — devem ficar de fora
            "val_ipva": [50_000_000.0],
            "val_itcd": [60_000_000.0],
            "val_outros": [70_000_000.0],
            "val_juros": [80_000_000.0],
            "val_multa": [90_000_000.0],
            # Totalizadores de linha — somariam tudo de novo
            "val_principal": [99_000_000.0],
            "val_pgto_total": [99_000_000.0],
        }
    )
    df.write_parquet(parquet_dir / "dados.parquet")
    return ArrecadacaoExtractor(parquet_base_path=str(parquet_dir))


def test_extract_soma_todas_as_parcelas_de_icms(extractor_todas_parcelas):
    """Todas as parcelas de ICMS entram na soma: 5.500.000 R$ = 5.5 M."""
    resultado = extractor_todas_parcelas.extract(PeriodoCalculo(ano=2022))

    assert resultado == Decimal("5.5")


def test_extract_ignora_tributos_que_nao_sao_icms(extractor_todas_parcelas):
    """IPVA, ITCD, juros, multa e os totalizadores de linha ficam de fora.

    Se qualquer um entrasse, o resultado passaria de 50 M.
    """
    resultado = extractor_todas_parcelas.extract(PeriodoCalculo(ano=2022))

    assert resultado < Decimal("50")


def test_extract_tolera_parquet_sem_as_colunas_novas(extractor):
    """Parquet sem fcp/fdi/idh/fruicao continua somando as parcelas que existem.

    O schema do GFIS2 evolui; a ausência de uma parcela não pode quebrar a
    extração nem zerar o total.
    """
    resultado = extractor.extract(PeriodoCalculo(ano=2022))

    assert resultado == ICMS_2022_ANUAL_MILHOES

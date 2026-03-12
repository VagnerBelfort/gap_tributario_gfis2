"""Interface de linha de comando (CLI) para a Calculadora de Gap Tributário.

Parsing de argumentos via argparse e orquestração do pipeline.

Códigos de saída:
    0  Sucesso
    1  Erro de validação (Pandera)
    2  Erro de extração (fonte indisponível)
    3  Erro de configuração
    4  Erro de argumento CLI
"""

from __future__ import annotations

import argparse
import logging
import sys

FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Cria e configura o parser de argumentos CLI."""
    from gap_tributario import __version__

    parser = argparse.ArgumentParser(
        prog="gap-tributario",
        description="Calculadora de Gap Tributário do ICMS-MA (Metodologia VRR/OCDE)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python -m gap_tributario --periodo 2022
  python -m gap_tributario --periodo 2022-T1 --formato pdf --saida ./relatorios/
  python -m gap_tributario --periodo 2023 --formato pdf excel --siscomex
        """,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    parser.add_argument(
        "--periodo",
        type=str,
        required=False,
        help='Período de cálculo. Formatos: "2022" (anual) ou "2022-T1" (trimestral)',
    )

    parser.add_argument(
        "--formato",
        nargs="+",
        choices=["pdf", "excel"],
        default=["pdf", "excel"],
        metavar="FORMATO",
        help="Formato(s) de saída: pdf, excel (default: pdf excel)",
    )

    parser.add_argument(
        "--saida",
        type=str,
        default="./output/",
        metavar="DIRETORIO",
        help="Diretório de saída (default: ./output/)",
    )

    parser.add_argument(
        "--config",
        type=str,
        default="./config/aliquotas.yaml",
        metavar="CONFIG",
        help="Caminho do arquivo de configuração (default: ./config/aliquotas.yaml)",
    )

    parser.add_argument(
        "--siscomex",
        action="store_true",
        default=False,
        help="Habilitar enriquecimento via Oracle Siscomex (requer credenciais)",
    )

    parser.add_argument(
        "--ptax-manual",
        type=float,
        metavar="VALOR",
        help="Cotação PTAX manual em R$/USD (substitui consulta à API BCB)",
    )

    parser.add_argument(
        "--vab-manual",
        type=float,
        metavar="VALOR",
        help="VAB manual em milhões R$ (substitui consulta à API IBGE SIDRA)",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Logging detalhado (DEBUG)",
    )

    return parser


def run() -> int:
    """Executa o pipeline completo de 7 estágios.

    Returns:
        Código de saída:
            0  Sucesso
            1  Erro de validação (Pandera / DadosVRR)
            2  Erro de extração (fonte indisponível / OSError relatório)
            3  Erro de configuração (YAML inválido / alíquota não encontrada)
            4  Erro de argumento CLI (período inválido)
    """
    parser = create_parser()
    args = parser.parse_args()

    if args.periodo is None:
        parser.print_help()
        return 0

    # === Estágio 6.6: Configurar Logging ===
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format=FORMAT)

    logger.info("=== Calculadora Gap Tributário ICMS-MA ===")

    # Imports lazy para não impactar tempo de --help/--version
    from decimal import Decimal
    from pathlib import Path

    from gap_tributario.config import load_config
    from gap_tributario.engine.vrr import MotorVRR
    from gap_tributario.extractors.arrecadacao import ArrecadacaoExtractor
    from gap_tributario.extractors.base import ExtractionError
    from gap_tributario.extractors.comex import ComexExtractor
    from gap_tributario.extractors.ibge import IBGEExtractor
    from gap_tributario.extractors.ptax import PTAXExtractor
    from gap_tributario.extractors.siscomex import SiscomexExtractor
    from gap_tributario.models import DadosVRR, PeriodoCalculo
    from gap_tributario.report.excel import ExcelReport
    from gap_tributario.report.pdf import PDFReport

    # === Estágio 1: CLI PARSE — PeriodoCalculo ===
    try:
        periodo = PeriodoCalculo.from_string(args.periodo)
    except ValueError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 4

    logger.info("Período: %s", periodo.label)

    # === Estágio 2: CONFIG LOAD ===
    try:
        config = load_config(args.config)
    except FileNotFoundError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 3
    except ValueError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 3

    # Override do diretório de saída via --saida
    config.output_path = Path(args.saida)

    try:
        aliquota = config.get_aliquota(periodo)
    except ValueError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 3

    logger.info("Alíquota: %s (Lei vigente para %d)", aliquota, periodo.ano)
    logger.info("--- Extração ---")

    # === Estágio 3: EXTRACT ===

    # 3a. BCB PTAX — cotação média do dólar
    try:
        if args.ptax_manual is not None:
            ptax_media = Decimal(str(args.ptax_manual))
            logger.info("PTAX manual (override): R$ %s/USD", ptax_media)
        else:
            ptax_media = PTAXExtractor().extract(periodo)
            logger.info("PTAX média %s: R$ %s/USD", periodo.label, ptax_media)
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3b. IBGE SIDRA — Valor Adicionado Bruto
    try:
        if args.vab_manual is not None:
            vab = Decimal(str(args.vab_manual))
            logger.info("VAB manual (override): R$ %s milhões", vab)
        else:
            vab = IBGEExtractor().extract(periodo)
            logger.info("VAB MA %s: R$ %s milhões", periodo.label, vab)
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3c. GFIS2 Parquet — ICMS arrecadado
    try:
        icms_arrecadado = ArrecadacaoExtractor(str(config.parquet_base_path)).extract(periodo)
        logger.info("ICMS arrecadado %s: R$ %s milhões", periodo.label, icms_arrecadado)
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3d. MDIC ComEx — exportações e importações
    try:
        exportacoes_brl, importacoes_brl = ComexExtractor(str(config.mdic_base_path)).extract(
            periodo, ptax_media
        )
        logger.info("Exportações MA %s: R$ %s milhões", periodo.label, exportacoes_brl)
        logger.info("Importações MA %s: R$ %s milhões", periodo.label, importacoes_brl)
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3e. Oracle Siscomex — enriquecimento opcional
    if args.siscomex:
        if not config.oracle_dsn:
            logger.warning(
                "--siscomex habilitado mas oracle_dsn não configurado em %s. "
                "Ignorando enriquecimento Siscomex.",
                args.config,
            )
        else:
            try:
                SiscomexExtractor(
                    config.oracle_dsn,
                    config.oracle_user or "",
                    config.oracle_password or "",
                ).extract(periodo)
            except (ExtractionError, NotImplementedError) as exc:
                print(f"Erro: {exc}", file=sys.stderr)
                return 2

    # === Estágio 4: VALIDATE — Construir DadosVRR ===
    try:
        dados_vrr = DadosVRR(
            periodo=periodo,
            icms_arrecadado=icms_arrecadado,
            vab=vab,
            exportacoes_brl=exportacoes_brl,
            importacoes_brl=importacoes_brl,
            aliquota_padrao=aliquota,
            ptax_media=ptax_media,
        )
    except Exception as exc:
        print(f"Erro de validação: {exc}", file=sys.stderr)
        return 1

    # === Estágio 5: CALCULATE ===
    try:
        motor = MotorVRR()
        resultado = motor.calcular(dados_vrr)
        logger.info("--- Cálculo VRR ---")
        logger.info("ICMS Potencial: R$ %s milhões", resultado.icms_potencial)
        logger.info("VRR: %s", resultado.vrr)
        logger.info("Gap Absoluto: R$ %s milhões", resultado.gap_absoluto)
        logger.info("Gap Percentual: %s%%", resultado.gap_percentual)
    except ValueError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 1

    logger.info("--- Relatório ---")

    # === Estágio 6: REPORT ===
    arquivos_gerados = []
    for formato in args.formato:
        try:
            if formato == "pdf":
                arquivo = PDFReport().gerar(resultado, dados_vrr, config, config.output_path)
            else:  # formato == "excel"
                arquivo = ExcelReport().gerar(resultado, dados_vrr, config, config.output_path)
            arquivos_gerados.append(arquivo)
            logger.info("Relatório gerado: %s", arquivo)
        except OSError as exc:
            print(
                f"Erro: não foi possível criar arquivo em {config.output_path}: {exc}",
                file=sys.stderr,
            )
            return 2

    # === Estágio 7: OUTPUT ===
    for arquivo in arquivos_gerados:
        print(arquivo)

    logger.info("=== Concluído com sucesso ===")
    return 0


if __name__ == "__main__":
    sys.exit(run())

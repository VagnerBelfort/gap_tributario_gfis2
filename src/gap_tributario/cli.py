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
  python -m gap_tributario --periodo 2023 --formato pdf excel
  python -m gap_tributario --periodo 2022 --imp-incluir-ni  # sensibilidade
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
        "--imp-incluir-ni",
        action="store_true",
        default=False,
        help=(
            "Somar às importações as DIs cujo importador não pôde ser atribuído "
            "a uma UF (teto da análise de sensibilidade)"
        ),
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
        "--exp-manual",
        type=float,
        metavar="VALOR",
        help="Exportações manuais em milhões R$ (substitui leitura dos CSVs MDIC)",
    )

    parser.add_argument(
        "--imp-manual",
        type=float,
        metavar="VALOR",
        help="Importações manuais em milhões R$ (substitui leitura dos CSVs MDIC)",
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
    from datetime import date
    from decimal import Decimal
    from pathlib import Path

    from gap_tributario.config import load_config
    from gap_tributario.engine.gap_decomposition import decompor_gap
    from gap_tributario.engine.vrr import MotorVRR
    from gap_tributario.extractors.amf_renuncia import AmfRenunciaReader
    from gap_tributario.extractors.arrecadacao import ArrecadacaoExtractor
    from gap_tributario.extractors.base import ExtractionError
    from gap_tributario.extractors.comex import ComexExtractor
    from gap_tributario.extractors.ibge import IBGEExtractor
    from gap_tributario.extractors.imesc_pib import ImescPibExtractor
    from gap_tributario.extractors.ptax import PTAXExtractor
    from gap_tributario.extractors.sigdef import SigdefIcmsExtractor
    from gap_tributario.extractors.siscomex import SiscomexSnapshotExtractor
    from gap_tributario.models import (
        ComparacaoFonte,
        DadosVRR,
        PeriodoCalculo,
        Proveniencia,
    )
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

    # Proveniência: cada variável registra a fonte que efetivamente venceu a
    # cascata (issue #5). A data de extração é stampada uma única vez.
    data_extracao = date.today().isoformat()
    proveniencias: list = []

    # 3a. BCB PTAX — cotação média do dólar
    try:
        if args.ptax_manual is not None:
            ptax_media = Decimal(str(args.ptax_manual))
            logger.info("PTAX manual (override): R$ %s/USD", ptax_media)
            proveniencias.append(
                Proveniencia(
                    variavel="Câmbio (PTAX)",
                    origem="Manual (--ptax-manual)",
                    fonte="Cotação informada via CLI",
                    data_extracao=data_extracao,
                    observacoes="Override manual do operador.",
                )
            )
        else:
            ptax_media = PTAXExtractor().extract(periodo)
            logger.info("PTAX média %s: R$ %s/USD", periodo.label, ptax_media)
            proveniencias.append(
                Proveniencia(
                    variavel="Câmbio (PTAX)",
                    origem="BCB — Banco Central do Brasil",
                    fonte="API Olinda PTAX (cotação média de venda do período)",
                    data_extracao=data_extracao,
                )
            )
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3b. VAB — cascata: --vab-manual → IMESC PIB Trimestral → IBGE SIDRA
    try:
        if args.vab_manual is not None:
            vab = Decimal(str(args.vab_manual))
            logger.info("VAB manual (override): R$ %s milhões", vab)
            proveniencias.append(
                Proveniencia(
                    variavel="VAB",
                    origem="Manual (--vab-manual)",
                    fonte="Valor informado via CLI",
                    data_extracao=data_extracao,
                    observacoes="Override manual do operador.",
                )
            )
        else:
            imesc = ImescPibExtractor()
            try:
                vab = imesc.extract(periodo)
                logger.info("VAB MA %s (IMESC): R$ %s milhões", periodo.label, vab)
                proveniencias.append(imesc.proveniencia(data_extracao))
            except ExtractionError as exc_imesc:
                logger.info(
                    "IMESC indisponível para %s (%s). Caindo para IBGE SIDRA.",
                    periodo.label,
                    exc_imesc,
                )
                vab = IBGEExtractor().extract(periodo)
                logger.info("VAB MA %s (IBGE fallback): R$ %s milhões", periodo.label, vab)
                proveniencias.append(
                    Proveniencia(
                        variavel="VAB",
                        origem="IBGE SIDRA (fallback da cascata)",
                        fonte="Contas Regionais, Tabela 5938 (PIB × 0,8932)",
                        data_extracao=data_extracao,
                        observacoes="Estimativa de VAB a partir do PIB; lag de ~2 anos.",
                    )
                )
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3c. ICMS arrecadado — cascata: SIGDEF (parquet limpo) → GFIS2 Parquet
    try:
        sigdef = SigdefIcmsExtractor()
        try:
            icms_arrecadado = sigdef.extract(periodo)
            logger.info("ICMS arrecadado %s (SIGDEF): R$ %s milhões", periodo.label, icms_arrecadado)
            proveniencias.append(sigdef.proveniencia(data_extracao))
        except ExtractionError as exc_sigdef:
            logger.info(
                "SIGDEF indisponível para %s (%s). Caindo para GFIS2.",
                periodo.label,
                exc_sigdef,
            )
            icms_arrecadado = ArrecadacaoExtractor(str(config.parquet_base_path)).extract(periodo)
            logger.info(
                "ICMS arrecadado %s (GFIS2 fallback): R$ %s milhões", periodo.label, icms_arrecadado
            )
            proveniencias.append(
                Proveniencia(
                    variavel="ICMS Arrecadado",
                    origem="GFIS2/SEFAZ-MA (fallback da cascata)",
                    fonte="Parquet GFIS2 (val_icms_normal + val_icms_imp + val_icms_st)",
                    data_extracao=data_extracao,
                )
            )
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3d. MDIC ComEx — exportações e importações
    try:
        if args.exp_manual is not None and args.imp_manual is not None:
            exportacoes_brl = Decimal(str(args.exp_manual))
            importacoes_brl = Decimal(str(args.imp_manual))
            logger.info("Exportações manual (override): R$ %s milhões", exportacoes_brl)
            logger.info("Importações manual (override): R$ %s milhões", importacoes_brl)
        elif args.exp_manual is not None or args.imp_manual is not None:
            # Caso parcial: buscar o dado faltante via MDIC
            exp_mdic, imp_mdic = ComexExtractor(str(config.mdic_base_path)).extract(
                periodo, ptax_media
            )
            exportacoes_brl = (
                Decimal(str(args.exp_manual)) if args.exp_manual is not None else exp_mdic
            )
            importacoes_brl = (
                Decimal(str(args.imp_manual)) if args.imp_manual is not None else imp_mdic
            )
            logger.info("Exportações MA %s: R$ %s milhões%s", periodo.label, exportacoes_brl,
                        " (manual)" if args.exp_manual is not None else "")
            logger.info("Importações MA %s: R$ %s milhões%s", periodo.label, importacoes_brl,
                        " (manual)" if args.imp_manual is not None else "")
        else:
            exportacoes_brl, importacoes_brl = ComexExtractor(str(config.mdic_base_path)).extract(
                periodo, ptax_media
            )
            logger.info("Exportações MA %s: R$ %s milhões", periodo.label, exportacoes_brl)
            logger.info("Importações MA %s: R$ %s milhões", periodo.label, importacoes_brl)
    except ExtractionError as exc:
        print(f"Erro: {exc}", file=sys.stderr)
        return 2

    # 3d-bis. Siscomex é a fonte nº1 das importações: só ele traz o domicílio
    # fiscal do importador, que separa a importação maranhense da carga em
    # trânsito por Itaqui. Sem snapshot ou sem cobertura no ano, cai para o MDIC.
    fonte_importacoes = "MDIC ComEx"
    comparacao_fontes: list = []
    importacoes_mdic = importacoes_brl
    if args.imp_manual is None:
        try:
            importacoes_brl = SiscomexSnapshotExtractor(
                config.siscomex_snapshot_path,
                incluir_nao_identificado=args.imp_incluir_ni,
            ).extract(periodo)
            fonte_importacoes = "Siscomex (SEFAZ-MA)"
            logger.info(
                "Importações MA %s (Siscomex): R$ %s milhões",
                periodo.label,
                importacoes_brl,
            )
            # A troca de fonte move o gap de forma material: o relatório mostra
            # as duas leituras, em vez de só a vencedora da cascata.
            comparacao_fontes = [
                ComparacaoFonte(
                    variavel="Importações",
                    fonte="Siscomex (SEFAZ-MA)",
                    valor_brl=importacoes_brl,
                    observacoes=(
                        "Valor aduaneiro (CIF) por domicílio fiscal do importador "
                        "(TDS_UF_IMPORTADOR), excluindo trânsito por Itaqui."
                    ),
                ),
                ComparacaoFonte(
                    variavel="Importações",
                    fonte="MDIC ComEx",
                    valor_brl=importacoes_mdic,
                    observacoes=(
                        "FOB convertido pela PTAX. SG_UF_NCM é local de desembaraço, "
                        "então mistura importação do MA com carga em trânsito."
                    ),
                ),
            ]
        except ExtractionError as exc:
            logger.warning("Siscomex indisponível (%s). Mantendo MDIC ComEx.", exc)

    for variavel, manual in (
        ("Exportações", args.exp_manual),
        ("Importações", args.imp_manual),
    ):
        if manual is not None:
            proveniencias.append(
                Proveniencia(
                    variavel=variavel,
                    origem="Manual (--exp-manual/--imp-manual)",
                    fonte="Valor informado via CLI",
                    data_extracao=data_extracao,
                    observacoes="Override manual do operador.",
                )
            )
        elif variavel == "Importações" and fonte_importacoes.startswith("Siscomex"):
            proveniencias.append(
                Proveniencia(
                    variavel=variavel,
                    origem="Siscomex (SEFAZ-MA)",
                    fonte="APL_SISCOMEX (Oracle C3) — snapshot agregado; TDS_UF_IMPORTADOR=MA",
                    data_extracao=data_extracao,
                    observacoes=(
                        "Valor aduaneiro (CIF) por domicílio fiscal do importador, "
                        "excluindo carga em trânsito por Itaqui. DIs sem UF atribuída "
                        + ("incluídas" if args.imp_incluir_ni else "excluídas")
                        + " (~5% do total em 2022)."
                    ),
                )
            )
        else:
            proveniencias.append(
                Proveniencia(
                    variavel=variavel,
                    origem="MDIC ComEx",
                    fonte="ComexStat (VL_FOB USD × PTAX → BRL); filtro SG_UF_NCM=MA",
                    data_extracao=data_extracao,
                    observacoes=(
                        "FOB em USD convertido pela PTAX média do período. "
                        "SG_UF_NCM é local de desembaraço, não domicílio fiscal."
                    ),
                )
            )

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

    # === Estágio 5.5: DECOMPOSIÇÃO (policy vs compliance) — aditivo, opcional ===
    # A renúncia da AMF é anual; só decompomos períodos anuais. Ano sem renúncia
    # degrada para "só gap total" sem quebrar.
    decomposicao = None
    if periodo.is_anual:
        amf = AmfRenunciaReader()
        renuncia = amf.ler(periodo.ano)
        if renuncia is not None:
            decomposicao = decompor_gap(resultado, renuncia)
            proveniencias.append(amf.proveniencia(renuncia, data_extracao))
            logger.info(
                "Decomposição (%s): policy=R$ %s mi, compliance=R$ %s mi%s",
                renuncia.vintage,
                decomposicao.policy_gap,
                decomposicao.compliance_gap,
                " [ATENÇÃO: renúncia > gap]" if decomposicao.compliance_negativo else "",
            )
    else:
        logger.info("Período trimestral: decomposição (renúncia anual) omitida.")

    logger.info("--- Relatório ---")

    # === Estágio 6: REPORT ===
    arquivos_gerados = []
    for formato in args.formato:
        try:
            if formato == "pdf":
                arquivo = PDFReport().gerar(
                    resultado,
                    dados_vrr,
                    config,
                    config.output_path,
                    decomposicao,
                    proveniencias,
                    comparacao_fontes,
                )
            else:  # formato == "excel"
                arquivo = ExcelReport().gerar(
                    resultado,
                    dados_vrr,
                    config,
                    config.output_path,
                    decomposicao,
                    proveniencias,
                    comparacao_fontes,
                )
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

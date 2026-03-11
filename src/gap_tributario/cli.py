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
import sys


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
    """Executa o pipeline completo.

    Returns:
        Código de saída (0=sucesso, 1-4=erro).
    """
    parser = create_parser()
    args = parser.parse_args()

    if args.periodo is None:
        parser.print_help()
        return 0

    # TODO: Implementado na tarefa de CLI e orquestração do pipeline
    print(f"Gap Tributário ICMS-MA — Período: {args.periodo}")
    print("Pipeline não implementado ainda. Execute após as tarefas subsequentes.")
    return 0


if __name__ == "__main__":
    sys.exit(run())

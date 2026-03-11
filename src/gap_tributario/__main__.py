"""Entrypoint para execução via `python -m gap_tributario`."""

from __future__ import annotations

import sys


def main() -> None:
    """Ponto de entrada principal da aplicação."""
    from gap_tributario.cli import run

    sys.exit(run())


if __name__ == "__main__":
    main()

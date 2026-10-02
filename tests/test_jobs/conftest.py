"""Os jobs do cluster não são pacote: são copiados soltos para o servidor."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs"))

"""Repository root for checked-out configuration and evidence; independent of cwd."""
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

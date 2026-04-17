"""
config.py
─────────────────────────────────────────────────────────────────────────────
Canonical path configuration for the procurement-chile pipeline.

The code lives under a local repo (e.g. Research-local/procurement-chile-local/)
but data and output artifacts live under a separate Dropbox folder so they can
be shared across users and machines. Both locations are configurable per user.

Resolution order for each path (first match wins):
  1. Environment variable      (PROCUREMENT_CHILE_CODE / PROCUREMENT_CHILE_DB)
  2. .env file at repo root    (same variable names, `KEY=/absolute/path`)
  3. For CODE_ROOT: auto-derived from the directory containing this file.
     For DROPBOX_ROOT: hard failure with instructions.

Exports:
  REPO_ROOT, CODE_ROOT
  DROPBOX_ROOT
  DATA_ROOT, DATA_RAW, DATA_CLEAN
  DATA_RAW_LICITACIONES, DATA_RAW_COMPRA_AGIL, DATA_RAW_SII, DATA_RAW_OTHER
  OUTPUT_ROOT
"""

from __future__ import annotations

import os
from pathlib import Path


_CONFIG_PARENT = Path(__file__).resolve().parent  # the code/ dir containing this file


def _read_env_file(env_path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    if not env_path.is_file():
        return entries
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        entries[key.strip()] = value
    return entries


# Load .env once (used for both CODE and DB resolution)
_ENV_ENTRIES = _read_env_file(_CONFIG_PARENT.parent / ".env")


def _resolve_from_env(var: str) -> Path | None:
    from_env = os.environ.get(var)
    if from_env:
        return Path(from_env).expanduser().resolve()
    if var in _ENV_ENTRIES:
        return Path(_ENV_ENTRIES[var]).expanduser().resolve()
    return None


def _resolve_code_root() -> Path:
    override = _resolve_from_env("PROCUREMENT_CHILE_CODE")
    if override is not None:
        return override
    # Fallback: derive from this file's location (always correct when running
    # scripts under the repo that contains this config.py).
    return _CONFIG_PARENT


def _resolve_dropbox_root() -> Path:
    override = _resolve_from_env("PROCUREMENT_CHILE_DB")
    if override is not None:
        return override
    raise RuntimeError(
        "PROCUREMENT_CHILE_DB is not set.\n"
        "Set it one of two ways:\n"
        f"  1. Export env var:  export PROCUREMENT_CHILE_DB=/path/to/procurement-chile-db\n"
        f"  2. Create {_CONFIG_PARENT.parent / '.env'} with:\n"
        f"       PROCUREMENT_CHILE_DB=/path/to/procurement-chile-db\n"
        "See .env.example at the repo root for a template."
    )


CODE_ROOT = _resolve_code_root()
REPO_ROOT = CODE_ROOT.parent

DROPBOX_ROOT = _resolve_dropbox_root()

if not DROPBOX_ROOT.is_dir():
    raise FileNotFoundError(
        f"DROPBOX_ROOT does not exist or is not a directory: {DROPBOX_ROOT}\n"
        "Check PROCUREMENT_CHILE_DB env var or .env file."
    )
if not CODE_ROOT.is_dir():
    raise FileNotFoundError(
        f"CODE_ROOT does not exist or is not a directory: {CODE_ROOT}\n"
        "Check PROCUREMENT_CHILE_CODE env var or .env file."
    )

# ── Data paths ───────────────────────────────────────────────────────────────
DATA_ROOT  = DROPBOX_ROOT / "data"
DATA_RAW   = DATA_ROOT / "raw"
DATA_CLEAN = DATA_ROOT / "clean"

DATA_RAW_LICITACIONES = DATA_RAW / "chilecompra" / "licitaciones"
DATA_RAW_COMPRA_AGIL  = DATA_RAW / "chilecompra" / "compra_agil"
DATA_RAW_SII          = DATA_RAW / "sii"
DATA_RAW_OTHER        = DATA_RAW / "other"

# ── Output paths ─────────────────────────────────────────────────────────────
OUTPUT_ROOT = DROPBOX_ROOT / "output"

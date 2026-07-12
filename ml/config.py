"""
Configuration module for the ML pipeline.

All settings are loaded from environment variables.
Copy .env.example to ml/.env and fill in the values.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
_ENV_FILE = Path(__file__).resolve().parent / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE)
else:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
POSTGRES_URI: str = os.getenv(
    "POSTGRES_URI",
    "postgresql://postgres:postgres@localhost:5432/postgres",
)

# ---------------------------------------------------------------------------
# Neon (read-only serving database for BI dashboards)
# ---------------------------------------------------------------------------
NEON_HOST: str = os.getenv("NEON_HOST", "")
NEON_PORT: str = os.getenv("NEON_PORT", "5432")
NEON_DATABASE: str = os.getenv("NEON_DATABASE", "")
NEON_USER: str = os.getenv("NEON_USER", "")
NEON_PASSWORD: str = os.getenv("NEON_PASSWORD", "")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ML_DIR: Path = Path(__file__).resolve().parent
SAVED_MODELS_DIR: Path = ML_DIR / "saved_models"
REPORTS_DIR: Path = ML_DIR / "reports"

# Ensure output directories exist
SAVED_MODELS_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

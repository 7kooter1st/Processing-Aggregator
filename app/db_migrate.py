from __future__ import annotations

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text

from app.config import settings

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_REVISION = "0002_page_text"


def alembic_config() -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", settings.database_url)
    return cfg


def upgrade_to_head() -> str:
    cfg = alembic_config()
    command.upgrade(cfg, "head")
    revision = current_revision()
    if revision != EXPECTED_REVISION:
        heads = ScriptDirectory.from_config(cfg).get_heads()
        if revision not in heads:
            raise RuntimeError(
                f"Schema version mismatch: current={revision} expected={EXPECTED_REVISION}"
            )
    logger.info("[SCHEMA] alembic head=%s", revision)
    return revision or EXPECTED_REVISION


def current_revision() -> str | None:
    engine = create_engine(settings.database_url)
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT to_regclass('public.alembic_version')")
        ).scalar()
        if not exists:
            return None
        return conn.execute(text("SELECT version_num FROM alembic_version")).scalar()

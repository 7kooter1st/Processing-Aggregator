"""Inline DOCX page text; object_key only for binary page images.

Revision ID: 0002_page_text
Revises: 0001_ha_workflow
Create Date: 2026-09-03
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0002_page_text"
down_revision: Union[str, Sequence[str], None] = "0001_ha_workflow"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE document_pages
            ALTER COLUMN object_key DROP NOT NULL;
        ALTER TABLE document_pages
            ADD COLUMN IF NOT EXISTS page_text TEXT;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE document_pages DROP COLUMN IF EXISTS page_text;
        ALTER TABLE document_pages
            ALTER COLUMN object_key SET NOT NULL;
        """
    )

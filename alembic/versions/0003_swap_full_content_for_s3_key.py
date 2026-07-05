"""swap full_content LargeBinary for full_content_s3_key String

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("records", "full_content")
    op.add_column("records", sa.Column("full_content_s3_key", sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column("records", "full_content_s3_key")
    op.add_column("records", sa.Column("full_content", sa.LargeBinary(), nullable=True))

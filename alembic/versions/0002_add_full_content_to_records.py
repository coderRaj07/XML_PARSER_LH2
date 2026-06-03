"""add full_content to records

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-03

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("records", sa.Column("full_content", sa.LargeBinary(), nullable=True))


def downgrade() -> None:
    op.drop_column("records", "full_content")

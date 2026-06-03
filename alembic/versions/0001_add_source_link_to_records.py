"""add source_link to records

Revision ID: 0001
Revises: 
Create Date: 2026-06-03

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("records", sa.Column("source_link", sa.Text(), nullable=False, server_default=""))


def downgrade() -> None:
    op.drop_column("records", "source_link")

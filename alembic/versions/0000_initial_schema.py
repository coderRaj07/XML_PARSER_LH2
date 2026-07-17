"""initial schema — create all tables

Revision ID: 0000
Revises:
Create Date: 2026-07-17

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0000"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("status", sa.String(20), server_default="pending"),
        sa.Column("total_tasks", sa.Integer(), server_default="0"),
        sa.Column("completed_tasks", sa.Integer(), server_default="0"),
        sa.Column("failed_tasks", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
    )

    op.create_table(
        "tasks",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("jobs.id"), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(20), server_default="pending"),
        sa.Column("attempts", sa.Integer(), server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )

    op.create_table(
        "records",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("task_id", sa.String(36), sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("title", sa.Text(), server_default=""),
        sa.Column("author", sa.Text(), server_default=""),
        sa.Column("published_date", sa.DateTime(), nullable=True),
        sa.Column("source_link", sa.Text(), server_default=""),
        sa.Column("description", sa.Text(), server_default=""),
        sa.Column("content", sa.Text(), server_default=""),
        sa.Column("full_content_s3_key", sa.String(255), nullable=True),
    )

    op.create_table(
        "summaries",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("record_id", sa.String(36), sa.ForeignKey("records.id"), nullable=False),
        sa.Column("summary_text", sa.Text(), server_default=""),
        sa.Column("summary_type", sa.String(50), server_default=""),
        sa.Column("model_used", sa.String(100), server_default=""),
    )


def downgrade() -> None:
    op.drop_table("summaries")
    op.drop_table("records")
    op.drop_table("tasks")
    op.drop_table("jobs")

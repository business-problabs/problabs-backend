"""Add pro_gifted fields to users table

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-05-06 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5f6a7b8c9d0'
down_revision: Union[str, Sequence[str], None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add pro_gifted, pro_gifted_at, pro_gifted_note to users."""
    op.add_column(
        'users',
        sa.Column('pro_gifted', sa.Boolean(), nullable=False, server_default='false')
    )
    op.add_column(
        'users',
        sa.Column('pro_gifted_at', sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        'users',
        sa.Column('pro_gifted_note', sa.String(), nullable=True)
    )


def downgrade() -> None:
    """Remove pro_gifted fields from users."""
    op.drop_column('users', 'pro_gifted_note')
    op.drop_column('users', 'pro_gifted_at')
    op.drop_column('users', 'pro_gifted')

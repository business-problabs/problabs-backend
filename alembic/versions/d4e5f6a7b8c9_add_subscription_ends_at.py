"""Add subscription_ends_at to users

Revision ID: d4e5f6a7b8c9
Revises: c3f1a2b4d5e6
Create Date: 2026-04-17 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, Sequence[str], None] = 'c3f1a2b4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add subscription_ends_at column to users table."""
    op.add_column(
        'users',
        sa.Column('subscription_ends_at', sa.DateTime(timezone=True), nullable=True)
    )


def downgrade() -> None:
    """Remove subscription_ends_at column from users table."""
    op.drop_column('users', 'subscription_ends_at')

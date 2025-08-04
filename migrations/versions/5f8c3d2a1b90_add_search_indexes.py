"""add search indexes

Revision ID: 5f8c3d2a1b90
Revises: 26a9efc758a0
Create Date: 2025-08-04 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5f8c3d2a1b90'
down_revision: Union[str, None] = '26a9efc758a0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add indexes for search optimization."""
    # Index on trace_id for efficient trace-based searches
    op.execute("CREATE INDEX idx_runs_trace_id ON runs(trace_id);")
    
    # Index on name for efficient name-based searches (supports ILIKE queries)
    op.execute("CREATE INDEX idx_runs_name ON runs(name);")
    
    # Composite index for combined trace_id + name searches
    op.execute("CREATE INDEX idx_runs_trace_id_name ON runs(trace_id, name);")


def downgrade() -> None:
    """Remove search indexes."""
    op.execute("DROP INDEX idx_runs_trace_id;")
    op.execute("DROP INDEX idx_runs_name;")
    op.execute("DROP INDEX idx_runs_trace_id_name;")
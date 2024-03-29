"""Initial state

Revision ID: 06850d25958a
Revises:
Create Date: 2023-02-03 19:47:10.190176

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "06850d25958a"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "concept",
        sa.Column("iri", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("iri"),
    )
    op.create_table(
        "dataset_distribution",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ds", sa.String(), nullable=False),
        sa.Column("distr", sa.String(), nullable=False),
        sa.Column("relevant", sa.Boolean, nullable=True, default=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "dataset_distribution_index_ds",
        "dataset_distribution",
        ["ds"],
        postgresql_using="HASH",
    )
    op.create_index(
        "dataset_distribution_index_distr",
        "dataset_distribution",
        ["distr"],
        postgresql_using="HASH",
    )
    op.create_table(
        "datacube",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("iri", sa.String(), nullable=False),
        sa.Column("rod", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "dataset_endpoint",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ds", sa.String(), nullable=False),
        sa.Column("endpoint", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "ddr",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("relationship_type", sa.String(), nullable=False),
        sa.Column("iri1", sa.String(), nullable=False),
        sa.Column("iri2", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ddr_index", "ddr", ["relationship_type", "iri1"], unique=True)
    op.create_index("ddr_rtype", "ddr", ["relationship_type"], postgresql_using="HASH")
    op.create_table(
        "label",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("iri", sa.String(), nullable=False),
        sa.Column("language_code", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "subject_object",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("distribution_iri", sa.String(), nullable=False),
        sa.Column("iri", sa.String(), nullable=False),
        sa.Column("pure_subject", sa.Boolean, nullable=True, default=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "subject_object_index",
        "subject_object",
        ["distribution_iri", "iri"],
        unique=True,
    )
    op.create_index(
        "subject_object_index_distr",
        "subject_object",
        ["distribution_iri"],
        postgresql_using="HASH",
    )

    op.create_table(
        "relationship",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("group", sa.String(), nullable=False),
        sa.Column("candidate", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    # op.create_index('relationship_index', 'relationship', ['type', 'group', 'candidate'], unique=True)
    op.create_index(
        "relationship_index_type", "relationship", ["type"], postgresql_using="HASH"
    )
    # op.create_index('relationship_index_group', 'relationship', ['group'], postgresql_using='HASH')
    op.create_index(
        "relationship_index_candidate",
        "relationship",
        ["candidate"],
        postgresql_using="HASH",
    )

    op.create_table(
        "robots_delay",
        sa.Column("iri", sa.String(), nullable=False),
        sa.Column("expiration", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("iri"),
    )

    op.create_table(
        "analysis",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("iri", sa.String(), nullable=False),
        sa.Column("analyzer", sa.String(), nullable=False),
        sa.Column("data", sa.JSON()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("analysis_index", "analysis", ["iri", "analyzer"], unique=True)
    op.create_index("analysis_index_iri", "analysis", ["iri"], postgresql_using="HASH")
    op.create_index(
        "analysis_index_analyzer", "analysis", ["analyzer"], postgresql_using="HASH"
    )

    op.create_table(
        "related",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("token", sa.String(), nullable=False),
        sa.Column("ds", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("related_index_ds", "related", ["ds"], postgresql_using="HASH")
    op.create_index(
        "related_index_token", "related", ["token"], postgresql_using="HASH"
    )
    op.create_index("related_index_type", "related", ["type"], postgresql_using="HASH")
    op.create_index("related_index", "related", ["ds", "token", "type"], unique=True)


def downgrade() -> None:
    op.drop_index("dataset_distribution_index_ds")
    op.drop_index("dataset_distribution_index_distr")
    op.drop_index("ddr_index")
    op.drop_index("ddr_rtype")
    op.drop_index("subject_object_index")
    op.drop_index("subject_object_index_distr")
    op.drop_index("relationship_index_type")
    op.drop_index("relationship_index_candidate")
    op.drop_index("analysis_index")
    op.drop_index("analysis_index_iri")
    op.drop_index("analysis_index_type")
    op.drop_index("related_index_ds")
    op.drop_index("related_index_token")
    op.drop_index("related_index_type")
    op.drop_index("related_index")
    op.drop_table("robots_delay")
    op.drop_table("relationship")
    op.drop_table("pure_subject")
    op.drop_table("label")
    op.drop_table("ddr")
    op.drop_table("dataset_endpoint")
    op.drop_table("dataset_distribution")
    op.drop_table("concept")
    op.drop_table("analysis")
    op.drop_table("related")

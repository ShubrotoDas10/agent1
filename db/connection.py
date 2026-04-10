from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URL
from loguru import logger

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from db import models  # noqa: F401 – registers all models
    Base.metadata.create_all(bind=engine)

    # Convert raw_signals to TimescaleDB hypertable (idempotent)
    with engine.connect() as conn:
        try:
            conn.execute(text(
                "SELECT create_hypertable('raw_signals','timestamp',"
                "if_not_exists => TRUE, migrate_data => TRUE);"
            ))
            conn.execute(text(
                "SELECT create_hypertable('signal_scores','computed_at',"
                "if_not_exists => TRUE, migrate_data => TRUE);"
            ))
            conn.commit()
            logger.info("TimescaleDB hypertables ready.")
        except Exception as e:
            logger.warning(f"Hypertable setup skipped (plain PG?): {e}")
            conn.rollback()


def migrate_db():
    """Add new columns to existing tables without recreating them."""
    migrations = [
        "ALTER TABLE opportunity_clusters ADD COLUMN IF NOT EXISTS clean_label VARCHAR;",
        "ALTER TABLE opportunity_clusters ADD COLUMN IF NOT EXISTS taxonomy_label VARCHAR;",
        "ALTER TABLE raw_signals ADD COLUMN IF NOT EXISTS source_tier INTEGER DEFAULT 1;",
    ]
    with engine.connect() as conn:
        for sql in migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                logger.debug(f"Migration skipped: {e}")

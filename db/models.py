from sqlalchemy import (
    Column, String, Float, Integer, DateTime, Text, JSON, Boolean, Index
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
import uuid
from db.connection import Base


def _uuid():
    return str(uuid.uuid4())


# ── Raw signals (time-series, one row per observation) ────────────────────────
class RawSignal(Base):
    __tablename__ = "raw_signals"

    id          = Column(String, primary_key=True, default=_uuid)
    entity      = Column(String, nullable=False, index=True)   # keyword / topic
    source      = Column(String, nullable=False, index=True)   # e.g. "youtube"
    source_tier = Column(Integer, default=1)                   # 1=dedicated,2=generic
    timestamp   = Column(DateTime(timezone=True), nullable=False, index=True,
                         server_default=func.now())
    raw_value   = Column(Float, nullable=True)
    raw_meta    = Column(JSON, nullable=True)                   # extra fields
    url         = Column(Text, nullable=True)
    category    = Column(String, nullable=True)

    __table_args__ = (
        Index("ix_raw_entity_source_ts", "entity", "source", "timestamp"),
    )


# ── Normalized + scored signals ───────────────────────────────────────────────
class SignalScore(Base):
    __tablename__ = "signal_scores"

    id              = Column(String, primary_key=True, default=_uuid)
    entity          = Column(String, nullable=False, index=True)
    cluster_id      = Column(String, nullable=True, index=True)
    computed_at     = Column(DateTime(timezone=True), nullable=False, index=True,
                             server_default=func.now())
    norm_value      = Column(Float, nullable=True)
    velocity        = Column(Float, nullable=True)
    acceleration    = Column(Float, nullable=True)
    consistency     = Column(Float, nullable=True)
    source_count    = Column(Integer, default=1)
    opportunity_score = Column(Float, nullable=True)
    lifecycle_stage = Column(String, nullable=True)   # early_spike/emerging/peaking/decaying
    attention_weight = Column(Float, nullable=True)
    sources_json    = Column(JSON, nullable=True)


# ── Opportunity clusters ──────────────────────────────────────────────────────
class OpportunityCluster(Base):
    __tablename__ = "opportunity_clusters"

    id              = Column(String, primary_key=True, default=_uuid)
    label           = Column(String, nullable=False)
    clean_label     = Column(String, nullable=True)   # Groq-generated human label
    taxonomy_label  = Column(String, nullable=True)   # taxonomy classification
    member_entities = Column(JSON, nullable=True)               # list of merged keywords
    top_score       = Column(Float, nullable=True)
    lifecycle_stage = Column(String, nullable=True)
    attention_weight = Column(Float, nullable=True)
    sources_json    = Column(JSON, nullable=True)
    updated_at      = Column(DateTime(timezone=True), server_default=func.now(),
                             onupdate=func.now())


# ── Discovered sources (meta-discovery results) ───────────────────────────────
class DiscoveredSource(Base):
    __tablename__ = "discovered_sources"

    id          = Column(String, primary_key=True, default=_uuid)
    url         = Column(Text, unique=True, nullable=False)
    name        = Column(String, nullable=True)
    category    = Column(String, nullable=True)
    tier        = Column(Integer, default=2)
    active      = Column(Boolean, default=True)
    discovered_at = Column(DateTime(timezone=True), server_default=func.now())
    last_scraped  = Column(DateTime(timezone=True), nullable=True)
    signal_count  = Column(Integer, default=0)


# ── Pipeline run log ──────────────────────────────────────────────────────────
class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id          = Column(String, primary_key=True, default=_uuid)
    started_at  = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    status      = Column(String, default="running")   # running/success/failed
    signals_collected = Column(Integer, default=0)
    errors_json = Column(JSON, nullable=True)

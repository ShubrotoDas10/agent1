"""
FastAPI Application
- Serves dashboard HTML at /
- Exposes REST endpoints for frontend
- Runs APScheduler for pipeline cycles
- All logs stream to stdout (visible in terminal)
"""
import asyncio
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from loguru import logger
from apscheduler.schedulers.background import BackgroundScheduler

from db.connection import get_db, init_db
from db.models import OpportunityCluster, PipelineRun, RawSignal, SignalScore, DiscoveredSource
from config import COLLECTION_INTERVAL_HOURS, TOP_N_OPPORTUNITIES
from orchestrator import trigger_pipeline

import os

app = FastAPI(title="Agent1 — Global Signal Radar", version="1.0.0")

# ── Static files ──────────────────────────────────────────────────────────────
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)

# ── Scheduler ─────────────────────────────────────────────────────────────────
scheduler = BackgroundScheduler()

@app.on_event("startup")
async def startup():
    logger.info("Initialising database …")
    init_db()
    from db.connection import migrate_db
    migrate_db()
    logger.info("Database ready.")

    # Schedule pipeline every N hours — does NOT run on boot
    scheduler.add_job(
        trigger_pipeline,
        "interval",
        hours=COLLECTION_INTERVAL_HOURS,
        id="pipeline",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    logger.info(f"Scheduler started — pipeline runs every {COLLECTION_INTERVAL_HOURS}h")
    logger.info("Server is live — pipeline will run on schedule or via manual trigger")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown(wait=False)


# ── Dashboard ─────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    with open(html_path, "r") as f:
        return HTMLResponse(content=f.read())


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/opportunities")
async def get_opportunities(
    limit: int = TOP_N_OPPORTUNITIES,
    stage: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = db.query(OpportunityCluster).order_by(
        OpportunityCluster.top_score.desc()
    )
    if stage:
        q = q.filter(OpportunityCluster.lifecycle_stage == stage)
    clusters = q.limit(limit).all()

    return [
        {
            "rank":             i + 1,
            "label":            c.clean_label or c.label,
            "raw_label":        c.label,
            "taxonomy":         c.taxonomy_label or "",
            "members":          c.member_entities or [],
            "score":            round(c.top_score or 0, 4),
            "lifecycle_stage":  c.lifecycle_stage,
            "attention_weight": round(c.attention_weight or 0, 6),
            "sources":          c.sources_json or [],
            "updated_at":       c.updated_at.isoformat() if c.updated_at else None,
        }
        for i, c in enumerate(clusters)
    ]


@app.get("/api/signals/recent")
async def get_recent_signals(limit: int = 100, db: Session = Depends(get_db)):
    rows = (
        db.query(RawSignal)
        .order_by(RawSignal.timestamp.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "entity":    r.entity,
            "source":    r.source,
            "timestamp": r.timestamp.isoformat(),
            "raw_value": r.raw_value,
            "category":  r.category,
        }
        for r in rows
    ]


@app.get("/api/signals/entity/{entity}")
async def get_entity_history(entity: str, db: Session = Depends(get_db)):
    rows = (
        db.query(SignalScore)
        .filter(SignalScore.entity == entity.lower())
        .order_by(SignalScore.computed_at.asc())
        .limit(100)
        .all()
    )
    return [
        {
            "computed_at":       r.computed_at.isoformat(),
            "norm_value":        r.norm_value,
            "velocity":          r.velocity,
            "acceleration":      r.acceleration,
            "opportunity_score": r.opportunity_score,
            "lifecycle_stage":   r.lifecycle_stage,
        }
        for r in rows
    ]


@app.get("/api/pipeline/runs")
async def get_pipeline_runs(limit: int = 20, db: Session = Depends(get_db)):
    rows = (
        db.query(PipelineRun)
        .order_by(PipelineRun.started_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id":               r.id,
            "started_at":       r.started_at.isoformat() if r.started_at else None,
            "finished_at":      r.finished_at.isoformat() if r.finished_at else None,
            "status":           r.status,
            "signals_collected": r.signals_collected,
            "errors":           r.errors_json or [],
        }
        for r in rows
    ]


@app.get("/api/sources/discovered")
async def get_discovered_sources(db: Session = Depends(get_db)):
    rows = (
        db.query(DiscoveredSource)
        .order_by(DiscoveredSource.discovered_at.desc())
        .limit(100)
        .all()
    )
    return [
        {
            "name":         r.name,
            "url":          r.url,
            "category":     r.category,
            "tier":         r.tier,
            "signal_count": r.signal_count,
            "discovered_at": r.discovered_at.isoformat() if r.discovered_at else None,
        }
        for r in rows
    ]


@app.get("/api/stats")
async def get_stats(db: Session = Depends(get_db)):
    total_signals   = db.query(RawSignal).count()
    total_entities  = db.query(SignalScore.entity).distinct().count()
    total_clusters  = db.query(OpportunityCluster).count()
    total_sources   = db.query(DiscoveredSource).count()
    last_run        = (
        db.query(PipelineRun)
        .order_by(PipelineRun.started_at.desc())
        .first()
    )
    return {
        "total_signals":   total_signals,
        "total_entities":  total_entities,
        "total_clusters":  total_clusters,
        "discovered_sources": total_sources,
        "last_run_status": last_run.status if last_run else "never",
        "last_run_at":     last_run.started_at.isoformat() if last_run else None,
    }


@app.post("/api/pipeline/trigger")
async def manual_trigger(background_tasks: BackgroundTasks):
    """Manually trigger a pipeline run."""
    background_tasks.add_task(trigger_pipeline)
    return {"message": "Pipeline triggered", "timestamp": datetime.now(timezone.utc).isoformat()}




@app.get("/api/niche/history/{niche_name}")
async def get_niche_history(niche_name: str, db: Session = Depends(get_db)):
    """Returns time-series of combined opportunity score for a niche across pipeline runs."""
    from sqlalchemy import func
    from db.models import PipelineRun

    # Get all pipeline runs
    runs = db.query(PipelineRun).filter(
        PipelineRun.status.in_(["success","partial"])
    ).order_by(PipelineRun.started_at.asc()).limit(50).all()

    result = []
    for run in runs:
        # Get scores computed around this run time (±30 min window)
        from datetime import timedelta
        window_start = run.started_at - timedelta(minutes=5)
        window_end   = run.started_at + timedelta(minutes=35)

        rows = db.query(SignalScore).filter(
            SignalScore.computed_at >= window_start,
            SignalScore.computed_at <= window_end,
        ).all()

        # Filter to niche and aggregate
        niche_scores = []
        for r in rows:
            # Check taxonomy via opportunity_cluster
            cluster = db.query(OpportunityCluster).filter(
                OpportunityCluster.label == r.entity
            ).first()
            tax = cluster.taxonomy_label if cluster else ""
            if tax == niche_name:
                niche_scores.append(r.opportunity_score or 0)

        if niche_scores:
            result.append({
                "timestamp": run.started_at.isoformat(),
                "avg_score": round(sum(niche_scores)/len(niche_scores)*100, 2),
                "max_score": round(max(niche_scores)*100, 2),
                "count":     len(niche_scores),
            })

    return result


@app.get("/api/niche/signals/{niche_name}")
async def get_niche_signals(niche_name: str, db: Session = Depends(get_db)):
    """Returns all current opportunity clusters for a given broad niche."""
    clusters = db.query(OpportunityCluster).filter(
        OpportunityCluster.taxonomy_label == niche_name
    ).order_by(OpportunityCluster.top_score.desc()).limit(100).all()

    return [
        {
            "rank":             i + 1,
            "label":            c.clean_label or c.label,
            "raw_label":        c.label,
            "taxonomy":         c.taxonomy_label or "",
            "members":          c.member_entities or [],
            "score":            round(c.top_score or 0, 4),
            "lifecycle_stage":  c.lifecycle_stage,
            "attention_weight": round(c.attention_weight or 0, 6),
            "sources":          c.sources_json or [],
            "updated_at":       c.updated_at.isoformat() if c.updated_at else None,
        }
        for i, c in enumerate(clusters)
    ]


@app.get("/api/scheduler/status")
async def scheduler_status():
    job = scheduler.get_job("pipeline")
    next_run = None
    if job and job.next_run_time:
        next_run = job.next_run_time.isoformat()
    return {
        "running":        scheduler.running,
        "next_run":       next_run,
        "interval_hours": COLLECTION_INTERVAL_HOURS,
    }

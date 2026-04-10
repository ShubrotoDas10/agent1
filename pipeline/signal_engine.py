"""
Signal Engine
Computes per-entity:
  - velocity     : Δ normalized_value over last interval
  - acceleration : Δ of velocity
  - consistency  : fraction of last N intervals where entity appeared
  - source_count : distinct sources mentioning entity in current cycle
"""
from typing import Dict, List, Tuple
from datetime import datetime, timezone, timedelta
from loguru import logger
from sqlalchemy.orm import Session

from db.models import SignalScore


CONSISTENCY_WINDOW = 5   # last N pipeline runs to check


def compute_signals(
    entity: str,
    current_norm: float,
    sources: List[str],
    db: Session,
) -> Dict:
    """
    Pull historical scores for entity and compute derived signals.
    Returns dict with velocity, acceleration, consistency, source_count.
    """
    # Fetch last CONSISTENCY_WINDOW scores for this entity
    history: List[SignalScore] = (
        db.query(SignalScore)
        .filter(SignalScore.entity == entity)
        .order_by(SignalScore.computed_at.desc())
        .limit(CONSISTENCY_WINDOW)
        .all()
    )

    history_vals = [h.norm_value for h in history if h.norm_value is not None]

    # ── Velocity ──────────────────────────────────────────────────────────────
    if history_vals:
        velocity = current_norm - history_vals[0]
    else:
        velocity = 0.0

    # ── Acceleration ──────────────────────────────────────────────────────────
    if len(history_vals) >= 2:
        prev_velocity = history_vals[0] - history_vals[1]
        acceleration  = velocity - prev_velocity
    else:
        acceleration = 0.0

    # ── Consistency ───────────────────────────────────────────────────────────
    # Fraction of last N intervals where entity had a score recorded
    consistency = len(history_vals) / CONSISTENCY_WINDOW

    # ── Source count ──────────────────────────────────────────────────────────
    source_count = len(set(sources))

    return {
        "velocity":     round(velocity,     6),
        "acceleration": round(acceleration, 6),
        "consistency":  round(consistency,  4),
        "source_count": source_count,
        "norm_value":   round(current_norm, 6),
    }

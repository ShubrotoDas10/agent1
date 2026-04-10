"""
Attention Allocator
Assigns each entity a dynamic attention weight proportional to its
opportunity score relative to the global signal pool.
Updated every pipeline cycle. Mirrors capital allocation logic.
"""
from typing import List, Dict
from loguru import logger
from sqlalchemy.orm import Session

from db.models import SignalScore


def allocate_attention(db: Session, scored_entities: List[Dict]) -> List[Dict]:
    """
    scored_entities: [{entity, opportunity_score, ...}, ...]
    Returns same list with attention_weight added (sums to 1.0 across all).
    """
    if not scored_entities:
        return []

    scores = [max(e.get("opportunity_score", 0.0), 0.0) for e in scored_entities]
    total  = sum(scores)

    if total < 1e-9:
        weight = 1.0 / len(scores)
        for e in scored_entities:
            e["attention_weight"] = round(weight, 6)
        return scored_entities

    for entity_dict, score in zip(scored_entities, scores):
        entity_dict["attention_weight"] = round(score / total, 6)

    # Persist back to SignalScore rows
    for e in scored_entities:
        latest = (
            db.query(SignalScore)
            .filter(SignalScore.entity == e["entity"])
            .order_by(SignalScore.computed_at.desc())
            .first()
        )
        if latest:
            latest.attention_weight = e["attention_weight"]

    db.commit()
    logger.info(f"[allocator] attention distributed across {len(scored_entities)} entities")
    return scored_entities

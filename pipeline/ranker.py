"""
Ranker
Aggregates scored entities → clusters → labels → top-N ranked list.
"""
from typing import List, Dict
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import OpportunityCluster, SignalScore
from config import TOP_N_OPPORTUNITIES
from pipeline.cluster_engine import cluster_entities
from pipeline.niche_labeler  import label_clusters


def build_rankings(db: Session) -> List[Dict]:
    # Latest score per entity
    subq = (
        db.query(SignalScore.entity, func.max(SignalScore.computed_at).label("latest"))
        .group_by(SignalScore.entity)
        .subquery()
    )
    rows: List[SignalScore] = (
        db.query(SignalScore)
        .join(subq, (SignalScore.entity == subq.c.entity) &
                    (SignalScore.computed_at == subq.c.latest))
        .order_by(SignalScore.opportunity_score.desc())
        .limit(500)
        .all()
    )

    if not rows:
        logger.warning("[ranker] no signal scores found")
        return []

    entity_scores = [(r.entity, r.opportunity_score or 0.0) for r in rows]
    clusters      = cluster_entities(entity_scores)
    clusters.sort(key=lambda c: c["top_score"], reverse=True)
    top_clusters  = clusters[:TOP_N_OPPORTUNITIES]

    # Add member lists to clusters for labeling
    score_map = {r.entity: r for r in rows}

    # Attach members with raw text for labeling
    for cl in top_clusters:
        cl["members"] = cl.get("members", [cl["label"]])

    # Stage 1+2 labeling
    top_clusters = label_clusters(top_clusters)

    now = datetime.now(timezone.utc)
    result = []

    for rank, cl in enumerate(top_clusters):
        label       = cl["label"]
        clean_label = cl.get("clean_label") or cl.get("taxonomy_label") or label
        tax_label   = cl.get("taxonomy_label", "")
        best_row    = score_map.get(label)

        sources = list({
            (score_map[m].sources_json[0]
             if score_map.get(m) and score_map[m].sources_json
             else "unknown")
            for m in cl["members"] if score_map.get(m)
        })

        lifecycle = best_row.lifecycle_stage if best_row else "stable"
        attn      = best_row.attention_weight if best_row else 0.0

        # Upsert cluster
        existing = db.query(OpportunityCluster).filter(
            OpportunityCluster.label == label
        ).first()

        if existing:
            existing.clean_label     = clean_label
            existing.taxonomy_label  = tax_label
            existing.member_entities = cl["members"]
            existing.top_score       = cl["top_score"]
            existing.lifecycle_stage = lifecycle
            existing.attention_weight= attn
            existing.sources_json    = sources
            existing.updated_at      = now
        else:
            db.add(OpportunityCluster(
                label=label,
                clean_label=clean_label,
                taxonomy_label=tax_label,
                member_entities=cl["members"],
                top_score=cl["top_score"],
                lifecycle_stage=lifecycle,
                attention_weight=attn,
                sources_json=sources,
            ))

        result.append({
            "rank":             rank + 1,
            "label":            clean_label,      # show clean label to UI
            "raw_label":        label,            # keep raw for entity lookup
            "taxonomy":         tax_label,
            "members":          cl["members"],
            "score":            cl["top_score"],
            "lifecycle_stage":  lifecycle,
            "attention_weight": attn,
            "sources":          sources,
        })

    db.commit()
    logger.info(f"[ranker] top {len(result)} opportunities ranked & labeled")
    return result

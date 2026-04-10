"""
Cluster Engine
Groups semantically similar entities using sentence-transformers embeddings.
Entities with cosine similarity >= CLUSTER_SIMILARITY are merged into one cluster.
The cluster representative (label) is the entity with the highest opportunity score.
"""
from typing import List, Dict, Tuple
import numpy as np
from loguru import logger

try:
    from sentence_transformers import SentenceTransformer
    _MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info("[cluster_engine] sentence-transformers model loaded")
except Exception as e:
    _MODEL = None
    logger.warning(f"[cluster_engine] model load failed, clustering disabled: {e}")

from config import CLUSTER_SIMILARITY


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    denom = (np.linalg.norm(a) * np.linalg.norm(b))
    return float(np.dot(a, b) / denom) if denom > 1e-9 else 0.0


def cluster_entities(
    entity_scores: List[Tuple[str, float]]   # [(entity, score), ...]
) -> List[Dict]:
    """
    Returns list of clusters:
      {
        "label":    str,           # representative keyword
        "members":  [str, ...],    # all grouped entities
        "top_score": float,
      }
    """
    if not entity_scores:
        return []

    entities = [e for e, _ in entity_scores]
    scores   = [s for _, s in entity_scores]

    if _MODEL is None or len(entities) == 0:
        # No clustering — each entity is its own cluster
        return [
            {"label": e, "members": [e], "top_score": s}
            for e, s in entity_scores
        ]

    try:
        embeddings = _MODEL.encode(entities, show_progress_bar=False, batch_size=64)
    except Exception as ex:
        logger.error(f"[cluster_engine] encode failed: {ex}")
        return [{"label": e, "members": [e], "top_score": s} for e, s in entity_scores]

    n = len(entities)
    assigned = [-1] * n       # cluster id per entity
    clusters: List[List[int]] = []

    for i in range(n):
        if assigned[i] != -1:
            continue
        cluster_id = len(clusters)
        clusters.append([i])
        assigned[i] = cluster_id

        for j in range(i + 1, n):
            if assigned[j] != -1:
                continue
            sim = _cosine(embeddings[i], embeddings[j])
            if sim >= CLUSTER_SIMILARITY:
                clusters[cluster_id].append(j)
                assigned[j] = cluster_id

    result = []
    for members_idx in clusters:
        member_entities = [entities[idx] for idx in members_idx]
        member_scores   = [scores[idx]   for idx in members_idx]
        best_idx        = int(np.argmax(member_scores))
        result.append({
            "label":     member_entities[best_idx],
            "members":   member_entities,
            "top_score": member_scores[best_idx],
        })

    logger.info(f"[cluster_engine] {n} entities → {len(result)} clusters")
    return result

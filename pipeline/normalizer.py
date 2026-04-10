"""
Normalization Layer
Converts heterogeneous raw_values (views, upvotes, rank, price, %) into
comparable [0, 1] scores using min-max scaling per source per run.
Raw values are never overwritten.
"""
import numpy as np
from typing import List, Dict, Tuple
from loguru import logger
from collectors.base import Signal


def normalize_signals(signals: List[Signal]) -> List[Tuple[Signal, float]]:
    """
    Returns list of (signal, normalized_value) pairs.
    Normalization is per-source to avoid cross-source scale dominance.
    """
    # Group by source
    by_source: Dict[str, List[int]] = {}
    for i, s in enumerate(signals):
        by_source.setdefault(s.source, []).append(i)

    results = [None] * len(signals)

    for source, indices in by_source.items():
        values = np.array([signals[i].raw_value for i in indices], dtype=float)

        # Replace NaN/inf
        values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)

        mn, mx = values.min(), values.max()
        if mx - mn < 1e-9:
            normed = np.ones_like(values) * 0.5
        else:
            normed = (values - mn) / (mx - mn)

        for idx, norm_val in zip(indices, normed):
            results[idx] = (signals[idx], float(norm_val))

        logger.debug(f"[normalizer] {source}: {len(indices)} signals normalized")

    return results

"""
Scoring Function
- First run (velocity=0): uses norm_value percentile for lifecycle variety
- Subsequent runs: velocity-based genuine classification
"""
from config import WEIGHTS


def compute_opportunity_score(signals: dict) -> float:
    v  = max(signals.get("velocity",     0.0), 0.0)
    a  = max(signals.get("acceleration", 0.0), 0.0)
    c  = signals.get("consistency",  0.0)
    sc = min(signals.get("source_count", 1) / 7.0, 1.0)

    score = (
        WEIGHTS["velocity"]     * v  +
        WEIGHTS["acceleration"] * a  +
        WEIGHTS["consistency"]  * c  +
        WEIGHTS["source_count"] * sc
    )

    if signals.get("source_count", 1) >= 3:
        score *= 1.3

    return round(score, 6)


def classify_lifecycle(score: float, velocity: float, acceleration: float,
                       norm_value: float = 0.5) -> str:
    first_run = abs(velocity) < 1e-6 and abs(acceleration) < 1e-6

    if first_run:
        # Spread across stages by signal strength so donut has variety
        if norm_value >= 0.75:
            return "early_spike"
        elif norm_value >= 0.45:
            return "emerging"
        elif norm_value >= 0.20:
            return "peaking"
        else:
            return "stable"
    else:
        if velocity > 0.15 and norm_value >= 0.5:
            return "early_spike"
        elif velocity > 0.04:
            return "emerging"
        elif velocity < -0.05:
            return "decaying"
        elif norm_value >= 0.45:
            return "peaking"
        else:
            return "stable"

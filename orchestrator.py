"""
Orchestrator
Ties every module together into one pipeline run.
Called by APScheduler every N hours (or manually).
"""
import asyncio
from datetime import datetime, timezone
from collections import defaultdict
from typing import List, Dict

from loguru import logger
from playwright.async_api import async_playwright

from db.connection import SessionLocal
from db.models import RawSignal, SignalScore, PipelineRun

from collectors.base import Signal
from collectors.google_trends import GoogleTrendsCollector
from collectors.youtube      import YouTubeCollector
from collectors.reddit       import RedditCollector
from collectors.amazon       import AmazonCollector
from collectors.flipkart     import FlipkartCollector
from collectors.tradingview  import TradingViewCollector
from collectors.yahoo_finance import YahooFinanceCollector
from collectors.generic      import GenericCollector

from discovery.meta_discovery  import MetaDiscovery
from pipeline.normalizer       import normalize_signals
from pipeline.signal_engine    import compute_signals
from pipeline.scorer           import compute_opportunity_score, classify_lifecycle
from pipeline.ranker           import build_rankings
from allocator.attention_allocator import allocate_attention


TIER1_COLLECTORS = [
    GoogleTrendsCollector,
    YouTubeCollector,
    RedditCollector,
    AmazonCollector,
    FlipkartCollector,
    TradingViewCollector,
    YahooFinanceCollector,
]


async def run_pipeline():
    db = SessionLocal()
    run_record = PipelineRun(started_at=datetime.now(timezone.utc), status="running")
    db.add(run_record)
    db.commit()

    errors = []
    all_signals: List[Signal] = []

    logger.info("=" * 60)
    logger.info("PIPELINE RUN STARTED")
    logger.info("=" * 60)

    async with async_playwright() as pw:

        # ── Step 1: Tier-1 dedicated collectors ──────────────────────────
        logger.info("[orchestrator] step 1 — tier-1 collectors")
        for CollectorClass in TIER1_COLLECTORS:
            try:
                collector = CollectorClass(playwright_instance=pw)
                signals   = await collector.collect()
                all_signals.extend(signals)
                await collector.close()
            except Exception as e:
                errors.append(str(e))
                logger.error(f"[orchestrator] collector {CollectorClass.__name__} failed: {e}")

        # ── Step 2: Meta-discovery (find new surfaces) ────────────────────
        logger.info("[orchestrator] step 2 — meta-discovery")
        try:
            discovery    = MetaDiscovery(db_session=db)
            new_sources  = await discovery.run()
            logger.info(f"[orchestrator] {len(new_sources)} new sources found")
        except Exception as e:
            errors.append(str(e))
            logger.error(f"[orchestrator] meta-discovery failed: {e}")
            new_sources = []

        # ── Step 3: Tier-2 generic collectors for discovered sources ──────
        logger.info("[orchestrator] step 3 — tier-2 generic collectors")
        from db.models import DiscoveredSource
        tier2_sources = (
            db.query(DiscoveredSource)
            .filter(DiscoveredSource.active == True, DiscoveredSource.tier == 2)
            .limit(20)   # cap per run to avoid overload
            .all()
        )

        for src in tier2_sources:
            try:
                gc = GenericCollector(
                    source_name=src.name or src.url[:30],
                    url=src.url,
                    category=src.category or "general",
                    playwright_instance=pw,
                )
                sigs = await gc.collect()
                all_signals.extend(sigs)
                src.last_scraped  = datetime.now(timezone.utc)
                src.signal_count += len(sigs)
                await gc.close()
            except Exception as e:
                errors.append(str(e))
                logger.warning(f"[orchestrator] generic collector {src.name} failed: {e}")

        db.commit()

    logger.info(f"[orchestrator] total signals collected: {len(all_signals)}")

    # ── Step 4: Persist raw signals ───────────────────────────────────────
    logger.info("[orchestrator] step 4 — persisting raw signals")
    for sig in all_signals:
        db.add(RawSignal(
            entity=sig.entity,
            source=sig.source,
            source_tier=sig.source_tier,
            timestamp=sig.timestamp,
            raw_value=sig.raw_value,
            raw_meta=sig.raw_meta,
            url=sig.url,
            category=sig.category,
        ))
    db.commit()

    # ── Step 5: Normalize ─────────────────────────────────────────────────
    logger.info("[orchestrator] step 5 — normalizing signals")
    normalized = normalize_signals(all_signals)

    # Group normalized values per entity (take max across sources per cycle)
    entity_norms: Dict[str, List] = defaultdict(list)
    entity_sources: Dict[str, List[str]] = defaultdict(list)

    for sig, norm_val in normalized:
        entity_norms[sig.entity].append(norm_val)
        entity_sources[sig.entity].append(sig.source)

    # ── Step 6: Signal engine + scoring ──────────────────────────────────
    logger.info("[orchestrator] step 6 — signal engine + scoring")
    scored_entities = []

    # Compute global norm_value percentiles for lifecycle classification
    all_norm_vals = [max(v) for v in entity_norms.values()]
    all_norm_vals.sort()
    n_total = len(all_norm_vals)
    def norm_percentile(val):
        if n_total == 0: return 0.5
        rank = sum(1 for v in all_norm_vals if v <= val)
        return rank / n_total

    for entity, norm_vals in entity_norms.items():
        current_norm = max(norm_vals)
        sources      = entity_sources[entity]

        sig_data = compute_signals(entity, current_norm, sources, db)
        opp_score = compute_opportunity_score(sig_data)
        lifecycle = classify_lifecycle(opp_score, sig_data["velocity"], sig_data["acceleration"], norm_percentile(current_norm))

        score_row = SignalScore(
            entity=entity,
            norm_value=sig_data["norm_value"],
            velocity=sig_data["velocity"],
            acceleration=sig_data["acceleration"],
            consistency=sig_data["consistency"],
            source_count=sig_data["source_count"],
            opportunity_score=opp_score,
            lifecycle_stage=lifecycle,
            sources_json=list(set(sources)),
        )
        db.add(score_row)
        scored_entities.append({
            "entity":            entity,
            "opportunity_score": opp_score,
            "lifecycle_stage":   lifecycle,
            "source_count":      sig_data["source_count"],
            **sig_data,
        })

    db.commit()

    # ── Step 7: Attention allocation ──────────────────────────────────────
    logger.info("[orchestrator] step 7 — attention allocation")
    scored_entities = allocate_attention(db, scored_entities)

    # ── Step 8: Build rankings ────────────────────────────────────────────
    logger.info("[orchestrator] step 8 — building rankings")
    rankings = build_rankings(db)

    # ── Finalize run record ───────────────────────────────────────────────
    run_record.finished_at      = datetime.now(timezone.utc)
    run_record.status           = "success" if not errors else "partial"
    run_record.signals_collected = len(all_signals)
    run_record.errors_json      = errors
    db.commit()
    db.close()

    logger.info("=" * 60)
    logger.info(f"PIPELINE RUN COMPLETE — {len(all_signals)} signals, "
                f"{len(rankings)} opportunities ranked")
    logger.info("=" * 60)

    return rankings


def trigger_pipeline():
    """Sync wrapper for APScheduler."""
    asyncio.run(run_pipeline())

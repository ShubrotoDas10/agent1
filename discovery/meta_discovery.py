"""
Meta-Discovery Engine
Finds new authoritative surfaces by:
  1. Querying Google for "top trending X site" across categories
  2. Traversing known directories (Product Hunt, Hacker News, etc.)
  3. Storing discovered URLs in discovered_sources table
  4. Skipping already-known Tier-1 sources
"""
import re
from typing import List, Dict
from loguru import logger
from playwright.async_api import async_playwright

from collectors.base import BaseCollector
from config import TIER1_SOURCES

DISCOVERY_QUERIES = [
    "top trending products site 2024",
    "trending discussions forum site",
    "top financial news trending",
    "viral content ranking site",
    "trending tech tools ranking",
    "top startup discovery platform",
    "trending niche market products",
]

SEED_DIRECTORIES = [
    {"url": "https://www.producthunt.com/",           "category": "tools"},
    {"url": "https://news.ycombinator.com/",          "category": "tech"},
    {"url": "https://www.similarweb.com/top-websites/","category": "meta"},
    {"url": "https://trends.google.com/trends/explore","category": "trends"},
]

KNOWN_DOMAINS = {
    "google.com", "youtube.com", "reddit.com", "amazon.com",
    "flipkart.com", "tradingview.com", "finance.yahoo.com",
    "twitter.com", "x.com", "facebook.com", "instagram.com",
}

CATEGORY_HINTS = {
    "finance": ["stock", "crypto", "invest", "market", "trading", "finance"],
    "commerce": ["shop", "product", "deal", "price", "buy", "ecommerce"],
    "discussion": ["forum", "community", "discuss", "thread", "reddit"],
    "content": ["video", "viral", "trending", "content", "media"],
    "tools": ["tool", "app", "software", "saas", "startup", "launch"],
}


def _infer_category(url: str, title: str) -> str:
    text = (url + " " + title).lower()
    for cat, hints in CATEGORY_HINTS.items():
        if any(h in text for h in hints):
            return cat
    return "general"


def _extract_domain(url: str) -> str:
    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return match.group(1) if match else url


class MetaDiscovery:
    def __init__(self, db_session):
        self._db = db_session

    async def run(self) -> List[Dict]:
        """Returns list of newly discovered sources."""
        discovered = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
                )
            )

            # ── 1. Google search discovery ────────────────────────────────
            for query in DISCOVERY_QUERIES[:4]:   # limit to avoid blocks
                try:
                    page = await context.new_page()
                    await page.goto(
                        f"https://www.google.com/search?q={query.replace(' ', '+')}",
                        wait_until="domcontentloaded",
                    )
                    import asyncio
                    await asyncio.sleep(2)

                    links = await page.query_selector_all("a[href]")
                    for link in links:
                        href  = await link.get_attribute("href") or ""
                        label = (await link.inner_text()).strip()
                        if href.startswith("http") and "google" not in href:
                            domain = _extract_domain(href)
                            if domain not in KNOWN_DOMAINS:
                                cat = _infer_category(href, label)
                                discovered.append({
                                    "url": href[:500],
                                    "name": domain,
                                    "category": cat,
                                })
                    await page.close()
                except Exception as e:
                    logger.debug(f"[meta_discovery] google query error: {e}")

            # ── 2. Seed directory traversal ───────────────────────────────
            for seed in SEED_DIRECTORIES:
                try:
                    page = await context.new_page()
                    await page.goto(seed["url"], wait_until="domcontentloaded")
                    import asyncio
                    await asyncio.sleep(2)

                    links = await page.query_selector_all("a[href]")
                    for link in links:
                        href = await link.get_attribute("href") or ""
                        if href.startswith("http"):
                            domain = _extract_domain(href)
                            if domain not in KNOWN_DOMAINS and len(domain) > 4:
                                discovered.append({
                                    "url": href[:500],
                                    "name": domain,
                                    "category": seed["category"],
                                })
                    await page.close()
                except Exception as e:
                    logger.debug(f"[meta_discovery] seed error: {e}")

            await browser.close()

        # ── Deduplicate & persist ─────────────────────────────────────────
        return await self._persist(discovered)

    async def _persist(self, sources: List[Dict]) -> List[Dict]:
        from db.models import DiscoveredSource
        seen_urls = set()
        new_sources = []

        for s in sources:
            url = s["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)

            existing = (
                self._db.query(DiscoveredSource)
                .filter(DiscoveredSource.url == url)
                .first()
            )
            if not existing:
                ds = DiscoveredSource(
                    url=url,
                    name=s.get("name", ""),
                    category=s.get("category", "general"),
                    tier=2,
                )
                self._db.add(ds)
                new_sources.append(s)

        self._db.commit()
        logger.info(f"[meta_discovery] {len(new_sources)} new sources discovered")
        return new_sources

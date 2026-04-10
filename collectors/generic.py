"""
Tier-2 Generic Collector
Handles newly discovered surfaces using DOM heuristics.
Skips blocked domains and low-quality pages.
"""
from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal
from config import BLOCKED_DOMAINS, MIN_SIGNALS_TO_KEEP

HEURISTIC_SELECTORS = [
    "table tbody tr",
    "ol li", "ul.ranked li", "ul.trending li",
    ".trending-item", ".rank-item", ".top-item",
    ".card", ".item", ".post", ".entry",
    "article",
    "h2", "h3",
]

TEXT_SELECTORS = ["h2", "h3", "h4", "a", ".title", ".name", "span.text"]
NUMBER_SELECTORS = [
    ".count", ".views", ".score", ".votes", ".rank",
    "span[class*='count']", "span[class*='num']",
    "td:nth-child(2)", "td:nth-child(3)",
]

# Min text length to be a valid entity
MIN_ENTITY_LEN = 8
# Skip if entity looks like a navigation link or legal text
SKIP_KEYWORDS = {
    "cookie", "privacy policy", "terms of service", "copyright",
    "all rights reserved", "contact us", "about us", "sign in",
    "log in", "sign up", "subscribe", "newsletter", "follow us",
    "english", "language", "addendum", "notice to", "dispute",
    "legal", "gdpr", "california residents",
}


def _is_valid_entity(text: str) -> bool:
    t = text.lower().strip()
    if len(t) < MIN_ENTITY_LEN:
        return False
    if any(skip in t for skip in SKIP_KEYWORDS):
        return False
    # Skip if mostly numbers/symbols
    alpha_ratio = sum(c.isalpha() for c in t) / max(len(t), 1)
    if alpha_ratio < 0.3:
        return False
    return True


class GenericCollector(BaseCollector):
    tier = 2

    def __init__(self, source_name: str, url: str, category: str, playwright_instance=None):
        super().__init__(playwright_instance)
        self.name      = source_name
        self._url      = url
        self._category = category

    def _is_blocked(self) -> bool:
        from urllib.parse import urlparse
        try:
            domain = urlparse(self._url).netloc.replace("www.", "")
            return any(domain == b or domain.endswith("." + b) for b in BLOCKED_DOMAINS)
        except Exception:
            return False

    async def _collect(self) -> List[Signal]:
        if self._is_blocked():
            logger.debug(f"[generic] skipping blocked domain: {self._url}")
            return []

        page    = await self._new_page()
        signals: List[Signal] = []

        if not await self._safe_goto(page, self._url):
            return signals

        await page.keyboard.press("End")
        await self._human_delay()

        items = []
        used_selector = None

        for sel in HEURISTIC_SELECTORS:
            try:
                candidates = await page.query_selector_all(sel)
                if len(candidates) >= 3:
                    items = candidates
                    used_selector = sel
                    break
            except Exception:
                continue

        logger.info(f"[generic:{self.name}] selector={used_selector} items={len(items)}")

        for rank, item in enumerate(items[:40]):
            try:
                entity = ""
                for ts in TEXT_SELECTORS:
                    el = await item.query_selector(ts)
                    if el:
                        t = (await el.inner_text()).strip()
                        if _is_valid_entity(t):
                            entity = t[:120]
                            break

                if not entity:
                    raw = (await item.inner_text()).strip()[:120]
                    if _is_valid_entity(raw):
                        entity = raw

                if not entity:
                    continue

                raw_val = float(rank + 1)
                for ns in NUMBER_SELECTORS:
                    el = await item.query_selector(ns)
                    if el:
                        t = (await el.inner_text()).strip().replace(",", "")
                        try:
                            raw_val = float(t)
                            break
                        except ValueError:
                            continue

                signals.append(self._make_signal(
                    entity=entity,
                    raw_value=raw_val,
                    raw_meta={"rank": rank + 1, "selector": used_selector},
                    url=self._url,
                    category=self._category,
                ))
            except Exception as e:
                logger.debug(f"[generic:{self.name}] item error: {e}")

        await page.close()

        if len(signals) < MIN_SIGNALS_TO_KEEP:
            logger.debug(f"[generic:{self.name}] too few signals ({len(signals)}), discarding")
            return []

        return signals

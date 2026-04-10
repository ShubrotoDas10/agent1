import asyncio
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from loguru import logger
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from config import HEADLESS, BROWSER_TIMEOUT, HUMAN_DELAY_MIN, HUMAN_DELAY_MAX


@dataclass
class Signal:
    """Unified schema enforced across ALL collectors."""
    entity:      str
    source:      str
    timestamp:   datetime
    raw_value:   float
    raw_meta:    Dict[str, Any] = field(default_factory=dict)
    url:         str = ""
    category:    str = "general"
    source_tier: int = 1


class BaseCollector(ABC):
    """
    Every collector inherits this.  Handles:
      - browser lifecycle (shared playwright instance passed in)
      - human-like delays
      - retry logic
      - cookie/session persistence
      - output schema validation
    """

    name: str = "base"
    tier: int = 1

    def __init__(self, playwright_instance=None):
        self._pw   = playwright_instance
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    # ── Browser helpers ───────────────────────────────────────────────────────

    async def _ensure_browser(self):
        if self._browser and self._browser.is_connected():
            return
        self._browser = await self._pw.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
            ],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        # stealth: mask webdriver flag
        await self._context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )

    async def _new_page(self) -> Page:
        await self._ensure_browser()
        page = await self._context.new_page()
        page.set_default_timeout(BROWSER_TIMEOUT)
        return page

    async def _human_delay(self):
        await asyncio.sleep(random.uniform(HUMAN_DELAY_MIN, HUMAN_DELAY_MAX))

    async def _safe_goto(self, page: Page, url: str, retries: int = 3) -> bool:
        for attempt in range(retries):
            try:
                await page.goto(url, wait_until="domcontentloaded")
                await self._human_delay()
                return True
            except Exception as e:
                logger.warning(f"[{self.name}] goto failed (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
        return False

    async def close(self):
        if self._browser:
            await self._browser.close()

    # ── Public interface ──────────────────────────────────────────────────────

    async def collect(self) -> List[Signal]:
        """Entry point — wraps _collect with error handling."""
        try:
            signals = await self._collect()
            logger.info(f"[{self.name}] collected {len(signals)} signals")
            return signals
        except Exception as e:
            logger.error(f"[{self.name}] collection failed: {e}")
            return []

    @abstractmethod
    async def _collect(self) -> List[Signal]:
        ...

    # ── Schema helper ─────────────────────────────────────────────────────────

    def _make_signal(
        self,
        entity: str,
        raw_value: float,
        raw_meta: dict = None,
        url: str = "",
        category: str = "general",
    ) -> Signal:
        return Signal(
            entity=entity.strip().lower(),
            source=self.name,
            timestamp=datetime.now(timezone.utc),
            raw_value=raw_value,
            raw_meta=raw_meta or {},
            url=url,
            category=category,
            source_tier=self.tier,
        )

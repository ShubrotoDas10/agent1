import asyncio
from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal

# Public RSS endpoints - no auth needed
RSS_URLS = [
    ("https://trends.google.com/trends/trendingsearches/daily/rss?geo=US", "US"),
    ("https://trends.google.com/trends/trendingsearches/daily/rss?geo=IN", "IN"),
    ("https://trends.google.com/trends/trendingsearches/daily/rss?geo=GB", "GB"),
]

# Backup: scrape the explore page
EXPLORE_QUERIES = [
    "https://trends.google.com/trends/explore?date=now%201-d&geo=US",
]

class GoogleTrendsCollector(BaseCollector):
    name = "google_trends"

    async def _collect(self) -> List[Signal]:
        signals: List[Signal] = []

        # Method 1: RSS feed (most reliable, plain XML)
        import httpx
        for rss_url, geo in RSS_URLS:
            try:
                async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                    resp = await client.get(rss_url, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                    })
                    if resp.status_code == 200:
                        import re
                        titles = re.findall(r"<title><!\[CDATA\[(.+?)\]\]></title>", resp.text)
                        traffics = re.findall(r"<ht:approx_traffic>(.+?)</ht:approx_traffic>", resp.text)

                        for i, title in enumerate(titles[:20]):
                            if title.lower() in ("google trends", ""):
                                continue
                            traffic_str = traffics[i] if i < len(traffics) else "0"
                            raw_val = self._parse_volume(traffic_str)
                            signals.append(self._make_signal(
                                entity=title.strip(),
                                raw_value=raw_val,
                                raw_meta={"rank": i+1, "geo": geo, "traffic": traffic_str},
                                url=rss_url,
                                category="trends",
                            ))
                        logger.info(f"[google_trends] RSS {geo}: {len(titles)} trends")
            except Exception as e:
                logger.warning(f"[google_trends] RSS {geo} failed: {e}")

        if signals:
            return signals

        # Method 2: Browser scrape as fallback
        for url in EXPLORE_QUERIES:
            page = await self._new_page()
            try:
                if not await self._safe_goto(page, url):
                    continue
                await asyncio.sleep(3)

                titles = await page.evaluate("""
                    () => {
                        const els = document.querySelectorAll('.label-text, .trending-searches-item .title a');
                        return Array.from(els).map(e=>e.innerText.trim()).filter(t=>t.length>2);
                    }
                """)
                for i, t in enumerate(titles[:20]):
                    signals.append(self._make_signal(
                        entity=t, raw_value=float(20-i),
                        raw_meta={"rank":i+1,"method":"browser"},
                        url=url, category="trends",
                    ))
                logger.info(f"[google_trends] browser scrape: {len(titles)} trends")
            except Exception as e:
                logger.warning(f"[google_trends] browser fallback failed: {e}")
            finally:
                await page.close()

        return signals

    @staticmethod
    def _parse_volume(text: str) -> float:
        text = text.upper().replace("+","").replace(",","").strip()
        if "K" in text:
            return float(text.replace("K","")) * 1_000
        if "M" in text:
            return float(text.replace("M","")) * 1_000_000
        try:
            return float(text)
        except:
            return 0.0

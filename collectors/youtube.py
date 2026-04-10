from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal

YT_PAGES = {
    "trending_gaming": ("https://www.youtube.com/feed/trending?bp=4gIcGhpnYW1pbmdfY29ycHVzX21vc3RfcG9wdWxhcg%3D%3D", "gaming"),
    "trending_music":  ("https://www.youtube.com/feed/trending?bp=4gINGgt5dG1hX2NoYXJ0cw%3D%3D", "music"),
    "trending_now":    ("https://www.youtube.com/feed/trending", "general"),
    "search_tech":     ("https://www.youtube.com/results?search_query=trending+technology+2026&sp=CAISAhAB", "tech"),
    "search_finance":  ("https://www.youtube.com/results?search_query=stock+market+crypto+2026&sp=CAISAhAB", "finance"),
    "search_music":    ("https://www.youtube.com/results?search_query=trending+music+2026&sp=CAISAhAB", "music"),
}

class YouTubeCollector(BaseCollector):
    name = "youtube"

    async def _collect(self) -> List[Signal]:
        signals: List[Signal] = []
        for cat, (url, category) in YT_PAGES.items():
            page = await self._new_page()
            try:
                if not await self._safe_goto(page, url):
                    continue

                # Dismiss consent/cookie dialogs
                for sel in ["button[aria-label*='Accept']", "button[aria-label*='agree']",
                            ".yt-spec-button-shape-next--filled", "#accept-button"]:
                    try:
                        btn = await page.query_selector(sel)
                        if btn:
                            await btn.click()
                            await self._human_delay()
                            break
                    except Exception:
                        pass

                # Scroll to load content
                for _ in range(2):
                    await page.keyboard.press("End")
                    await self._human_delay()

                # Try multiple selectors
                items = []
                for sel in ["ytd-video-renderer", "ytd-grid-video-renderer", "ytd-compact-video-renderer", "ytd-rich-item-renderer"]:
                    items = await page.query_selector_all(sel)
                    if items:
                        break

                logger.info(f"[youtube:{cat}] found {len(items)} videos")

                if not items:
                    # JS fallback
                    titles = await page.evaluate("""
                        () => Array.from(document.querySelectorAll('#video-title, a#video-title'))
                            .slice(0,25).map(e=>({
                                title:(e.getAttribute('title')||e.innerText||'').trim(),
                                href:e.getAttribute('href')||''
                            })).filter(d=>d.title.length>3)
                    """)
                    for i, d in enumerate(titles):
                        signals.append(self._make_signal(
                            entity=d['title'], raw_value=float(25-i),
                            raw_meta={"rank":i+1,"subcategory":cat},
                            url=f"https://youtube.com{d['href']}" if d['href'] else url,
                            category=category,
                        ))
                    logger.info(f"[youtube:{cat}] JS fallback: {len(titles)} titles")
                else:
                    for rank, item in enumerate(items[:25]):
                        try:
                            title_el = await item.query_selector("#video-title, a#video-title")
                            views_el = await item.query_selector("#metadata-line span:first-child, .ytd-video-meta-block span:first-child")
                            title = ""
                            if title_el:
                                title = await title_el.get_attribute("title") or await title_el.inner_text()
                                title = title.strip()
                            if not title:
                                continue
                            views_t = (await views_el.inner_text()).strip() if views_el else "0"
                            href    = await title_el.get_attribute("href") if title_el else ""
                            signals.append(self._make_signal(
                                entity=title, raw_value=self._parse_views(views_t) or float(25-rank),
                                raw_meta={"rank":rank+1,"views":views_t,"subcategory":cat},
                                url=f"https://youtube.com{href}" if href else url,
                                category=category,
                            ))
                        except Exception as e:
                            logger.debug(f"[youtube:{cat}] item error: {e}")
            except Exception as e:
                logger.error(f"[youtube:{cat}] error: {e}")
            finally:
                await page.close()
        return signals

    @staticmethod
    def _parse_views(text: str) -> float:
        t = text.upper().replace("VIEWS","").replace(",","").strip()
        if "K" in t:
            return float(t.replace("K",""))*1_000
        if "M" in t:
            return float(t.replace("M",""))*1_000_000
        if "B" in t:
            return float(t.replace("B",""))*1_000_000_000
        try:
            return float(t)
        except:
            return 0.0

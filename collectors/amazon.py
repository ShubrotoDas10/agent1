from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal

AMAZON_PAGES = {
    "movers_shakers": "https://www.amazon.com/gp/movers-and-shakers/",
    "best_sellers":   "https://www.amazon.com/bestsellers/",
    "new_releases":   "https://www.amazon.com/gp/new-releases/",
    "hot_new":        "https://www.amazon.com/gp/new-releases/electronics/",
}

class AmazonCollector(BaseCollector):
    name = "amazon"

    async def _collect(self) -> List[Signal]:
        signals: List[Signal] = []
        for page_name, url in AMAZON_PAGES.items():
            page = await self._new_page()
            try:
                if not await self._safe_goto(page, url):
                    continue
                await self._human_delay()

                # Try scrolling to load lazy content
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
                await self._human_delay()

                # Multiple selector attempts for Amazon's frequently changing DOM
                selector_groups = [
                    # Movers & shakers / BSR style
                    ".zg-item-immersion",
                    ".p13n-desktop-grid .zg-item",
                    "[data-component-type='s-search-result']",
                    ".a-list-item .zg-bdg-img",
                    # New style grid
                    "li.zg-item-immersion",
                    "div.zg-grid-general-faceout",
                    # Fallback
                    ".s-result-item[data-asin]",
                ]

                items = []
                for sel in selector_groups:
                    items = await page.query_selector_all(sel)
                    if len(items) >= 3:
                        logger.info(f"[amazon:{page_name}] selector '{sel}' → {len(items)} items")
                        break

                if not items:
                    # Last resort: grab all product titles via JS
                    titles = await page.evaluate("""
                        () => {
                            const els = document.querySelectorAll(
                                '.p13n-sc-truncate, .p13n-sc-line-clamp-1, ' +
                                '[data-hook="title"], .a-size-base-plus'
                            );
                            return Array.from(els).slice(0,40).map(e => e.innerText.trim()).filter(t => t.length > 5);
                        }
                    """)
                    for i, title in enumerate(titles):
                        signals.append(self._make_signal(
                            entity=title, raw_value=1.0/(i+1),
                            raw_meta={"rank": i+1, "page_type": page_name},
                            url=url, category="commerce",
                        ))
                    logger.info(f"[amazon:{page_name}] JS fallback: {len(titles)} titles")
                else:
                    for rank, item in enumerate(items[:40]):
                        try:
                            title_el = await item.query_selector(
                                ".p13n-sc-truncate, .p13n-sc-line-clamp-1, "
                                ".a-size-base-plus, span[data-hook='title'], "
                                ".zg-text-center-align span"
                            )
                            price_el = await item.query_selector(".p13n-sc-price, .a-price .a-offscreen")
                            title  = (await title_el.inner_text()).strip() if title_el else ""
                            price  = (await price_el.inner_text()).strip() if price_el else ""
                            if not title or len(title) < 4:
                                continue
                            signals.append(self._make_signal(
                                entity=title, raw_value=1.0/(rank+1),
                                raw_meta={"rank": rank+1, "price": price, "page_type": page_name},
                                url=url, category="commerce",
                            ))
                        except Exception as e:
                            logger.debug(f"[amazon] item error: {e}")
            except Exception as e:
                logger.error(f"[amazon:{page_name}] error: {e}")
            finally:
                await page.close()
        return signals

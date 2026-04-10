from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal

FLIPKART_PAGES = {
    "deals_of_day":  "https://www.flipkart.com/deals-of-the-day",
    "trending":      "https://www.flipkart.com/all-in-one",
    "electronics":   "https://www.flipkart.com/search?q=trending+electronics&sort=popularity",
    "mobiles":       "https://www.flipkart.com/search?q=bestseller+mobile&sort=popularity",
}

class FlipkartCollector(BaseCollector):
    name = "flipkart"

    async def _collect(self) -> List[Signal]:
        signals: List[Signal] = []
        for page_name, url in FLIPKART_PAGES.items():
            page = await self._new_page()
            try:
                if not await self._safe_goto(page, url):
                    continue
                await self._human_delay()

                # Dismiss login popup
                try:
                    close = await page.query_selector("button._2KpZ6l._2doB4z, button[class*='close']")
                    if close:
                        await close.click()
                        await self._human_delay()
                except Exception:
                    pass

                # Scroll to load products
                await page.evaluate("window.scrollTo(0, 600)")
                await self._human_delay()

                selector_groups = [
                    "div[data-id]",
                    "._1AtVbE",
                    "._4ddWXP",
                    "._2kHMtA",
                    ".s1Q9rs",
                    "._13oc-S",   # search results
                    "div._1xHGtK",
                ]

                items = []
                for sel in selector_groups:
                    items = await page.query_selector_all(sel)
                    if len(items) >= 3:
                        logger.info(f"[flipkart:{page_name}] selector '{sel}' → {len(items)} items")
                        break

                if not items:
                    # JS fallback — grab product names directly
                    titles = await page.evaluate("""
                        () => {
                            const els = document.querySelectorAll(
                                '._4rR01T, .IRpwTa, ._2WkVRV, [class*="title"], ' +
                                'a[title], div[title]'
                            );
                            return Array.from(els).slice(0,40)
                                .map(e => (e.getAttribute("title") || e.innerText || "").trim())
                                .filter(t => t.length > 5);
                        }
                    """)
                    for i, title in enumerate(titles[:30]):
                        signals.append(self._make_signal(
                            entity=title, raw_value=1.0/(i+1),
                            raw_meta={"rank": i+1, "page_type": page_name},
                            url=url, category="commerce",
                        ))
                    logger.info(f"[flipkart:{page_name}] JS fallback: {len(titles)} titles")
                else:
                    for rank, item in enumerate(items[:30]):
                        try:
                            title_el  = await item.query_selector("._4rR01T, .IRpwTa, ._2WkVRV, a[title]")
                            price_el  = await item.query_selector("._30jeq3, ._1_WHN1")
                            rating_el = await item.query_selector("._3LWZlK")
                            title = ""
                            if title_el:
                                title = await title_el.get_attribute("title") or await title_el.inner_text()
                                title = title.strip()
                            if not title or len(title) < 4:
                                continue
                            price  = (await price_el.inner_text()).strip() if price_el else ""
                            rating = (await rating_el.inner_text()).strip() if rating_el else "0"
                            signals.append(self._make_signal(
                                entity=title, raw_value=1.0/(rank+1),
                                raw_meta={"rank": rank+1, "price": price, "rating": rating, "page_type": page_name},
                                url=url, category="commerce",
                            ))
                        except Exception as e:
                            logger.debug(f"[flipkart] item error: {e}")
            except Exception as e:
                logger.error(f"[flipkart:{page_name}] error: {e}")
            finally:
                await page.close()
        return signals

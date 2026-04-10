from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal

TV_PAGES = {
    "most_active":    "https://www.tradingview.com/markets/stocks-usa/market-movers-active/",
    "gainers":        "https://www.tradingview.com/markets/stocks-usa/market-movers-gainers/",
    "crypto_market":  "https://www.tradingview.com/markets/cryptocurrencies/prices-all/",
    "forex":          "https://www.tradingview.com/markets/currencies/rates-all/",
}

class TradingViewCollector(BaseCollector):
    name = "tradingview"

    async def _collect(self) -> List[Signal]:
        signals: List[Signal] = []
        for page_name, url in TV_PAGES.items():
            page = await self._new_page()
            try:
                if not await self._safe_goto(page, url):
                    continue

                # TradingView is heavy JS — wait longer
                try:
                    await page.wait_for_selector(
                        "tr.row-RdUXZpkv, .tv-screener-table__result-row, "
                        "table[class*='table'] tr, .symbol-table tr",
                        timeout=18000
                    )
                except Exception:
                    await self._human_delay()

                # Try multiple row selectors
                row_selectors = [
                    "tr.row-RdUXZpkv",
                    ".tv-screener-table__result-row",
                    "table tr:not(:first-child)",
                    "[class*='listRow']",
                    "[class*='row-']:not([class*='header'])",
                ]

                rows = []
                for sel in row_selectors:
                    rows = await page.query_selector_all(sel)
                    if len(rows) >= 3:
                        logger.info(f"[tradingview:{page_name}] selector '{sel}' → {len(rows)} rows")
                        break

                if not rows:
                    # JS extraction fallback
                    data = await page.evaluate("""
                        () => {
                            const rows = document.querySelectorAll('tr');
                            return Array.from(rows).slice(1, 31).map(r => {
                                const cells = r.querySelectorAll('td');
                                return {
                                    name: cells[0]?.innerText?.trim() || '',
                                    change: cells[4]?.innerText?.trim() || cells[3]?.innerText?.trim() || '0',
                                };
                            }).filter(d => d.name.length > 0);
                        }
                    """)
                    for i, d in enumerate(data):
                        try:
                            val = float(d['change'].replace('%','').replace('+','').replace(',',''))
                        except:
                            val = 0.0
                        if d['name']:
                            signals.append(self._make_signal(
                                entity=d['name'], raw_value=val,
                                raw_meta={"rank": i+1, "change": d['change'], "page_type": page_name},
                                url=url, category="finance",
                            ))
                    logger.info(f"[tradingview:{page_name}] JS fallback: {len(data)} rows")
                else:
                    for rank, row in enumerate(rows[:30]):
                        try:
                            name_el   = await row.query_selector("td:first-child, [class*='symbol'], [class*='name']")
                            change_el = await row.query_selector("td:nth-child(5), td:nth-child(4), [class*='change']")
                            name   = (await name_el.inner_text()).strip() if name_el else ""
                            change = (await change_el.inner_text()).strip() if change_el else "0"
                            if not name:
                                continue
                            try:
                                val = float(change.replace('%','').replace('+','').replace(',',''))
                            except:
                                val = 0.0
                            signals.append(self._make_signal(
                                entity=name, raw_value=val,
                                raw_meta={"rank": rank+1, "change": change, "page_type": page_name},
                                url=url, category="finance",
                            ))
                        except Exception as e:
                            logger.debug(f"[tradingview] row error: {e}")
            except Exception as e:
                logger.error(f"[tradingview:{page_name}] error: {e}")
            finally:
                await page.close()
        return signals

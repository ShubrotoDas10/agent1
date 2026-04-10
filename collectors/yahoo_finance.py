from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal

YF_PAGES = {
    "trending_tickers": "https://finance.yahoo.com/trending-tickers",
    "most_active":      "https://finance.yahoo.com/most-active",
    "gainers":          "https://finance.yahoo.com/gainers",
}


class YahooFinanceCollector(BaseCollector):
    name = "yahoo_finance"

    async def _collect(self) -> List[Signal]:
        signals: List[Signal] = []

        for page_name, url in YF_PAGES.items():
            page = await self._new_page()
            try:
                if not await self._safe_goto(page, url):
                    continue

                try:
                    await page.wait_for_selector("table tbody tr", timeout=12000)
                except Exception:
                    pass

                rows = await page.query_selector_all("table tbody tr")
                logger.info(f"[yahoo_finance:{page_name}] found {len(rows)} rows")

                for rank, row in enumerate(rows[:30]):
                    try:
                        cells = await row.query_selector_all("td")
                        if len(cells) < 3:
                            continue

                        symbol   = (await cells[0].inner_text()).strip()
                        name_t   = (await cells[1].inner_text()).strip() if len(cells) > 1 else symbol
                        change_t = (await cells[4].inner_text()).strip() if len(cells) > 4 else "0"

                        if not symbol:
                            continue

                        try:
                            raw_val = float(change_t.replace("%","").replace("+","").replace(",",""))
                        except ValueError:
                            raw_val = 0.0

                        signals.append(self._make_signal(
                            entity=f"{symbol} {name_t}".strip(),
                            raw_value=raw_val,
                            raw_meta={
                                "symbol": symbol,
                                "name": name_t,
                                "change_text": change_t,
                                "page_type": page_name,
                                "rank": rank + 1,
                            },
                            url=url,
                            category="finance",
                        ))
                    except Exception as e:
                        logger.debug(f"[yahoo_finance] row error: {e}")
            finally:
                await page.close()

        return signals

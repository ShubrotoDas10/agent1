from typing import List
from loguru import logger
from collectors.base import BaseCollector, Signal

# Using old.reddit for reliability + adding more subreddits for niche coverage
SUBREDDITS = [
    # General
    ("r/technology",    "tech"),
    ("r/business",      "business"),
    ("r/finance",       "finance"),
    ("r/investing",     "finance"),
    ("r/entrepreneur",  "business"),
    ("r/startups",      "business"),
    ("r/gaming",        "gaming"),
    ("r/science",       "science"),
    ("r/worldnews",     "news"),
    ("r/news",          "news"),
    # Niche-rich subreddits
    ("r/artificial",    "ai"),
    ("r/MachineLearning","ai"),
    ("r/CryptoCurrency", "crypto"),
    ("r/stocks",        "finance"),
    ("r/personalfinance","finance"),
    ("r/SideProject",   "business"),
    ("r/Marketing",     "marketing"),
    ("r/learnprogramming","tech"),
    ("r/webdev",        "tech"),
    ("r/datascience",   "ai"),
]

class RedditCollector(BaseCollector):
    name = "reddit"

    async def _collect(self) -> List[Signal]:
        signals: List[Signal] = []
        for sub, category in SUBREDDITS:
            page = await self._new_page()
            url  = f"https://old.reddit.com/{sub}/?sort=hot"
            try:
                if not await self._safe_goto(page, url):
                    continue

                # old.reddit uses straightforward HTML
                posts = await page.query_selector_all("div.thing.link:not(.promoted)")
                logger.info(f"[reddit:{sub}] found {len(posts)} posts")

                if not posts:
                    # Try new reddit as fallback
                    posts = await page.query_selector_all("article, [data-testid='post-container']")

                for rank, post in enumerate(posts[:20]):
                    try:
                        title_el = await post.query_selector("a.title, h3")
                        score_el = await post.query_selector(
                            ".score.unvoted, .score.likes, .score.dislikes, [class*='score']"
                        )
                        title  = (await title_el.inner_text()).strip() if title_el else ""
                        score_t= ""
                        if score_el:
                            score_t = await score_el.get_attribute("title") or await score_el.inner_text()
                            score_t = score_t.strip()
                        href = await title_el.get_attribute("href") if title_el else ""

                        if not title or len(title) < 5:
                            continue

                        try:
                            raw_val = float(score_t.replace(",","")) if score_t and score_t.lstrip("-").isdigit() else float(rank + 1) * 10
                        except:
                            raw_val = float(rank + 1) * 10

                        signals.append(self._make_signal(
                            entity=title,
                            raw_value=raw_val,
                            raw_meta={"rank": rank+1, "subreddit": sub, "score_text": score_t, "category": category},
                            url=f"https://reddit.com{href}" if href and href.startswith("/") else href,
                            category=category,
                        ))
                    except Exception as e:
                        logger.debug(f"[reddit:{sub}] post error: {e}")
            except Exception as e:
                logger.error(f"[reddit:{sub}] error: {e}")
            finally:
                await page.close()
        return signals

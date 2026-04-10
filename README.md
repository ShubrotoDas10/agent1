# Agent 1 — Global Signal Radar

Browser-driven, domain-agnostic trend discovery and signal scoring engine.

## Setup

```bash
# 1. Create & activate venv
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Install Playwright browsers
playwright install chromium

# 4. Install spaCy model
python -m spacy download en_core_web_sm

# 5. Run (everything starts on one command)
python run.py
```

## Access

- Dashboard : http://localhost:9100
- API docs  : http://localhost:9100/docs

## What happens on startup

1. DB tables created automatically
2. Pipeline triggers immediately (background thread)
3. APScheduler runs pipeline every 2 hours
4. All logs visible in terminal

## Pipeline flow

```
Meta-Discovery → Tier-1 Collectors → Tier-2 Generic Collectors
    → Normalize → Signal Engine → Score → Attention Allocate → Rank
```

## Sources (Tier-1)

| Source        | Data                        |
|---------------|-----------------------------|
| Google Trends | Trending searches + volume  |
| YouTube       | Trending videos + views     |
| Reddit        | Hot posts + upvotes         |
| Amazon        | Movers, bestsellers         |
| Flipkart      | Trending products           |
| TradingView   | Stocks + crypto movers      |
| Yahoo Finance | Trending tickers + gainers  |

Tier-2 sources are discovered automatically each cycle.

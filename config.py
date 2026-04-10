import os
from dotenv import load_dotenv

load_dotenv()

# Database
DB_USER     = os.getenv("DB_USER", "sdj")
DB_PASSWORD = os.getenv("DB_PASSWORD", "12345678")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "searcher")
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Server
API_HOST = "0.0.0.0"
API_PORT = 9100

# Scheduler
COLLECTION_INTERVAL_HOURS    = 2
HIGH_VOLATILITY_INTERVAL_MIN = 30
LOW_VOLATILITY_INTERVAL_MIN  = 240

# Scoring weights
WEIGHTS = {
    "velocity":     0.40,
    "acceleration": 0.25,
    "consistency":  0.20,
    "source_count": 0.15,
}

# Lifecycle — based on norm_value so first run still gets variety
LIFECYCLE = {
    "early_spike": 0.75,
    "emerging":    0.45,
    "peaking":     0.20,
}

# NLP
EMBEDDING_MODEL    = "all-MiniLM-L6-v2"
CLUSTER_SIMILARITY = 0.72

# Groq
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "YOUR_GROQ_API_KEY")
GROQ_MODEL     = "llama-3.3-70b-versatile"
GROQ_LABEL_TOP = 50

# Browser
HEADLESS        = True
BROWSER_TIMEOUT = 30_000
HUMAN_DELAY_MIN = 1.2
HUMAN_DELAY_MAX = 3.5

# Blocked domains for tier-2 generic collector
BLOCKED_DOMAINS = {
    "cloudflare.com", "google.com", "googleapis.com", "gstatic.com",
    "goo.gl", "maps.app.goo.gl",
    "similarweb.com", "account.similarweb.com", "lp.similarweb.com",
    "secure.similarweb.com", "academy.similarweb.com", "ir.similarweb.com",
    "support.similarweb.com", "developers.similarweb.com",
    "facebook.com", "twitter.com", "x.com", "instagram.com", "tiktok.com",
    "apple.com", "microsoft.com", "amazon.com", "linkedin.com",
    "wikipedia.org", "wikimedia.org", "archive.org",
    "w3.org", "schema.org", "iana.org", "ietf.org",
    "only-eu.eu", "playlists.at", "moonrf.com", "lalitm.com",
    "apps.apple.com", "jsnover.com", "chromewebstore.google.com",
    "mp.weixin.qq.com", "cookiebot.com", "privacy.microsoft.com",
    "bugcrowd.com", "support.giphy.com",
}

MIN_SIGNALS_TO_KEEP = 3

TIER1_SOURCES = [
    "google_trends", "youtube", "reddit",
    "amazon", "flipkart", "tradingview", "yahoo_finance",
]

TOP_N_OPPORTUNITIES = 50

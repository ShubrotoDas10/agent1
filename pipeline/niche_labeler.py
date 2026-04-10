"""
Niche Labeler - Broad top-level niches, taxonomy after Groq
"""
import json, re, httpx
from typing import List, Dict
from loguru import logger
from config import GROQ_API_KEY, GROQ_MODEL, GROQ_LABEL_TOP

# 8 broad human-meaningful top-level niches
BROAD_NICHES = {
    "Investment":    ["stock", "crypto", "bitcoin", "ethereum", "btc", "eth", "solana", "cardano", "dogecoin", "ripple", "altcoin", "nft", "defi", "forex", "commodity", "gold", "oil", "equity", "shares", "nasdaq", "nyse", "s&p", "dow", "ipo", "fund", "etf", "bond", "treasury", "hedge", "portfolio", "dividend", "capital", "asset", "trading", "invest", "market cap", "bull", "bear", "rally", "crash", "avalanche crypto", "hedera crypto", "litecoin", "polkadot", "chainlink", "uniswap", "aave", "compound", "maker", "yearn"],
    "Business":      ["startup", "founder", "entrepreneur", "business", "revenue", "profit", "vc", "venture", "funding", "series a", "series b", "seed", "unicorn", "saas", "b2b", "b2c", "marketing", "sales", "growth", "customer", "product market fit", "pivot", "burn rate", "valuation", "acquisition", "merger", "ipo", "listing", "freelance", "side hustle", "agency", "consulting", "ecommerce", "shopify", "amazon fba", "dropshipping", "affiliate", "passive income", "monetize", "brand", "pitch deck", "accelerator"],
    "Technology":    ["ai", "artificial intelligence", "machine learning", "llm", "gpt", "software", "code", "developer", "github", "api", "cloud", "aws", "azure", "gcp", "blockchain", "web3", "cybersecurity", "hack", "data science", "python", "javascript", "react", "docker", "kubernetes", "devops", "open source", "framework", "chip", "semiconductor", "nvidia", "robot", "automation", "saas platform", "app", "mobile", "ios", "android", "quantum", "ar", "vr", "metaverse"],
    "Entertainment": ["game", "gaming", "gta", "minecraft", "fortnite", "esports", "stream", "twitch", "youtube", "netflix", "movie", "film", "series", "music", "song", "artist", "album", "spotify", "concert", "rapper", "pop", "viral", "trend", "meme", "influencer", "tiktok", "instagram", "creator", "content", "social media", "celebrity", "hollywood", "box office", "anime", "manga", "comic", "sports", "nba", "nfl", "soccer", "cricket", "football", "basketball", "free fire", "valorant", "league of legends"],
    "Commerce":      ["product", "review", "buy", "sell", "price", "deal", "discount", "amazon", "flipkart", "ebay", "walmart", "shopify", "bestseller", "new release", "gadget", "electronics", "phone", "laptop", "camera", "kettle", "appliance", "furniture", "clothing", "fashion", "shoe", "sneaker", "beauty", "skincare", "makeup", "cosmetic", "food", "beverage", "supplement", "health product", "home", "garden", "outdoor", "sports equipment", "toy", "baby"],
    "Health":        ["health", "medical", "medicine", "drug", "fda", "clinical trial", "disease", "cancer", "diabetes", "heart", "mental health", "anxiety", "depression", "therapy", "wellness", "fitness", "workout", "gym", "nutrition", "diet", "weight loss", "sleep", "stress", "mindfulness", "meditation", "yoga", "supplement", "vitamin", "protein", "biohacking", "longevity", "vaccine", "research", "study", "hospital", "pharma", "biotech", "genetics", "crispr"],
    "World & News":  ["news", "politics", "election", "government", "president", "war", "conflict", "geopolitics", "economy", "gdp", "inflation", "recession", "federal reserve", "interest rate", "trade", "tariff", "sanction", "diplomacy", "nato", "un", "climate", "environment", "energy", "oil price", "education", "science", "space", "nasa", "spacex", "discovery", "research", "law", "regulation", "policy", "social issue", "immigration", "protest"],
    "Real Estate":   ["real estate", "property", "housing", "home", "apartment", "rent", "mortgage", "landlord", "tenant", "construction", "commercial property", "reit", "land", "development", "architecture", "interior design", "home decor", "renovation", "smart home"],
}

_ALL_KEYS   = list(BROAD_NICHES.keys())
_ALL_SEEDS  = [" ".join(v) for v in BROAD_NICHES.values()]
_EMBED_CACHE = None

def _get_embeddings():
    global _EMBED_CACHE
    if _EMBED_CACHE is None:
        try:
            from sentence_transformers import SentenceTransformer
            import numpy as np
            model = SentenceTransformer("all-MiniLM-L6-v2")
            _EMBED_CACHE = (model, np.array(model.encode(_ALL_SEEDS, show_progress_bar=False)))
            logger.info("[niche_labeler] broad taxonomy embeddings cached")
        except Exception as e:
            logger.warning(f"[niche_labeler] embedding failed: {e}")
    return _EMBED_CACHE

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\x00-\x7F]+", " ", text)).strip()[:200]

def broad_classify(label: str) -> str:
    """Classify a clean Groq label into one of the 8 broad niches."""
    t = label.lower()
    # Keyword priority — most reliable
    for niche, keywords in BROAD_NICHES.items():
        if any(kw in t for kw in keywords):
            return niche
    # Embedding fallback
    cache = _get_embeddings()
    if cache:
        try:
            import numpy as np
            model, emb = cache
            vec   = model.encode([t], show_progress_bar=False)[0]
            norms = np.linalg.norm(emb, axis=1) * np.linalg.norm(vec) + 1e-9
            sims  = emb @ vec / norms
            best  = int(np.argmax(sims))
            if float(sims[best]) > 0.12:
                return _ALL_KEYS[best]
        except Exception:
            pass
    return "World & News"

def _parse_groq(content: str) -> list:
    content = re.sub(r"```(?:json)?", "", content).replace("```","").strip()
    s, e = content.find("["), content.rfind("]")
    if s == -1: raise ValueError("no array")
    return json.loads(content[s:e+1])

def groq_label_clusters(clusters: List[Dict]) -> List[Dict]:
    if not clusters or not GROQ_API_KEY:
        return clusters
    top   = clusters[:GROQ_LABEL_TOP]
    lines = []
    for i, c in enumerate(top):
        members = (c.get("members") or [c.get("label","")])[:3]
        sample  = " | ".join(_clean(m)[:55] for m in members)
        lines.append(f"{i+1}. {sample}")

    prompt = (
        "You are a trend analyst. Convert each raw signal group into a clean 2-5 word human-readable label.\n\n"
        "RULES:\n"
        "- Return ONLY a raw JSON array of strings\n"
        "- 2-5 words, Title Case\n"
        "- Crypto coins → '[Name] Crypto' e.g. 'Bitcoin Crypto'\n"
        "- Stock tickers → '[Company] Stock' e.g. 'Tesla Stock'\n"
        "- Products → '[Product] Market' e.g. 'Electric Kettle Market'\n"
        "- Games → '[Game] Gaming' e.g. 'GTA V Gaming'\n"
        "- Music → '[Artist/Genre] Music'\n"
        "- News → 3-4 word topic e.g. 'US Tariff Policy'\n"
        "- NEVER return ticker symbols, NEVER return 'Unknown'\n\n"
        f"Input:\n" + "\n".join(lines) + "\n\nRespond ONLY: [\"Label 1\", ...]"
    )
    try:
        resp = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": GROQ_MODEL, "messages": [{"role":"user","content":prompt}], "max_tokens":1500, "temperature":0.1},
            timeout=45,
        )
        resp.raise_for_status()
        labels = _parse_groq(resp.json()["choices"][0]["message"]["content"])
        ok = 0
        for i, lbl in enumerate(labels):
            if i < len(top) and isinstance(lbl, str) and 2 <= len(lbl.strip()) <= 80:
                top[i]["clean_label"] = lbl.strip(); ok += 1
        logger.info(f"[niche_labeler] Groq labeled {ok}/{len(top)}")
    except Exception as e:
        logger.error(f"[niche_labeler] Groq failed: {e}")
        for c in top:
            if "clean_label" not in c:
                c["clean_label"] = c.get("label","Unknown")[:50]
    for c in clusters[GROQ_LABEL_TOP:]:
        if "clean_label" not in c:
            c["clean_label"] = c.get("label","Unknown")[:50]
    return clusters

def label_clusters(clusters: List[Dict]) -> List[Dict]:
    clusters = groq_label_clusters(clusters)
    for c in clusters:
        c["taxonomy_label"] = broad_classify(c.get("clean_label",""))
    return clusters

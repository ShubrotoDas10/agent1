"""
run.py — Single command to start Agent 1.
Runs FastAPI + APScheduler (pipeline) on port 9100.
All logs stream to stdout.

Usage:
    python run.py
"""
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))

from loguru import logger
import uvicorn

# ── Logging config ────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{line}</cyan> — "
        "<level>{message}</level>"
    ),
    level="INFO",
    colorize=True,
    filter=lambda r: not (r["name"] == "uvicorn.access" and "GET /api/" in r["message"]),
)
logger.add(
    "agent1.log",
    rotation="50 MB",
    retention="14 days",
    level="DEBUG",
    format="{time} | {level} | {name}:{line} — {message}",
)

if __name__ == "__main__":
    logger.info("Starting Agent 1 — Global Signal Radar on port 9100")
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=9100,
        reload=True,
        reload_dirs=["api", "pipeline", "collectors", "db", "allocator", "discovery"],
        log_level="warning",
        access_log=False,
    )

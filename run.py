import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from loguru import logger
import uvicorn

logger.remove()
logger.add(sys.stdout, level="INFO", colorize=False)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 9100))
    logger.info(f"Starting Agent 1 — Global Signal Radar on port {port}")
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="warning",
        access_log=False,
    )

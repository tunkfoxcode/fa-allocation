"""
Local development runner for Finance Allocation Report API

Usage:
    python run_local.py
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import uvicorn
from app_config import get_settings

if __name__ == "__main__":
    settings = get_settings()
    
    print(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    print(f"API will be available at: http://{settings.API_HOST}:{settings.API_PORT}")
    print(f"Swagger UI: http://localhost:{settings.API_PORT}/docs")
    print(f"ReDoc: http://localhost:{settings.API_PORT}/redoc")
    print(f"Debug mode: {settings.DEBUG}")
    print("-" * 50)
    
    uvicorn.run(
        "api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="info"
    )

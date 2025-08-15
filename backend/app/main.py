from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.settings import settings
from app.api.v1.health import router as health_router
from app.api.v1.sites import router as sites_router

app = FastAPI(title="RAN Quality Evaluator API", debug=settings.API_DEBUG)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(health_router, prefix="/api")
app.include_router(sites_router, prefix="/api")


@app.get("/")
def root():
    return {"service": "ran-quality-evaluator-api", "status": "ok"}


# Entrypoint for uvicorn: uvicorn app.main:app --reload --port 8000

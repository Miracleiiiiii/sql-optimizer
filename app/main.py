from __future__ import annotations

from fastapi import FastAPI

from app.api.analysis import router as analysis_router


app = FastAPI(
    title="Spark AI Optimizer",
    version="0.1.0",
    description="Diagnose Spark on YARN applications and generate parameter tuning recommendations.",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(analysis_router)


from __future__ import annotations

from pydantic import BaseModel, Field


class AnalysisRequest(BaseModel):
    application_id: str = Field(alias="applicationId")
    force_refresh: bool = Field(default=False, alias="forceRefresh")


class AnalysisResponse(BaseModel):
    analysis_id: str = Field(alias="analysisId")
    application_id: str = Field(alias="applicationId")
    status: str


class FeedbackRequest(BaseModel):
    accepted: bool
    comment: str | None = None


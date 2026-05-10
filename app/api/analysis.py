from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.api.schemas import AnalysisRequest, AnalysisResponse, FeedbackRequest
from app.core.config import settings
from app.services.analysis_service import AnalysisService
from app.storage.sqlite import SQLiteStore


router = APIRouter(prefix="/api/v1", tags=["analysis"])
store = SQLiteStore(settings.analysis.sqlite_path)
service = AnalysisService(settings, store)


@router.post("/analysis", response_model=AnalysisResponse, response_model_by_alias=True)
def submit_analysis(request: AnalysisRequest, background_tasks: BackgroundTasks) -> AnalysisResponse:
    analysis_id = service.submit(request.application_id)
    background_tasks.add_task(service.run, analysis_id, request.application_id)
    return AnalysisResponse(analysisId=analysis_id, applicationId=request.application_id, status="running")


@router.get("/analysis/{analysis_id}")
def get_analysis(analysis_id: str) -> dict:
    item = service.get_analysis(analysis_id)
    if not item:
        raise HTTPException(status_code=404, detail="analysis not found")
    return {
        "analysisId": item["analysis_id"],
        "applicationId": item["application_id"],
        "status": item["status"],
        "reportId": item["report_id"],
        "errorMessage": item["error_message"],
    }


@router.get("/reports/{report_id}")
def get_report(report_id: str) -> dict:
    item = service.get_report(report_id)
    if not item:
        raise HTTPException(status_code=404, detail="report not found")
    return {
        "reportId": item["report_id"],
        "applicationId": item["application_id"],
        "report": item["report_json"],
        "createdAt": item["created_at"],
    }


@router.post("/recommendations/{recommendation_id}/feedback")
def submit_feedback(recommendation_id: str, request: FeedbackRequest) -> dict:
    return service.feedback(recommendation_id, request.accepted, request.comment)


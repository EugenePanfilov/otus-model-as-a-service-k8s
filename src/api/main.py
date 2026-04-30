import logging

from fastapi import FastAPI, HTTPException

from src.api.model_service import ModelService
from src.api.schemas import (
    HealthResponse,
    PredictRequest,
    PredictResponse,
    ReadinessResponse,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Fraud Detection API",
    description="REST API for fraud detection model inference",
    version="1.0.0",
)

model_service = ModelService()


@app.on_event("startup")
def startup_event() -> None:
    try:
        model_service.load_model()
        logger.info("Model loaded successfully from %s", model_service.model_uri)
    except Exception:
        logger.exception("Failed to load model")
        raise


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get("/ready", response_model=ReadinessResponse)
def ready() -> ReadinessResponse:
    if not model_service.is_ready():
        raise HTTPException(status_code=503, detail="Model is not ready")

    return ReadinessResponse(
        status="ready",
        model_uri=model_service.model_uri,
    )


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    if not request.records:
        raise HTTPException(status_code=400, detail="records must not be empty")

    try:
        predictions = model_service.predict(request.records)
    except Exception as exc:
        logger.exception("Prediction failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return PredictResponse(predictions=predictions)
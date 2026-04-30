from typing import List, Optional

from pydantic import BaseModel, Field


class TransactionFeatures(BaseModel):
    transaction_id: Optional[int] = Field(default=None)
    customer_id: int
    terminal_id: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int
    tx_hour: Optional[int] = Field(default=None)
    tx_day_of_week: Optional[int] = Field(default=None)
    tx_month: Optional[int] = Field(default=None)
    is_weekend: Optional[int] = Field(default=None)


class PredictRequest(BaseModel):
    records: List[TransactionFeatures]


class PredictionItem(BaseModel):
    transaction_id: Optional[int] = None
    fraud_probability: float
    fraud_prediction: int


class PredictResponse(BaseModel):
    predictions: List[PredictionItem]


class HealthResponse(BaseModel):
    status: str


class ReadinessResponse(BaseModel):
    status: str
    model_uri: Optional[str] = None
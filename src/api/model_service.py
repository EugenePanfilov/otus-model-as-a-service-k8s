import os
from typing import Any, List

import mlflow.pyfunc
import numpy as np
import pandas as pd

from src.api.schemas import TransactionFeatures, PredictionItem


DEFAULT_FEATURE_COLUMNS = [
    "customer_id",
    "terminal_id",
    "tx_amount",
    "tx_time_seconds",
    "tx_time_days",
    "tx_hour",
    "tx_day_of_week",
    "tx_month",
    "is_weekend",
]


class ModelService:
    def __init__(self) -> None:
        self.model_uri = os.getenv("MODEL_URI")
        self.threshold = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
        self.feature_columns = os.getenv(
            "FEATURE_COLUMNS",
            ",".join(DEFAULT_FEATURE_COLUMNS),
        ).split(",")

        self.model: Any = None

    def load_model(self) -> None:
        if not self.model_uri:
            raise RuntimeError(
                "MODEL_URI is not set. "
                "Set MODEL_URI, for example: models:/fraud_detection_model@champion"
            )

        self.model = mlflow.pyfunc.load_model(self.model_uri)

    def is_ready(self) -> bool:
        return self.model is not None

    def _to_dataframe(self, records: List[TransactionFeatures]) -> pd.DataFrame:
        data = [record.model_dump() for record in records]
        df = pd.DataFrame(data)

        for column in self.feature_columns:
            if column not in df.columns:
                df[column] = 0

        df[self.feature_columns] = df[self.feature_columns].fillna(0)

        return df

    @staticmethod
    def _extract_probability(raw_predictions: Any) -> np.ndarray:
        preds = np.asarray(raw_predictions)

        if preds.ndim == 1:
            return preds.astype(float)

        if preds.ndim == 2 and preds.shape[1] == 2:
            return preds[:, 1].astype(float)

        if preds.ndim == 2 and preds.shape[1] == 1:
            return preds[:, 0].astype(float)

        raise ValueError(f"Unsupported prediction shape: {preds.shape}")

    def predict(self, records: List[TransactionFeatures]) -> List[PredictionItem]:
        if self.model is None:
            raise RuntimeError("Model is not loaded")

        df = self._to_dataframe(records)
        model_input = df[self.feature_columns]

        raw_predictions = self.model.predict(model_input)
        probabilities = self._extract_probability(raw_predictions)

        result: List[PredictionItem] = []

        for record, prob in zip(records, probabilities):
            fraud_prediction = int(prob >= self.threshold)

            result.append(
                PredictionItem(
                    transaction_id=record.transaction_id,
                    fraud_probability=float(prob),
                    fraud_prediction=fraud_prediction,
                )
            )

        return result
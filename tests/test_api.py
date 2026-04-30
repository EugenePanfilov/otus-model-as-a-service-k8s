import numpy as np
from fastapi.testclient import TestClient

from src.api.main import app, model_service


class DummyModel:
    def predict(self, data):
        return np.array([0.1, 0.9])


def setup_module():
    model_service.model_uri = "dummy-model"
    model_service.threshold = 0.5
    model_service.model = DummyModel()


client = TestClient(app)


def test_health():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready():
    response = client.get("/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert response.json()["model_uri"] == "dummy-model"


def test_predict():
    payload = {
        "records": [
            {
                "transaction_id": 1,
                "customer_id": 101,
                "terminal_id": 1001,
                "tx_amount": 42.5,
                "tx_time_seconds": 3600,
                "tx_time_days": 1,
                "tx_hour": 1,
                "tx_day_of_week": 2,
                "tx_month": 8,
                "is_weekend": 0,
            },
            {
                "transaction_id": 2,
                "customer_id": 102,
                "terminal_id": 1002,
                "tx_amount": 1000.0,
                "tx_time_seconds": 7200,
                "tx_time_days": 1,
                "tx_hour": 2,
                "tx_day_of_week": 2,
                "tx_month": 8,
                "is_weekend": 0,
            },
        ]
    }

    response = client.post("/predict", json=payload)

    assert response.status_code == 200

    body = response.json()

    assert len(body["predictions"]) == 2

    assert body["predictions"][0]["transaction_id"] == 1
    assert body["predictions"][0]["fraud_probability"] == 0.1
    assert body["predictions"][0]["fraud_prediction"] == 0

    assert body["predictions"][1]["transaction_id"] == 2
    assert body["predictions"][1]["fraud_probability"] == 0.9
    assert body["predictions"][1]["fraud_prediction"] == 1


def test_predict_empty_records():
    response = client.post("/predict", json={"records": []})

    assert response.status_code == 400
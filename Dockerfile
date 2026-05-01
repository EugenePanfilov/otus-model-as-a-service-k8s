FROM python:3.8-slim-bullseye

WORKDIR /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ curl openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-api.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-api.txt

COPY src ./src

EXPOSE 8000

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
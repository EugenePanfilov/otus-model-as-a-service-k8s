# OTUS. Model as a Service в Kubernetes

Проект реализует end-to-end MLOps-пайплайн для обучения и развёртывания модели выявления фродовых транзакций в Yandex Cloud.

Основной сценарий работы:

1. Terraform разворачивает облачную инфраструктуру.
2. Airflow запускает обучающий DAG.
3. Dataproc/Spark выполняет подготовку данных и обучение модели.
4. MLflow Tracking Server сохраняет эксперименты, метрики и артефакты модели.
5. Лучшая модель регистрируется в MLflow Model Registry под именем `fraud_detection_model`.
6. Для production-инференса используется alias `champion`.
7. FastAPI-сервис в Kubernetes загружает модель из MLflow и отдаёт предсказания через REST API.

Текущая версия проекта использует синхронный REST-инференс. Kafka/streaming-контур был исключён из активной инфраструктуры, чтобы упростить и стабилизировать сдаваемую версию проекта. В дальнейшем Kafka может быть возвращена как отдельный асинхронный контур скоринга и буферизации нагрузки.

## Компоненты инфраструктуры

Инфраструктура описана в директории `/infra` и разворачивается через Terraform.

Основные компоненты:

- **Managed Airflow** — оркестрация пайплайна обучения
- **Managed Dataproc** — временный Spark-кластер для запуска PySpark jobs
- **MLflow Server** — отслеживание экспериментов, хранение моделей и Model Registry
- **Managed PostgreSQL** — backend store для MLflow
- **Yandex Object Storage** — хранение данных, исходного кода, окружения и MLflow artifacts
- **Managed Kubernetes** — развёртывание REST API для инференса
- **LoadBalancer** — публичный доступ к FastAPI-сервису
- **VPC network / subnet / NAT gateway / security groups** — сетевой слой проекта

MLflow Server разворачивается без публичного IP и доступен внутри VPC по внутреннему адресу. Это снижает зависимость от публичных IPv4-адресов и уменьшает поверхность атаки.

## Архитектура

```text
S3 Object Storage
  ├── data/input_data/
  ├── src/
  ├── dags/
  └── venvs/venv.tar.gz
        │
        ▼
Airflow DAG: training_pipeline_homework
        │
        ▼
Yandex Dataproc / Spark job
        │
        ▼
MLflow Tracking Server + PostgreSQL backend
        │
        ▼
MLflow Model Registry
fraud_detection_model@champion
        │
        ▼
Kubernetes Deployment: fraud-api
        │
        ▼
FastAPI REST API
/health
/ready
/predict
```

## Структура проекта

```text
.
├── dags/
│   ├── training_pipeline_homework.py
│   └── streaming_pipeline_homework.py
├── data/
│   └── input_data/
├── infra/
│   ├── main.tf
│   ├── variables.tf
│   ├── modules/
│   │   ├── airflow-cluster/
│   │   ├── iam/
│   │   ├── k8s/
│   │   ├── mlflow-server/
│   │   ├── network/
│   │   ├── postgres-cluster/
│   │   └── storage/
│   └── variables.json
├── k8s/
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── deployment.yaml
│   └── service.yaml
├── scripts/
│   ├── bootstrap_k8s.sh
│   └── create_venv_archive.sh
├── src/
│   ├── api/
│   ├── train_fraud_detection.py
│   └── common_fraud.py
├── Dockerfile
├── Makefile
├── requirements.txt
├── requirements-api.txt
└── README.md
```

`streaming_pipeline_homework.py` оставлен в репозитории как задел под будущий Kafka-based streaming scoring, но в текущей активной инфраструктуре Kafka отключена.

## Предварительные требования

Локально должны быть установлены:

- Terraform
- Yandex Cloud CLI
- Docker
- kubectl
- s3cmd
- Python virtual environment
- GitHub Personal Access Token с доступом к GHCR packages

## Развёртывание инфраструктуры

```bash
cd infra
terraform init
terraform apply
```

После успешного применения Terraform создаёт файл:

```text
infra/variables.json
```

Этот файл используется для загрузки переменных в Airflow.

Также обновляется `.env` в корне проекта.

## Загрузка ресурсов в S3

Из корня проекта:

```bash
source .env
source .venv/bin/activate

make deploy-full
```

Команда выполняет:

```text
создание архива Python-окружения
загрузку venvs/venv.tar.gz в S3
загрузку src/ в S3
загрузку dags/ в S3
загрузку входных данных в S3
```

Если после пересоздания инфраструктуры возникает ошибка S3 `SignatureDoesNotMatch`, нужно обновить локальный `~/.s3cfg` актуальными ключами из `.env`.

## Настройка Airflow

В Airflow UI необходимо импортировать переменные из файла:

```text
infra/variables.json
```

В текущей версии используется основной DAG:

```text
training_pipeline_homework
```

Он запускает обучение модели на Dataproc и логирует результат в MLflow.

## Проверка модели в MLflow

После успешного выполнения DAG модель должна быть зарегистрирована как:

```text
fraud_detection_model
```

Для production-инференса используется alias:

```text
champion
```

Если alias не назначен автоматически, его можно назначить через MLflow Client из pod-а внутри Kubernetes, так как MLflow доступен только внутри VPC.

## Деплой API в Kubernetes

Перед запуском bootstrap нужно задать GitHub token для скачивания Docker image из GHCR:

```bash
export GITHUB_TOKEN='your_github_token'
```

Запуск bootstrap:

```bash
./scripts/bootstrap_k8s.sh
```

Скрипт выполняет:

```text
получение kubeconfig
создание namespace fraud-detection
создание ConfigMap
создание fraud-api-secret
создание ghcr-pull-secret
применение Deployment и Service
ожидание rollout
smoke checks /health и /ready
```

## Проверка REST API

Получить внешний IP:

```bash
kubectl get svc -n fraud-detection
```

Пример текущего endpoint:

```text
http://81.26.185.115
```

Проверка health:

```bash
curl http://81.26.185.115/health
```

Ожидаемый ответ:

```json
{"status":"ok"}
```

Проверка readiness:

```bash
curl http://81.26.185.115/ready
```

Ожидаемый ответ:

```json
{"status":"ready","model_uri":"models:/fraud_detection_model@champion"}
```

Проверка prediction endpoint:

```bash
curl -X POST "http://81.26.185.115/predict" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "transaction_id": 1,
        "customer_id": 101,
        "terminal_id": 1001,
        "tx_amount": 150.5,
        "tx_time_seconds": 3600,
        "tx_time_days": 1,
        "tx_hour": 1,
        "tx_day_of_week": 2,
        "tx_month": 8,
        "is_weekend": 0
      }
    ]
  }'
```

Пример ответа:

```json
{
  "predictions": [
    {
      "transaction_id": 1,
      "fraud_probability": 1.0,
      "fraud_prediction": 1
    }
  ]
}
```

## GitHub Actions

CI/CD workflow выполняет:

```text
запуск тестов
сборку Docker image
push image в GHCR
деплой Kubernetes manifests
rollout fraud-api
```

После полного пересоздания инфраструктуры нужно обновить GitHub Secrets:

```text
YC_SERVICE_ACCOUNT_KEY_JSON
YC_K8S_CLUSTER_ID
```

Также необходимо создать Kubernetes RBAC binding для service account, от имени которого GitHub Actions применяет manifests:

```bash
kubectl delete clusterrolebinding yc-ci-cluster-admin --ignore-not-found

kubectl create clusterrolebinding yc-ci-cluster-admin \
  --clusterrole=cluster-admin \
  --user=<current_yandex_service_account_id>
```

## Текущее состояние и ограничения

Текущая версия проекта стабилизирована под дедлайн:

- Kafka убрана из активной инфраструктуры
- Kubernetes worker node разворачивается без публичного IP
- MLflow Server разворачивается без публичного IP
- MLflow настраивается через cloud-init без SSH provisioners
- REST API работает через Kubernetes LoadBalancer
- модель загружается из MLflow Model Registry по alias `champion`

## Future improvements

Планируемые улучшения:

- вернуть Kafka как отдельный асинхронный контур скоринга
- реализовать Kafka-backed request/reply inference
- заменить Spark MLflow flavor на лёгкую Python-модель для serving
- убрать PySpark и Java из API-контейнера
- добавить HPA для `fraud-api`
- автоматизировать обновление GitHub Secrets после пересоздания инфраструктуры
- автоматизировать Kubernetes RBAC для GitHub Actions
- добавить полноценный мониторинг качества модели и drift detection

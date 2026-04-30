"""
Общие функции для fraud-модели на PySpark:
- загрузка и подготовка данных
- обучение Spark Pipeline
- оценка моделей
- bootstrap + статистическое сравнение
- MLflow Model Registry helpers
"""

import time
import warnings
from typing import Dict, List, Optional, Tuple

import mlflow
import mlflow.spark
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql.window import Window
from scipy.stats import ttest_ind

warnings.simplefilter(action="ignore", category=(FutureWarning, UserWarning))


def setup_mlflow(
    tracking_uri: str = "http://localhost:5000",
    experiment_name: str = "fraud_detection",
) -> str:
    """
    Настройка MLflow.
    """
    mlflow.set_tracking_uri(tracking_uri)

    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            experiment_id = mlflow.create_experiment(experiment_name)
            print(f"Создан новый эксперимент: {experiment_name}")
        else:
            experiment_id = experiment.experiment_id
            print(f"Используется существующий эксперимент: {experiment_name}")

        mlflow.set_experiment(experiment_name)
        return experiment_id
    except Exception as e:
        print(f"Ошибка при настройке MLflow: {e}")
        mlflow.set_tracking_uri("file:./mlruns")
        mlflow.set_experiment(experiment_name)
        print("Используется локальное хранилище MLflow")
        experiment = mlflow.get_experiment_by_name(experiment_name)
        return experiment.experiment_id if experiment else ""


def create_spark_session(s3_config: Optional[Dict[str, str]] = None):
    """
    Создаёт SparkSession builder.
    """
    builder = SparkSession.builder.appName("FraudDetectionPipeline")

    if s3_config and all(
        k in s3_config for k in ["endpoint_url", "access_key", "secret_key"]
    ):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", s3_config["endpoint_url"])
            .config("spark.hadoop.fs.s3a.access.key", s3_config["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", s3_config["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        )

    return builder


def _stratified_train_test_split(
    df: DataFrame,
    label_col: str = "tx_fraud",
    train_fraction: float = 0.8,
    seed: int = 42,
) -> Tuple[DataFrame, DataFrame]:
    """
    Стратифицированное разбиение Spark DataFrame на train/test.
    """
    counts = {
        int(row[label_col]): int(row["count"])
        for row in df.groupBy(label_col).count().collect()
    }

    count_0 = counts.get(0, 0)
    count_1 = counts.get(1, 0)

    if count_0 < 2 or count_1 < 2:
        raise ValueError(
            f"Недостаточно объектов для split: {label_col}=0 -> {count_0}, {label_col}=1 -> {count_1}"
        )

    w_class = Window.partitionBy(label_col)
    w_order = Window.partitionBy(label_col).orderBy(F.rand(seed))

    df = (
        df
        .withColumn("_class_count", F.count("*").over(w_class))
        .withColumn("_rn", F.row_number().over(w_order))
        .withColumn(
            "_train_count",
            F.when(
                F.floor(F.col("_class_count") * F.lit(train_fraction)) < 1,
                F.lit(1),
            ).otherwise(F.floor(F.col("_class_count") * F.lit(train_fraction)))
        )
        .withColumn(
            "_train_count",
            F.when(
                F.col("_train_count") >= F.col("_class_count"),
                F.col("_class_count") - 1,
            ).otherwise(F.col("_train_count"))
        )
        .withColumn(
            "_split",
            F.when(F.col("_rn") <= F.col("_train_count"), F.lit("train")).otherwise(F.lit("test"))
        )
    )

    train_df = (
        df.filter(F.col("_split") == "train")
        .drop("_class_count", "_rn", "_train_count", "_split")
    )
    test_df = (
        df.filter(F.col("_split") == "test")
        .drop("_class_count", "_rn", "_train_count", "_split")
    )

    return train_df, test_df


def load_data(
    spark: SparkSession,
    input_path: str,
    input_format: str = "auto",
) -> Tuple[DataFrame, DataFrame]:
    """
    Загружает данные и делает внешний train/test split.
    """
    fmt = input_format.lower()
    if fmt == "auto":
        fmt = "parquet" if input_path.lower().endswith(".parquet") else "txt"

    if fmt == "parquet":
        df = spark.read.parquet(input_path)

    elif fmt == "txt":
        raw_schema = StructType([
            StructField("_c0", StringType(), True),
            StructField("_c1", StringType(), True),
            StructField("_c2", StringType(), True),
            StructField("_c3", StringType(), True),
            StructField("_c4", StringType(), True),
            StructField("_c5", StringType(), True),
            StructField("_c6", StringType(), True),
            StructField("_c7", StringType(), True),
            StructField("_c8", StringType(), True),
        ])

        df = spark.read.csv(
            input_path,
            header=False,
            inferSchema=False,
            sep=",",
            comment="#",
            schema=raw_schema,
        )

        df = df.toDF(
            "transaction_id",
            "tx_datetime",
            "customer_id",
            "terminal_id",
            "tx_amount",
            "tx_time_seconds",
            "tx_time_days",
            "tx_fraud",
            "tx_fraud_scenario",
        )

        for c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    else:
        raise ValueError(f"Unsupported input_format: {input_format}")

    required_cols = {
        "tx_datetime",
        "tx_amount",
        "tx_time_seconds",
        "tx_time_days",
        "tx_fraud",
    }
    missing_cols = sorted(required_cols - set(df.columns))
    if missing_cols:
        raise ValueError(f"В датасете отсутствуют обязательные колонки: {missing_cols}")

    df = (
        df
        .withColumn("tx_datetime", F.to_timestamp("tx_datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("tx_amount", F.col("tx_amount").cast("double"))
        .withColumn("tx_time_seconds", F.col("tx_time_seconds").cast("long"))
        .withColumn("tx_time_days", F.col("tx_time_days").cast("int"))
        .withColumn("tx_fraud", F.col("tx_fraud").cast("int"))
        .filter(F.col("tx_datetime").isNotNull())
        .filter(F.col("tx_amount").isNotNull())
        .filter(F.col("tx_time_seconds").isNotNull())
        .filter(F.col("tx_time_days").isNotNull())
        .filter(F.col("tx_fraud").isNotNull())
    )

    print("DEBUG: Распределение классов после очистки:")
    df.groupBy("tx_fraud").count().show()

    if df.count() == 0:
        raise ValueError("После очистки датасет пуст")

    train_df, test_df = _stratified_train_test_split(
        df=df,
        label_col="tx_fraud",
        train_fraction=0.8,
        seed=42,
    )

    print("DEBUG: train class distribution:")
    train_df.groupBy("tx_fraud").count().show()

    print("DEBUG: test class distribution:")
    test_df.groupBy("tx_fraud").count().show()

    return train_df, test_df


def prepare_features(
    train_df: DataFrame,
    test_df: DataFrame,
) -> Tuple[DataFrame, DataFrame, List[str]]:
    """
    Добавляет фичи и возвращает список feature columns.
    """
    def add_time_features(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("tx_hour", F.hour("tx_datetime"))
            .withColumn("tx_day_of_week", F.dayofweek("tx_datetime"))
            .withColumn("tx_month", F.month("tx_datetime"))
            .withColumn(
                "is_weekend",
                F.when(F.dayofweek("tx_datetime").isin([1, 7]), 1).otherwise(0),
            )
        )

    train_df = add_time_features(train_df)
    test_df = add_time_features(test_df)

    numeric_fill_cols = [
        "tx_amount",
        "tx_time_seconds",
        "tx_time_days",
        "tx_hour",
        "tx_day_of_week",
        "tx_month",
        "is_weekend",
    ]

    train_df = train_df.fillna(0, subset=numeric_fill_cols)
    test_df = test_df.fillna(0, subset=numeric_fill_cols)

    feature_cols = [
        "tx_amount",
        "tx_time_seconds",
        "tx_time_days",
        "tx_hour",
        "tx_day_of_week",
        "tx_month",
        "is_weekend",
    ]

    return train_df, test_df, feature_cols


def load_and_prepare_data(
    spark: SparkSession,
    input_path: str,
    input_format: str = "auto",
) -> Tuple[DataFrame, DataFrame, List[str]]:
    """
    Полный цикл: загрузка + подготовка + feature engineering.
    """
    train_df, test_df = load_data(
        spark=spark,
        input_path=input_path,
        input_format=input_format,
    )
    train_df, test_df, feature_cols = prepare_features(train_df, test_df)
    return train_df, test_df, feature_cols


def add_class_weights(
    train_df: DataFrame,
    target_col: str = "tx_fraud",
    weight_col: str = "class_weight",
) -> Tuple[DataFrame, Dict[str, float]]:
    """
    Добавляет class weights для дисбаланса.
    """
    class_counts = {
        int(row[target_col]): int(row["count"])
        for row in train_df.groupBy(target_col).count().collect()
    }

    count_0 = class_counts.get(0, 0)
    count_1 = class_counts.get(1, 0)

    if count_0 == 0 or count_1 == 0:
        raise ValueError(
            f"Некорректное распределение классов в train: {target_col}=0 -> {count_0}, {target_col}=1 -> {count_1}"
        )

    total = count_0 + count_1
    weight_0 = total / (2.0 * count_0)
    weight_1 = total / (2.0 * count_1)

    train_df = train_df.withColumn(
        weight_col,
        F.when(F.col(target_col) == 1, F.lit(weight_1)).otherwise(F.lit(weight_0))
    )

    return train_df, {
        "count_0": count_0,
        "count_1": count_1,
        "weight_0": weight_0,
        "weight_1": weight_1,
    }


def create_base_pipeline(
    feature_cols: List[str],
    num_trees: int = 100,
    max_depth: int = 10,
    min_instances_per_node: int = 1,
    feature_subset_strategy: str = "auto",
    seed: int = 42,
) -> Pipeline:
    """
    Создаёт базовый Spark pipeline.
    """
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

    classifier = RandomForestClassifier(
        labelCol="tx_fraud",
        featuresCol="features",
        weightCol="class_weight",
        numTrees=num_trees,
        maxDepth=max_depth,
        minInstancesPerNode=min_instances_per_node,
        featureSubsetStrategy=feature_subset_strategy,
        seed=seed,
    )

    return Pipeline(stages=[assembler, classifier])


def fit_model(
    train_df: DataFrame,
    feature_cols: List[str],
    params: Optional[Dict] = None,
):
    """
    Обучает Spark pipeline.
    """
    params = params or {}

    train_df_weighted, class_info = add_class_weights(train_df)

    pipeline = create_base_pipeline(
        feature_cols=feature_cols,
        num_trees=int(params.get("numTrees", 100)),
        max_depth=int(params.get("maxDepth", 10)),
        min_instances_per_node=int(params.get("minInstancesPerNode", 1)),
        feature_subset_strategy=params.get("featureSubsetStrategy", "auto"),
        seed=int(params.get("seed", 42)),
    )

    model = pipeline.fit(train_df_weighted)
    return model, class_info


def evaluate_model(model, test_df: DataFrame) -> Tuple[Dict[str, float], DataFrame]:
    """
    Оценивает модель на test_df.

    Returns
    -------
    metrics : dict
        Метрики качества модели
    pred_sdf : pyspark.sql.DataFrame
        Spark DataFrame с колонками row_id, y_true, y_pred
    """
    predictions = model.transform(test_df).cache()

    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="tx_fraud",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="tx_fraud",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR",
    )
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="tx_fraud",
        predictionCol="prediction",
        metricName="accuracy",
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="tx_fraud",
        predictionCol="prediction",
        metricName="f1",
    )

    auc = float(evaluator_auc.evaluate(predictions))
    pr_auc = float(evaluator_pr.evaluate(predictions))
    accuracy = float(evaluator_acc.evaluate(predictions))
    f1 = float(evaluator_f1.evaluate(predictions))

    tp = predictions.filter((F.col("tx_fraud") == 1) & (F.col("prediction") == 1)).count()
    fp = predictions.filter((F.col("tx_fraud") == 0) & (F.col("prediction") == 1)).count()
    tn = predictions.filter((F.col("tx_fraud") == 0) & (F.col("prediction") == 0)).count()
    fn = predictions.filter((F.col("tx_fraud") == 1) & (F.col("prediction") == 0)).count()

    fraud_precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    fraud_recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

    metrics = {
        "auc": auc,
        "pr_auc": pr_auc,
        "accuracy": accuracy,
        "f1": f1,
        "fraud_precision": float(fraud_precision),
        "fraud_recall": float(fraud_recall),
        "tp": float(tp),
        "fp": float(fp),
        "tn": float(tn),
        "fn": float(fn),
    }

    pred_sdf = (
        predictions
        .select(
            F.monotonically_increasing_id().alias("_mid"),
            F.col("tx_fraud").cast("int").alias("y_true"),
            F.col("prediction").cast("int").alias("y_pred"),
        )
    )

    w = Window.orderBy(F.col("_mid"))
    pred_sdf = (
        pred_sdf
        .withColumn("row_id", F.row_number().over(w) - 1)
        .drop("_mid")
        .select("row_id", "y_true", "y_pred")
        .cache()
    )

    predictions.unpersist()

    return metrics, pred_sdf


def bootstrap_metrics(
    pred_sdf: DataFrame,
    n_iterations: int = 100,
    random_state: int = 42,
) -> pd.DataFrame:
    """
    Spark-native bootstrap.
    На драйвер возвращается только маленькая таблица метрик по итерациям.
    """
    n = pred_sdf.count()
    if n == 0:
        raise ValueError("Пустой pred_sdf для bootstrap")

    spark = pred_sdf.sql_ctx.sparkSession
    base_pred = pred_sdf.select("row_id", "y_true", "y_pred").cache()

    sampled_idx = (
        spark.range(n_iterations * n)
        .select(
            (F.col("id") / F.lit(n)).cast("long").alias("iter_id"),
            F.floor(F.rand(random_state) * F.lit(n)).cast("long").alias("row_id"),
        )
    )

    sampled = sampled_idx.join(base_pred, on="row_id", how="inner")

    agg = (
        sampled
        .groupBy("iter_id")
        .agg(
            F.sum(
                F.when((F.col("y_true") == 1) & (F.col("y_pred") == 1), 1).otherwise(0)
            ).alias("tp"),
            F.sum(
                F.when((F.col("y_true") == 0) & (F.col("y_pred") == 1), 1).otherwise(0)
            ).alias("fp"),
            F.sum(
                F.when((F.col("y_true") == 0) & (F.col("y_pred") == 0), 1).otherwise(0)
            ).alias("tn"),
            F.sum(
                F.when((F.col("y_true") == 1) & (F.col("y_pred") == 0), 1).otherwise(0)
            ).alias("fn"),
        )
    )

    scores_sdf = (
        agg
        .select(
            "iter_id",
            F.when(
                (2 * F.col("tp") + F.col("fp") + F.col("fn")) > 0,
                (2.0 * F.col("tp")) / (2.0 * F.col("tp") + F.col("fp") + F.col("fn")),
            ).otherwise(F.lit(0.0)).alias("F1"),
            F.when(
                (F.col("tp") + F.col("fp")) > 0,
                F.col("tp") / (F.col("tp") + F.col("fp")),
            ).otherwise(F.lit(0.0)).alias("P"),
            F.when(
                (F.col("tp") + F.col("fn")) > 0,
                F.col("tp") / (F.col("tp") + F.col("fn")),
            ).otherwise(F.lit(0.0)).alias("R"),
        )
        .orderBy("iter_id")
    )

    scores_pdf = scores_sdf.toPandas()

    base_pred.unpersist()

    return scores_pdf


def statistical_comparison(
    scores_base: pd.DataFrame,
    scores_candidate: pd.DataFrame,
    alpha: float = 0.01,
    metrics: Optional[List[str]] = None,
) -> Dict[str, Dict[str, float]]:
    """
    Статистическое сравнение двух моделей через t-test.
    """
    if metrics is None:
        metrics = ["F1", "P"]

    results = {}

    for metric in metrics:
        t_stat, pvalue = ttest_ind(
            scores_base[metric],
            scores_candidate[metric],
            equal_var=False,
            nan_policy="omit",
        )

        pooled_std = np.sqrt(
            (scores_base[metric].var(ddof=1) + scores_candidate[metric].var(ddof=1)) / 2
        )

        if pooled_std == 0 or np.isnan(pooled_std):
            effect_size = 0.0
        else:
            effect_size = abs(
                scores_candidate[metric].mean() - scores_base[metric].mean()
            ) / pooled_std

        results[metric] = {
            "t_statistic": float(t_stat),
            "p_value": float(pvalue),
            "effect_size": float(effect_size),
            "is_significant": bool(pvalue < alpha),
            "base_mean": float(scores_base[metric].mean()),
            "candidate_mean": float(scores_candidate[metric].mean()),
            "improvement": float(
                scores_candidate[metric].mean() - scores_base[metric].mean()
            ),
        }

    return results


def save_model_to_mlflow(
    model,
    model_name: str,
    metrics: Dict[str, float],
    params: Optional[Dict] = None,
    register_model: bool = False,
    description: str = "",
) -> Dict:
    """
    Сохраняет Spark-модель в MLflow.
    """
    params = params or {}

    with mlflow.start_run() as run:
        for param_name, value in params.items():
            mlflow.log_param(param_name, value)

        for metric_name, value in metrics.items():
            if isinstance(value, (int, float, np.integer, np.floating)):
                mlflow.log_metric(metric_name, float(value))

        mlflow.spark.log_model(model, "model")

        run_id = run.info.run_id
        model_uri = f"runs:/{run_id}/model"

    model_info = {
        "run_id": run_id,
        "model_uri": model_uri,
    }

    if register_model:
        client = MlflowClient()

        try:
            client.get_registered_model(model_name)
        except Exception:
            client.create_registered_model(model_name)

        model_details = mlflow.register_model(model_uri, model_name)
        time.sleep(5)

        if description:
            client.update_model_version(
                name=model_name,
                version=model_details.version,
                description=description,
            )

        model_info["version"] = str(model_details.version)
        model_info["model_details"] = model_details

    return model_info


def load_model_from_mlflow(model_name: str, alias: str = "champion"):
    """
    Загружает Spark-модель из MLflow Model Registry по alias.
    """
    client = MlflowClient()

    try:
        try:
            model_uri = f"models:/{model_name}@{alias}"
            model = mlflow.spark.load_model(model_uri)
            print(f"Модель '{model_name}' с алиасом '{alias}' успешно загружена")
            return model
        except Exception:
            print(f"Поиск модели '{model_name}' по тегу alias='{alias}'")
            versions = list(client.search_model_versions(f"name='{model_name}'"))

            for version in versions:
                aliases = list(getattr(version, "aliases", []) or [])
                tags = getattr(version, "tags", {}) or {}

                if alias in aliases or tags.get("alias") == alias:
                    model_uri = f"models:/{model_name}/{version.version}"
                    model = mlflow.spark.load_model(model_uri)
                    print(
                        f"Модель '{model_name}' версии {version.version} "
                        f"с alias/tag '{alias}' загружена"
                    )
                    return model

            print(f"Модель '{model_name}' с алиасом '{alias}' не найдена")
            return None

    except Exception as e:
        print(f"Ошибка при загрузке модели: {e}")
        return None


def get_model_version_by_alias_safe(model_name: str, alias: str):
    """
    Возвращает model version по alias или None.
    """
    client = MlflowClient()

    try:
        if hasattr(client, "get_model_version_by_alias"):
            return client.get_model_version_by_alias(model_name, alias)
    except Exception:
        pass

    try:
        versions = list(client.search_model_versions(f"name='{model_name}'"))
        for version in versions:
            aliases = list(getattr(version, "aliases", []) or [])
            tags = getattr(version, "tags", {}) or {}
            if alias in aliases or tags.get("alias") == alias:
                return version
    except Exception:
        pass

    return None


def set_model_alias(
    model_name: str,
    version: str,
    alias: str,
    description: str = "",
) -> None:
    """
    Устанавливает alias для модели.
    """
    client = MlflowClient()

    try:
        if hasattr(client, "set_registered_model_alias"):
            client.set_registered_model_alias(
                name=model_name,
                alias=alias,
                version=version,
            )
        else:
            client.set_model_version_tag(model_name, version, "alias", alias)
    except Exception as e:
        print(f"Ошибка установки алиаса '{alias}': {e}")
        client.set_model_version_tag(model_name, version, "alias", alias)

    if description:
        client.update_model_version(
            name=model_name,
            version=version,
            description=description,
        )

    print(f"Модель {model_name} версии {version} получила алиас '{alias}'")


def register_model_as_candidate(
    run_id: str,
    model_name: str,
    description: str = "",
) -> str:
    """
    Регистрирует новую версию модели.
    Если champion уже существует -> новая версия получает alias challenger.
    Если champion отсутствует -> новая версия получает alias champion.
    """
    client = MlflowClient()

    try:
        client.get_registered_model(model_name)
    except Exception:
        client.create_registered_model(model_name)

    model_uri = f"runs:/{run_id}/model"
    model_details = mlflow.register_model(model_uri, model_name)
    new_version = str(model_details.version)

    time.sleep(5)

    champion_version = get_model_version_by_alias_safe(model_name, "champion")

    if champion_version is None:
        set_model_alias(
            model_name=model_name,
            version=new_version,
            alias="champion",
            description=description or "Первая зарегистрированная версия модели",
        )
        print(f"Первая версия {new_version} зарегистрирована как champion")
    else:
        set_model_alias(
            model_name=model_name,
            version=new_version,
            alias="challenger",
            description=description or "Новая модель-кандидат для A/B теста",
        )
        print(f"Новая версия {new_version} зарегистрирована как challenger")

    return new_version
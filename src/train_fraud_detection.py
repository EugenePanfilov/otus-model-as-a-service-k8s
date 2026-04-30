import os
import sys
import traceback
import argparse

import mlflow
import mlflow.spark
import optuna

from common_fraud import (
    setup_mlflow,
    create_spark_session,
    load_and_prepare_data,
    _stratified_train_test_split,
    fit_model,
    evaluate_model,
    register_model_as_candidate,
)

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass


def compute_fraud_f1(metrics: dict) -> float:
    """
    Считает F1 именно для fraud-класса на основе fraud_precision и fraud_recall.
    """
    precision = float(metrics.get("fraud_precision", 0.0))
    recall = float(metrics.get("fraud_recall", 0.0))

    if precision + recall == 0:
        return 0.0

    return 2.0 * precision * recall / (precision + recall)


def optimize_hyperparameters_optuna(
    train_df,
    feature_cols,
    n_trials=20,
    valid_fraction=0.2,
    random_state=42,
):
    """
    Подбор гиперпараметров через Optuna на validation split внутри train.
    Целевая метрика: fraud_f1.
    """
    print("Поиск оптимальных гиперпараметров через Optuna...")

    opt_train_df, valid_df = _stratified_train_test_split(
        train_df,
        label_col="tx_fraud",
        train_fraction=1.0 - valid_fraction,
        seed=random_state,
    )

    def objective(trial: optuna.Trial) -> float:
        params = {
            "numTrees": trial.suggest_int("numTrees", 50, 200, step=25),
            "maxDepth": trial.suggest_int("maxDepth", 4, 16),
            "minInstancesPerNode": trial.suggest_int("minInstancesPerNode", 1, 20),
            "featureSubsetStrategy": trial.suggest_categorical(
                "featureSubsetStrategy",
                ["auto", "sqrt", "log2", "onethird"],
            ),
            "seed": random_state,
        }

        model, _ = fit_model(
            train_df=opt_train_df,
            feature_cols=feature_cols,
            params=params,
        )

        metrics, pred_sdf = evaluate_model(model, valid_df)
        pred_sdf.unpersist()

        fraud_f1 = compute_fraud_f1(metrics)

        trial.set_user_attr("fraud_f1", fraud_f1)
        trial.set_user_attr("fraud_precision", metrics["fraud_precision"])
        trial.set_user_attr("fraud_recall", metrics["fraud_recall"])
        trial.set_user_attr("auc", metrics["auc"])
        trial.set_user_attr("pr_auc", metrics["pr_auc"])
        trial.set_user_attr("weighted_f1", metrics["f1"])

        return float(fraud_f1)

    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best_params = dict(study.best_trial.params)
    best_params["seed"] = random_state
    best_score = float(study.best_value)

    print(f"Лучший validation fraud_f1: {best_score:.4f}")
    print(f"Лучшие параметры: {best_params}")

    return best_params, best_score


def train_and_log_model(
    train_df,
    test_df,
    feature_cols,
    params,
    run_name,
    train_mode,
    validation_score=None,
):
    """
    Обучает модель, логирует её в MLflow и возвращает model, metrics.
    """
    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id

        model, class_info = fit_model(
            train_df=train_df,
            feature_cols=feature_cols,
            params=params,
        )

        for param_name, value in params.items():
            mlflow.log_param(param_name, value)

        mlflow.log_param("target_col", "tx_fraud")
        mlflow.log_param("feature_cols", ",".join(feature_cols))
        mlflow.log_param("weight_col", "class_weight")
        mlflow.log_param("model_type", "RandomForestClassifier")
        mlflow.log_param("train_mode", train_mode)

        if validation_score is not None:
            mlflow.log_metric("best_validation_fraud_f1", float(validation_score))

        mlflow.log_metric("train_class_0_count", class_info["count_0"])
        mlflow.log_metric("train_class_1_count", class_info["count_1"])
        mlflow.log_metric("train_class_0_weight", class_info["weight_0"])
        mlflow.log_metric("train_class_1_weight", class_info["weight_1"])

        metrics, pred_sdf = evaluate_model(model, test_df)
        pred_sdf.unpersist()

        metrics["fraud_f1"] = compute_fraud_f1(metrics)

        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, float(value))

        mlflow.spark.log_model(model, "model")

        metrics["run_id"] = run_id
        return model, metrics


def save_model(model, output_path):
    model.write().overwrite().save(output_path)
    print(f"Model saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Fraud Detection Model Training")
    parser.add_argument("--input", required=True, help="Input data path")
    parser.add_argument("--output", required=True, help="Output model path")
    parser.add_argument("--input-format", default="auto", choices=["auto", "txt", "parquet"])
    parser.add_argument("--tracking-uri")
    parser.add_argument("--experiment-name", default="fraud_detection")
    parser.add_argument("--model-name", default=None, help="Имя модели в MLflow Registry")
    parser.add_argument("--auto-register", action="store_true")
    parser.add_argument("--run-name", default=None)
    parser.add_argument("--train-mode", default="baseline", choices=["baseline", "tuned"])
    parser.add_argument("--n-trials", type=int, default=20)
    parser.add_argument("--s3-endpoint-url")
    parser.add_argument("--s3-access-key")
    parser.add_argument("--s3-secret-key")

    os.environ["GIT_PYTHON_REFRESH"] = "quiet"
    args = parser.parse_args()

    s3_config = None
    if args.s3_endpoint_url and args.s3_access_key and args.s3_secret_key:
        s3_config = {
            "endpoint_url": args.s3_endpoint_url,
            "access_key": args.s3_access_key,
            "secret_key": args.s3_secret_key,
        }
        os.environ["AWS_ACCESS_KEY_ID"] = args.s3_access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = args.s3_secret_key
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = args.s3_endpoint_url

    tracking_uri = args.tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    setup_mlflow(tracking_uri=tracking_uri, experiment_name=args.experiment_name)

    spark = None
    try:
        spark = create_spark_session(s3_config).getOrCreate()

        train_df, test_df, feature_cols = load_and_prepare_data(
            spark=spark,
            input_path=args.input,
            input_format=args.input_format,
        )

        model_name = args.model_name or f"{args.experiment_name}_model"

        if args.train_mode == "baseline":
            params = {
                "numTrees": 100,
                "maxDepth": 10,
                "minInstancesPerNode": 1,
                "featureSubsetStrategy": "auto",
                "seed": 42,
            }
            best_score = None
        else:
            params, best_score = optimize_hyperparameters_optuna(
                train_df=train_df,
                feature_cols=feature_cols,
                n_trials=args.n_trials,
            )

        run_name = args.run_name or (
            f"fraud_detection_{args.train_mode}_{os.path.basename(args.input)}"
        )

        model, metrics = train_and_log_model(
            train_df=train_df,
            test_df=test_df,
            feature_cols=feature_cols,
            params=params,
            run_name=run_name,
            train_mode=args.train_mode,
            validation_score=best_score,
        )

        save_model(model, args.output)

        if args.auto_register:
            description = (
                f"Train mode: {args.train_mode}. "
                f"Test fraud_f1={metrics['fraud_f1']:.4f}, "
                f"fraud_precision={metrics['fraud_precision']:.4f}, "
                f"fraud_recall={metrics['fraud_recall']:.4f}, "
                f"AUC={metrics['auc']:.4f}, PR_AUC={metrics['pr_auc']:.4f}. "
                f"Params: {params}"
            )
            version = register_model_as_candidate(
                run_id=metrics["run_id"],
                model_name=model_name,
                description=description,
            )
            print(f"Registered model version: {version}")

        print("Training completed successfully!")

    except Exception as e:
        print(f"ERROR: Ошибка во время обучения: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
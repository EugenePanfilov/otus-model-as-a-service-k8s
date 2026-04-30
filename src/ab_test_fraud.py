"""
A/B тестирование fraud-моделей:
- загружает champion и challenger из MLflow Registry
- сравнивает их на одном и том же test set
- при необходимости промоутит challenger -> champion
"""

import argparse
import os
import sys
import traceback

from common_fraud import (
    setup_mlflow,
    create_spark_session,
    load_and_prepare_data,
    evaluate_model,
    bootstrap_metrics,
    statistical_comparison,
    load_model_from_mlflow,
    set_model_alias,
    get_model_version_by_alias_safe,
)

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass


def load_two_models_for_ab(model_name):
    champion_model = load_model_from_mlflow(model_name, alias="champion")
    challenger_model = load_model_from_mlflow(model_name, alias="challenger")
    return champion_model, challenger_model


def ab_test_models(
    production_model,
    candidate_model,
    test_df,
    bootstrap_iterations=100,
    alpha=0.01,
):
    """
    A/B тестирование двух моделей.
    """
    print("\nA/B ТЕСТИРОВАНИЕ МОДЕЛЕЙ")
    print("=" * 50)

    print("Оценка производственной модели...")
    prod_metrics, prod_pred_sdf = evaluate_model(production_model, test_df)

    print("Метрики производственной модели:")
    for metric_name, value in prod_metrics.items():
        print(f"  {metric_name}: {value:.4f}")

    print("\nОценка модели-кандидата...")
    cand_metrics, cand_pred_sdf = evaluate_model(candidate_model, test_df)

    print("Метрики модели-кандидата:")
    for metric_name, value in cand_metrics.items():
        print(f"  {metric_name}: {value:.4f}")

    print(f"\nBootstrap анализ ({bootstrap_iterations} итераций)...")

    print("Bootstrap для производственной модели...")
    prod_bootstrap = bootstrap_metrics(prod_pred_sdf, bootstrap_iterations)

    print("Bootstrap для модели-кандидата...")
    cand_bootstrap = bootstrap_metrics(cand_pred_sdf, bootstrap_iterations)

    prod_pred_sdf.unpersist()
    cand_pred_sdf.unpersist()

    print(f"\nСтатистическое сравнение (α = {alpha})...")
    comparison_results = statistical_comparison(
        prod_bootstrap,
        cand_bootstrap,
        alpha=alpha,
        metrics=["F1", "P"],
    )

    print("\nРезультаты t-теста:")
    for metric, results in comparison_results.items():
        print(f"\n{metric}-score:")
        print(f"  Production: {results['base_mean']:.4f}")
        print(f"  Candidate:  {results['candidate_mean']:.4f}")
        print(f"  Улучшение:  {results['improvement']:+.4f}")
        print(f"  p-value:    {results['p_value']:.6f}")
        print(f"  Cohen's d:  {results['effect_size']:.4f}")

        if results["is_significant"]:
            if results["improvement"] > 0:
                print(f"  Статистически значимое улучшение при α={alpha}")
            elif results["improvement"] < 0:
                print(f"  Статистически значимое ухудшение при α={alpha}")
            else:
                print(f"  Статистически значимое различие при α={alpha}")
        else:
            print(f"  Статистически незначимое различие при α={alpha}")

    f1_significant = comparison_results["F1"]["is_significant"]
    f1_improvement = comparison_results["F1"]["improvement"] > 0
    should_deploy = f1_significant and f1_improvement

    print(f"\n{'=' * 50}")
    print("ИТОГОВОЕ РЕШЕНИЕ:")
    if should_deploy:
        print("РАЗВЕРНУТЬ новую модель в Production")
        print("   Модель-кандидат показала статистически значимое улучшение")
    else:
        print("ОСТАВИТЬ текущую модель в Production")
        if not f1_improvement:
            print("   Модель-кандидат не показала улучшения")
        else:
            print("   Улучшение статистически незначимо")
    print(f"{'=' * 50}")

    return {
        "should_deploy": should_deploy,
        "production_metrics": prod_metrics,
        "candidate_metrics": cand_metrics,
        "comparison_results": comparison_results,
        "production_bootstrap": prod_bootstrap,
        "candidate_bootstrap": cand_bootstrap,
    }


def run_ab_test(
    spark,
    input_path,
    input_format="auto",
    experiment_name="fraud_detection",
    model_name=None,
    bootstrap_iterations=100,
    alpha=0.01,
    auto_deploy=False,
):
    """
    Выполняет полный цикл A/B тестирования:
    - загружает данные и готовит test set
    - загружает champion и challenger из Registry
    - сравнивает их
    - при необходимости делает promotion challenger -> champion
    """
    print("=" * 60)
    print("A/B ТЕСТИРОВАНИЕ FRAUD-МОДЕЛЕЙ")
    print("=" * 60)

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    setup_mlflow(
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
    )

    _, test_df, _ = load_and_prepare_data(
        spark=spark,
        input_path=input_path,
        input_format=input_format,
    )

    model_name = model_name or f"{experiment_name}_model"

    print(f"Загрузка моделей champion и challenger из '{model_name}'...")
    production_model, candidate_model = load_two_models_for_ab(model_name)

    if production_model is None:
        print("Модель champion не найдена. A/B тест пропущен.")
        return None

    if candidate_model is None:
        print("Модель challenger не найдена. Сравнивать не с чем. A/B тест пропущен.")
        return None

    ab_results = ab_test_models(
        production_model=production_model,
        candidate_model=candidate_model,
        test_df=test_df,
        bootstrap_iterations=bootstrap_iterations,
        alpha=alpha,
    )

    if ab_results["should_deploy"] and auto_deploy:
        challenger_version = get_model_version_by_alias_safe(model_name, "challenger")

        if challenger_version is None:
            print("Не удалось определить версию challenger для promotion")
        else:
            set_model_alias(
                model_name=model_name,
                version=str(challenger_version.version),
                alias="champion",
                description=(
                    "Автоматически развернута после A/B теста. "
                    f"F1 improvement: {ab_results['comparison_results']['F1']['improvement']:+.4f}"
                ),
            )
            print("challenger promoted to champion")
    else: 
        print(f"DEBUG should_deploy={ab_results['should_deploy']}, auto_deploy={auto_deploy}")

    print("\n" + "=" * 60)
    print("A/B ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 60)

    return ab_results


def main():
    parser = argparse.ArgumentParser(
        description="A/B тестирование fraud-моделей на PySpark"
    )
    parser.add_argument("--input", required=True, help="Input data path")
    parser.add_argument(
        "--input-format",
        default="auto",
        choices=["auto", "txt", "parquet"],
    )
    parser.add_argument("--tracking-uri", default=None)
    parser.add_argument("--experiment-name", default="fraud_detection")
    parser.add_argument("--model-name", default=None, help="Имя модели в MLflow Registry")
    parser.add_argument(
        "--bootstrap-iterations",
        type=int,
        default=100,
        help="Количество итераций bootstrap",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=0.01,
        help="Уровень значимости для статистических тестов",
    )
    parser.add_argument(
        "--auto-deploy",
        action="store_true",
        help="Автоматически разворачивать лучшую модель",
    )
    parser.add_argument("--s3-endpoint-url")
    parser.add_argument("--s3-access-key")
    parser.add_argument("--s3-secret-key")

    args = parser.parse_args()
    os.environ["GIT_PYTHON_REFRESH"] = "quiet"

    if args.tracking_uri:
        os.environ["MLFLOW_TRACKING_URI"] = args.tracking_uri

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

    spark = None
    try:
        spark = create_spark_session(s3_config).getOrCreate()

        run_ab_test(
            spark=spark,
            input_path=args.input,
            input_format=args.input_format,
            experiment_name=args.experiment_name,
            model_name=args.model_name,
            bootstrap_iterations=args.bootstrap_iterations,
            alpha=args.alpha,
            auto_deploy=args.auto_deploy,
        )

    except Exception as e:
        print(f"ERROR: Ошибка во время A/B теста: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
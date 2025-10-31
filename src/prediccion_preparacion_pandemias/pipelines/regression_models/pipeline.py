"""
Pipeline de regresión para predecir días hasta 70% cobertura vacunal.
Implementa 7 modelos con GridSearchCV y CrossValidation (k=5).
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import train_all_regression_models


def create_pipeline(**kwargs) -> Pipeline:
    """
    Crea el pipeline de modelos de regresión.
    
    Returns:
        Pipeline de Kedro para entrenamiento de modelos de regresión
    """
    return pipeline(
        [
            node(
                func=train_all_regression_models,
                inputs={
                    "df": "regression_dataset",  # Dataset preparado de la fase anterior
                    "target_column": "params:regression.target_column",
                    "cv_folds": "params:regression.cv_folds"
                },
                outputs=["regression_comparison_table", "regression_models_metrics"],
                name="train_all_regression_models_node",
                tags=["regression", "modeling", "gridsearch"]
            )
        ],
        namespace="regression_models",
        inputs={"regression_dataset"},
        outputs={"regression_comparison_table", "regression_models_metrics"}
    )
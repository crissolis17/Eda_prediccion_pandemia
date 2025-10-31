"""
Pipeline de modelos de clasificación.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    prepare_classification_data,
    scale_classification_features,
    train_all_classification_models,
    create_classification_comparison_table
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Crea el pipeline de modelos de clasificafación.
    
    Returns:
        Pipeline de Kedro
    """
    return pipeline(
        [
            node(
                func=prepare_classification_data,
                inputs=[
                    "classification_dataset",
                    "params:classification_models.classification.target_column",
                    "params:classification_models.classification.test_size",
                    "params:classification_models.classification.random_state"
                ],
                outputs=["X_train_clf", "X_test_clf", "y_train_clf", "y_test_clf"],
                name="prepare_classification_data_node",
            ),
            node(
                func=scale_classification_features,
                inputs=["X_train_clf", "X_test_clf"],
                outputs=["X_train_scaled_clf", "X_test_scaled_clf", "classification_scaler"],
                name="scale_classification_features_node",
            ),
            node(
                func=train_all_classification_models,
                inputs=[
                    "X_train_scaled_clf",
                    "y_train_clf",
                    "X_test_scaled_clf",
                    "y_test_clf",
                    "params:classification_models.classification.cv_folds",
                    "params:classification_models.classification.random_state"
                ],
                outputs=["classification_models_metrics", "best_classification_model"],
                name="train_all_classification_models_node",
            ),
            node(
                func=create_classification_comparison_table,
                inputs="classification_models_metrics",
                outputs="classification_comparison_table",
                name="create_classification_comparison_table_node",
            ),
        ],
        # namespace="classification",
        # tags=["classification", "modeling"]
    )
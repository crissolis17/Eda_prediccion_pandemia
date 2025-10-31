"""
Pipeline de limpieza de datos
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    select_important_columns,
    impute_missing_values,
    create_features,
    integrate_datasets,
    create_ml_datasets,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=select_important_columns,
                inputs=["covid_compact_validated", "vaccination_global_validated"],
                outputs=["covid_selected", "vacc_selected"],
                name="select_columns",
                tags=["cleaning"],
            ),
            node(
                func=impute_missing_values,
                inputs=["covid_selected", "vacc_selected"],
                outputs=["covid_imputed", "vacc_imputed"],
                name="impute_missing",
                tags=["cleaning"],
            ),
            node(
                func=create_features,
                inputs=["covid_imputed", "vacc_imputed"],
                outputs=["covid_features", "vacc_features"],
                name="create_features",
                tags=["feature_engineering"],
            ),
            node(
                func=integrate_datasets,
                inputs=["covid_features", "vacc_features"],
                outputs="integrated_data",
                name="integrate_datasets",
                tags=["integration"],
            ),
            node(
                func=create_ml_datasets,
                inputs="integrated_data",
                outputs=["regression_data", "classification_data"],
                name="create_ml_datasets",
                tags=["ml_prep"],
            ),
        ]
    )

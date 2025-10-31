"""Project pipelines."""
from typing import Dict
from kedro.pipeline import Pipeline
from prediccion_preparacion_pandemias.pipelines import (
    data_engineering,
    data_cleaning,
    regression_models,
    classification_models,  # ✅ ACTIVADO
)


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.
    
    Returns:
        Dictionary of pipelines by name
    """
    
    # Crear pipelines individuales
    de_pipeline = data_engineering.create_pipeline()
    dc_pipeline = data_cleaning.create_pipeline()
    reg_pipeline = regression_models.create_pipeline()
    clf_pipeline = classification_models.create_pipeline()  # ✅ AÑADIDO
    
    return {
        "__default__": (
            de_pipeline 
            + dc_pipeline 
            + reg_pipeline 
            + clf_pipeline  # ✅ AÑADIDO
        ),
        "de": de_pipeline,
        "dc": dc_pipeline,
        "regression": reg_pipeline,
        "classification": clf_pipeline,  # ✅ AÑADIDO
    }
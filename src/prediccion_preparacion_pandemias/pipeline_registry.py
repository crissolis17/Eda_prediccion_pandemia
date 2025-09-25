"""
Pipeline Registry
=================

Este archivo es como el "índice" de todos los pipelines del proyecto.
Kedro mira aquí para saber qué pipelines existen y cómo ejecutarlos.

Es como el menú de un restaurante: lista todos los "platos" (pipelines)
disponibles y cómo prepararlos.
"""

"""Project pipelines."""
from typing import Dict
from kedro.pipeline import Pipeline
from prediccion_preparacion_pandemias.pipelines import data_engineering, data_cleaning


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines."""

    data_engineering_pipeline = data_engineering.create_pipeline()
    data_cleaning_pipeline = data_cleaning.create_pipeline()

    return {
        "__default__": data_engineering_pipeline + data_cleaning_pipeline,
        "data_engineering": data_engineering_pipeline,
        "data_cleaning": data_cleaning_pipeline,
    }


"""
NOTAS IMPORTANTES:

1. "__default__": Es el pipeline que se ejecuta cuando solo escribes 'kedro run'
   
2. Nombres personalizados: Puedes tener múltiples pipelines y ejecutar uno específico:
   kedro run --pipeline=data_engineering
   
3. Combinar pipelines: Puedes combinar varios pipelines:
   combined_pipeline = pipeline1 + pipeline2 + pipeline3
   
4. Orden de ejecución: Kedro determina automáticamente el orden basándose en
   las dependencias entre nodos (inputs/outputs)
"""

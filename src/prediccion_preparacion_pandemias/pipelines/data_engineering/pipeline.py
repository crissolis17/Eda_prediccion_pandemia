"""
Pipeline de Data Engineering
=============================

Un pipeline en Kedro es como un diagrama de flujo: conecta nodos (funciones)
definiendo qué datos entran y salen de cada uno. Kedro se encarga automáticamente
de ejecutarlos en el orden correcto basándose en las dependencias.

Piensa en esto como armar un Lego: cada pieza (nodo) tiene conectores específicos
(inputs/outputs), y Kedro figura out cómo encajarlos todos.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    load_and_validate_covid,
    load_and_validate_vaccination,
    generate_data_quality_report,
)


def create_pipeline(**kwargs) -> Pipeline:
    """
    Crea el pipeline de validación de datos.

    Este es nuestro primer pipeline simple. Hace tres cosas:
    1. Valida datos de COVID
    2. Valida datos de Vacunación
    3. Genera un reporte de calidad

    Returns:
        Pipeline: Pipeline de Kedro listo para ejecutar
    """
    return pipeline(
        [
            # ========================================
            # NODO 1: Validar COVID-19
            # ========================================
            # Este nodo:
            # - Lee "covid_compact_raw" del catálogo
            # - Ejecuta la función load_and_validate_covid()
            # - Guarda el resultado como "covid_compact_validated"
            node(
                func=load_and_validate_covid,
                inputs="covid_compact_raw",  # Del catálogo
                outputs="covid_compact_validated",  # Al catálogo
                name="validate_covid_data",  # Nombre descriptivo del nodo
                tags=["validation", "covid"],  # Tags para filtrar ejecución
            ),
            # ========================================
            # NODO 2: Validar Vacunación
            # ========================================
            # Similar al nodo anterior pero para datos de vacunación
            node(
                func=load_and_validate_vaccination,
                inputs="vaccination_global_raw",
                outputs="vaccination_global_validated",
                name="validate_vaccination_data",
                tags=["validation", "vaccination"],
            ),
            # ========================================
            # NODO 3: Generar Reporte de Calidad
            # ========================================
            # Este nodo toma MÚLTIPLES inputs (los outputs de los nodos anteriores)
            # y genera un reporte consolidado
            node(
                func=generate_data_quality_report,
                inputs=[
                    "covid_compact_validated",  # Output del Nodo 1
                    "vaccination_global_validated",  # Output del Nodo 2
                ],
                outputs="eda_summary_stats",  # Reporte final
                name="generate_quality_report",
                tags=["reporting", "quality"],
            ),
        ]
    )


# NOTA IMPORTANTE: Kedro espera que el pipeline se llame "create_pipeline"
# No cambies este nombre o Kedro no encontrará tu pipeline

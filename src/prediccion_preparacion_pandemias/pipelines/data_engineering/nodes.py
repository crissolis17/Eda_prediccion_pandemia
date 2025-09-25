"""
Nodos del Pipeline de Data Engineering
=======================================

Un "nodo" en Kedro es una función que toma datos de entrada y produce datos de salida.
Piensa en cada nodo como un paso en una receta: tomas ingredientes, haces algo con ellos,
y produces un resultado.

Por ahora, crearemos nodos muy simples solo para validar que todo funciona.
"""

import pandas as pd
import logging
from typing import Dict

# Configurar logging para ver mensajes informativos
logger = logging.getLogger(__name__)


def load_and_validate_covid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida y limpia el dataset de COVID-19.

    Este es nuestro primer nodo. Toma el DataFrame crudo y hace validaciones básicas.

    Args:
        df (pd.DataFrame): Dataset COVID-19 crudo del catálogo

    Returns:
        pd.DataFrame: Dataset validado

    Raises:
        ValueError: Si el dataset no cumple requisitos mínimos
    """
    logger.info(f"🔍 Validando COVID data: {df.shape}")

    # 1. Verificar que no esté vacío
    if df.empty:
        raise ValueError("❌ El dataset COVID está vacío!")

    # 2. Verificar columnas críticas
    required_cols = ["country", "date", "total_cases"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"❌ Columnas faltantes: {missing_cols}")

    # 3. Eliminar duplicados (si los hay)
    original_rows = len(df)
    df = df.drop_duplicates(subset=["country", "date"], keep="last")
    removed_rows = original_rows - len(df)

    if removed_rows > 0:
        logger.warning(f"⚠️  Eliminados {removed_rows} duplicados")

    # 4. Ordenar por país y fecha
    df = df.sort_values(["country", "date"]).reset_index(drop=True)

    logger.info(f"✅ COVID data validada: {df.shape}")
    logger.info(f"   Países: {df['country'].nunique()}")
    logger.info(f"   Rango de fechas: {df['date'].min()} a {df['date'].max()}")

    return df


def load_and_validate_vaccination(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida y limpia el dataset de vacunación global.

    Similar al nodo anterior, pero enfocado en datos de vacunación.

    Args:
        df (pd.DataFrame): Dataset de vacunación crudo

    Returns:
        pd.DataFrame: Dataset validado
    """
    logger.info(f"🔍 Validando Vaccination data: {df.shape}")

    # Verificaciones básicas
    if df.empty:
        raise ValueError("❌ El dataset de Vacunación está vacío!")

    required_cols = ["country", "date", "total_vaccinations"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"❌ Columnas faltantes: {missing_cols}")

    # Eliminar duplicados
    original_rows = len(df)
    df = df.drop_duplicates(subset=["country", "date"], keep="last")
    removed_rows = original_rows - len(df)

    if removed_rows > 0:
        logger.warning(f"⚠️  Eliminados {removed_rows} duplicados")

    # Ordenar
    df = df.sort_values(["country", "date"]).reset_index(drop=True)

    logger.info(f"✅ Vaccination data validada: {df.shape}")
    logger.info(f"   Países: {df['country'].nunique()}")

    return df


def generate_data_quality_report(
    covid_df: pd.DataFrame, vacc_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Genera un reporte de calidad de datos.

    Este nodo toma múltiples datasets y produce un resumen de su calidad.
    Es útil para documentar el estado de los datos.

    Args:
        covid_df (pd.DataFrame): Dataset COVID validado
        vacc_df (pd.DataFrame): Dataset Vacunación validado

    Returns:
        pd.DataFrame: Reporte de calidad
    """
    logger.info("📊 Generando reporte de calidad...")

    report = []

    # Reporte COVID
    covid_countries = covid_df["country"].nunique()
    covid_date_range = f"{covid_df['date'].min()} a {covid_df['date'].max()}"
    covid_missing_pct = covid_df.isnull().sum().sum() / covid_df.size * 100

    report.append(
        {
            "dataset": "COVID-19",
            "n_rows": len(covid_df),
            "n_columns": len(covid_df.columns),
            "n_countries": covid_countries,
            "date_range": covid_date_range,
            "missing_data_pct": round(covid_missing_pct, 2),
        }
    )

    # Reporte Vacunación
    vacc_countries = vacc_df["country"].nunique()
    vacc_date_range = f"{vacc_df['date'].min()} a {vacc_df['date'].max()}"
    vacc_missing_pct = vacc_df.isnull().sum().sum() / vacc_df.size * 100

    report.append(
        {
            "dataset": "Vacunación",
            "n_rows": len(vacc_df),
            "n_columns": len(vacc_df.columns),
            "n_countries": vacc_countries,
            "date_range": vacc_date_range,
            "missing_data_pct": round(vacc_missing_pct, 2),
        }
    )

    report_df = pd.DataFrame(report)

    logger.info("✅ Reporte de calidad generado")
    print("\n📊 REPORTE DE CALIDAD DE DATOS:")
    print("=" * 70)
    print(report_df.to_string(index=False))
    print("=" * 70 + "\n")

    return report_df

"""
Nodos para limpieza y preparación de datos
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def select_important_columns(covid_df: pd.DataFrame, vacc_df: pd.DataFrame) -> tuple:
    """Selecciona columnas importantes y descarta las demás"""

    covid_cols = [
        "country",
        "date",
        "total_cases",
        "new_cases",
        "total_deaths",
        "new_deaths",
        "total_cases_per_million",
        "total_deaths_per_million",
        "population",
        "population_density",
        "median_age",
        "gdp_per_capita",
        "hospital_beds_per_thousand",
        "life_expectancy",
        "human_development_index",
    ]

    vacc_cols = [
        "country",
        "date",
        "total_vaccinations",
        "people_vaccinated",
        "people_fully_vaccinated",
        "total_vaccinations_per_hundred",
        "people_vaccinated_per_hundred",
        "people_fully_vaccinated_per_hundred",
    ]

    # Filtrar solo columnas que existen
    covid_available = [col for col in covid_cols if col in covid_df.columns]
    vacc_available = [col for col in vacc_cols if col in vacc_df.columns]

    covid_clean = covid_df[covid_available].copy()
    vacc_clean = vacc_df[vacc_available].copy()

    logger.info(f"COVID: {covid_df.shape} -> {covid_clean.shape}")
    logger.info(f"Vacunación: {vacc_df.shape} -> {vacc_clean.shape}")

    return covid_clean, vacc_clean


def impute_missing_values(covid_df: pd.DataFrame, vacc_df: pd.DataFrame) -> tuple:
    """Imputa valores faltantes según estrategias definidas"""

    covid_imputed = covid_df.copy()
    vacc_imputed = vacc_df.copy()

    # Estrategias COVID
    covid_strategies = {
        "total_cases": 0,
        "new_cases": 0,
        "total_deaths": 0,
        "new_deaths": 0,
        "total_cases_per_million": 0,
        "total_deaths_per_million": 0,
    }

    # Imputar con valores específicos
    for col, value in covid_strategies.items():
        if col in covid_imputed.columns:
            covid_imputed[col].fillna(value, inplace=True)

    # Imputar variables socioeconómicas con mediana
    socio_cols = [
        "population_density",
        "median_age",
        "gdp_per_capita",
        "hospital_beds_per_thousand",
        "life_expectancy",
        "human_development_index",
    ]

    for col in socio_cols:
        if col in covid_imputed.columns:
            covid_imputed[col].fillna(covid_imputed[col].median(), inplace=True)

    # Estrategias Vacunación (rellenar con 0)
    vacc_fill_cols = [
        col for col in vacc_imputed.columns if col not in ["country", "date"]
    ]

    for col in vacc_fill_cols:
        vacc_imputed[col].fillna(0, inplace=True)

    logger.info(
        f"Missing COVID después de imputación: {covid_imputed.isnull().sum().sum()}"
    )
    logger.info(
        f"Missing Vacunación después de imputación: {vacc_imputed.isnull().sum().sum()}"
    )

    return covid_imputed, vacc_imputed


def create_features(covid_df: pd.DataFrame, vacc_df: pd.DataFrame) -> tuple:
    """Crea nuevas variables (feature engineering)"""

    covid_features = covid_df.copy()
    vacc_features = vacc_df.copy()

    # Features COVID
    # 1. Tasa de mortalidad
    covid_features["death_rate"] = (
        (covid_features["total_deaths"] / covid_features["total_cases"] * 100)
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )

    # 2. Índice de capacidad sanitaria
    if (
        "hospital_beds_per_thousand" in covid_features.columns
        and "gdp_per_capita" in covid_features.columns
    ):
        covid_features["healthcare_capacity_index"] = (
            covid_features["hospital_beds_per_thousand"]
            * covid_features["gdp_per_capita"]
            / 10000
        ).fillna(0)

    # Features Vacunación
    # 1. Velocidad de vacunación
    vacc_features["vaccination_speed"] = (
        vacc_features.groupby("country")["people_vaccinated_per_hundred"]
        .diff()
        .fillna(0)
    )

    # 2. Eficiencia
    vacc_features["vaccination_efficiency"] = (
        vacc_features["people_fully_vaccinated"] / vacc_features["people_vaccinated"]
    ).replace([np.inf, -np.inf], 0).fillna(0) * 100

    logger.info(
        f"Features creados - COVID: {len(covid_features.columns) - len(covid_df.columns)}"
    )
    logger.info(
        f"Features creados - Vacunación: {len(vacc_features.columns) - len(vacc_df.columns)}"
    )

    return covid_features, vacc_features


def integrate_datasets(covid_df: pd.DataFrame, vacc_df: pd.DataFrame) -> pd.DataFrame:
    """Integra datasets COVID y Vacunación"""

    # Último registro por país
    covid_latest = covid_df.sort_values("date").groupby("country").last().reset_index()
    vacc_latest = vacc_df.sort_values("date").groupby("country").last().reset_index()

    # Merge
    integrated = covid_latest.merge(
        vacc_latest, on="country", how="left", suffixes=("_covid", "_vacc")
    )

    logger.info(f"Dataset integrado: {integrated.shape}")
    logger.info(f"Países: {integrated['country'].nunique()}")

    return integrated


def create_ml_datasets(integrated_df: pd.DataFrame) -> tuple:
    """Crea datasets específicos para cada problema ML"""

    # Dataset Regresión
    regression_cols = [
        "country",
        "gdp_per_capita",
        "hospital_beds_per_thousand",
        "population_density",
        "median_age",
        "healthcare_capacity_index",
        "people_fully_vaccinated_per_hundred",
        "vaccination_speed",
    ]

    regression_data = integrated_df[
        [col for col in regression_cols if col in integrated_df.columns]
    ].dropna()

    # Dataset Clasificación
    classification_cols = [
        "country",
        "total_deaths_per_million",
        "gdp_per_capita",
        "hospital_beds_per_thousand",
        "human_development_index",
        "death_rate",
        "healthcare_capacity_index",
    ]

    classification_data = integrated_df[
        [col for col in classification_cols if col in integrated_df.columns]
    ].dropna()

    logger.info(f"Dataset Regresión: {regression_data.shape}")
    logger.info(f"Dataset Clasificación: {classification_data.shape}")

    return regression_data, classification_data

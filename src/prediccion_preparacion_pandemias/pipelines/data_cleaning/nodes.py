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

    # ============================================================================
    # Dataset Clasificación
    # ============================================================================
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
    ].copy()
    
    logger.info(f"Dataset clasificación inicial: {classification_data.shape}")
    logger.info(f"Nulos antes de imputación:")
    logger.info(f"\n{classification_data.isnull().sum()}")
    
    # ============================================================================
    # PASO 1: IMPUTAR NULOS EN FEATURES (CRÍTICO)
    # ============================================================================
    # Imputar human_development_index (100% nulo) con mediana global o valor por defecto
    if 'human_development_index' in classification_data.columns:
        # Calcular un HDI sintético basado en GDP
        if classification_data['human_development_index'].isnull().all():
            logger.warning("⚠️ human_development_index está 100% nulo, creando índice sintético...")
            # HDI sintético: normalización de GDP (0.3-0.95 range típico)
            gdp_normalized = (classification_data['gdp_per_capita'] - classification_data['gdp_per_capita'].min()) / \
                            (classification_data['gdp_per_capita'].max() - classification_data['gdp_per_capita'].min())
            classification_data['human_development_index'] = 0.3 + (gdp_normalized * 0.65)
            logger.info("✅ HDI sintético creado basado en GDP per capita")
        else:
            classification_data['human_development_index'].fillna(
                classification_data['human_development_index'].median(), 
                inplace=True
            )
    
    # Imputar otras columnas numéricas con mediana
    numeric_cols = ['gdp_per_capita', 'hospital_beds_per_thousand', 
                    'death_rate', 'healthcare_capacity_index', 
                    'total_deaths_per_million']
    
    for col in numeric_cols:
        if col in classification_data.columns:
            if classification_data[col].isnull().sum() > 0:
                median_val = classification_data[col].median()
                classification_data[col].fillna(median_val, inplace=True)
                logger.info(f"✅ Imputados {classification_data[col].isnull().sum()} nulos en '{col}' con mediana: {median_val:.2f}")
    
    logger.info(f"\nNulos después de imputación:")
    logger.info(f"\n{classification_data.isnull().sum()}")
    
    # ============================================================================
    # PASO 2: CREAR TARGET preparedness_level (después de imputar)
    # ============================================================================
    required_cols = [
        'healthcare_capacity_index', 
        'gdp_per_capita', 
        'hospital_beds_per_thousand', 
        'human_development_index'
    ]
    
    if all(col in classification_data.columns for col in required_cols):
        # Función para normalizar (0-1)
        def normalize(series):
            """Normaliza una serie al rango [0, 1]"""
            min_val = series.min()
            max_val = series.max()
            if max_val == min_val:
                return pd.Series(0.5, index=series.index)  # Valor medio si todos iguales
            return (series - min_val) / (max_val - min_val)
        
        # Calcular índice de preparación (weighted average)
        preparedness_index = (
            normalize(classification_data['healthcare_capacity_index']) * 0.30 +
            normalize(classification_data['gdp_per_capita']) * 0.25 +
            normalize(classification_data['hospital_beds_per_thousand']) * 0.25 +
            normalize(classification_data['human_development_index']) * 0.20
        )
        
        # Verificar que no hay NaN en el índice
        if preparedness_index.isnull().sum() > 0:
            logger.warning(f"⚠️ {preparedness_index.isnull().sum()} valores NaN en preparedness_index, imputando con mediana...")
            preparedness_index.fillna(preparedness_index.median(), inplace=True)
        
        # Clasificar en 3 niveles basado en terciles
        classification_data['preparedness_level'] = pd.cut(
            preparedness_index,
            bins=[-np.inf, 0.33, 0.67, np.inf],
            labels=['Alto_Riesgo', 'Riesgo_Medio', 'Bajo_Riesgo']
        )
        
        logger.info("✅ Target 'preparedness_level' creado exitosamente")
        logger.info(f"\nDistribución de clases:")
        logger.info(f"\n{classification_data['preparedness_level'].value_counts()}")
        logger.info(f"\nPorcentajes:")
        logger.info(f"\n{classification_data['preparedness_level'].value_counts(normalize=True) * 100}")
        
        # Verificar si quedaron NaN en el target
        target_nulls = classification_data['preparedness_level'].isnull().sum()
        if target_nulls > 0:
            logger.warning(f"⚠️ {target_nulls} valores NaN en preparedness_level, eliminando...")
            classification_data = classification_data.dropna(subset=['preparedness_level'])
            logger.info(f"✅ Dataset después de eliminar NaN en target: {classification_data.shape}")
    else:
        missing = [col for col in required_cols if col not in classification_data.columns]
        logger.error(f"❌ No se pudo crear 'preparedness_level' - Faltan columnas: {missing}")

    logger.info(f"\n📊 RESUMEN FINAL:")
    logger.info(f"Dataset Regresión: {regression_data.shape}")
    logger.info(f"Dataset Clasificación: {classification_data.shape}")

    return regression_data, classification_data
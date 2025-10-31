"""
Nodos para el pipeline de modelos de regresión.
Implementa múltiples modelos con búsqueda de hiperparámetros y validación cruzada.

Objetivo: Predecir días para alcanzar 70% cobertura vacunal
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Tuple, Any
from sklearn.model_selection import GridSearchCV, cross_val_score, KFold
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    mean_squared_error, 
    mean_absolute_error, 
    r2_score,
    mean_absolute_percentage_error
)

# Modelos de regresión
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import (
    RandomForestRegressor, 
    GradientBoostingRegressor,
    AdaBoostRegressor,
    ExtraTreesRegressor
)
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

logger = logging.getLogger(__name__)


def prepare_regression_data(df: pd.DataFrame, target_column: str = 'days_to_70_percent') -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepara los datos para regresión, separando features y target.
    
    Args:
        df: DataFrame con features y target
        target_column: Nombre de la columna target
        
    Returns:
        Tuple con (X, y) - features y target
    """
    logger.info(f"Preparando datos para regresión. Shape inicial: {df.shape}")
    
    # Verificar que existe la columna target
    if target_column not in df.columns:
        raise ValueError(f"Columna target '{target_column}' no encontrada. Columnas disponibles: {df.columns.tolist()}")
    
    # Separar features y target
    X = df.drop(columns=[target_column])
    y = df[target_column]
    
    # Remover columnas no numéricas si existen
    numeric_cols = X.select_dtypes(include=[np.number]).columns
    X = X[numeric_cols]
    
    logger.info(f"Features: {X.shape[1]} columnas")
    logger.info(f"Target: {y.shape[0]} muestras")
    logger.info(f"Target stats - Mean: {y.mean():.2f}, Std: {y.std():.2f}, Min: {y.min():.2f}, Max: {y.max():.2f}")
    
    return X, y


def scale_features(X: pd.DataFrame) -> Tuple[np.ndarray, StandardScaler]:
    """
    Escala las features usando StandardScaler.
    
    Args:
        X: DataFrame con features
        
    Returns:
        Tuple con (X_scaled, scaler)
    """
    logger.info("Escalando features con StandardScaler")
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    logger.info(f"Features escaladas. Shape: {X_scaled.shape}")
    
    return X_scaled, scaler


def train_linear_regression(X: np.ndarray, y: pd.Series, cv_folds: int = 5) -> Dict[str, Any]:
    """
    Entrena modelo de Regresión Lineal con validación cruzada.
    
    Args:
        X: Features escaladas
        y: Target
        cv_folds: Número de folds para CV
        
    Returns:
        Diccionario con modelo entrenado y métricas
    """
    logger.info("=" * 80)
    logger.info("MODELO 1: Linear Regression")
    logger.info("=" * 80)
    
    model = LinearRegression()
    
    # Validación cruzada
    kfold = KFold(n_splits=cv_folds, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X, y, cv=kfold, scoring='neg_mean_squared_error')
    cv_rmse = np.sqrt(-cv_scores)
    
    # Entrenar modelo final
    model.fit(X, y)
    y_pred = model.predict(X)
    
    # Calcular métricas
    metrics = {
        'model_name': 'Linear Regression',
        'model': model,
        'cv_rmse_mean': cv_rmse.mean(),
        'cv_rmse_std': cv_rmse.std(),
        'train_rmse': np.sqrt(mean_squared_error(y, y_pred)),
        'train_mae': mean_absolute_error(y, y_pred),
        'train_r2': r2_score(y, y_pred),
        'train_mape': mean_absolute_percentage_error(y, y_pred) * 100
    }
    
    logger.info(f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}")
    logger.info(f"Train RMSE: {metrics['train_rmse']:.2f}")
    logger.info(f"Train R²: {metrics['train_r2']:.4f}")
    
    return metrics


def train_ridge_regression(X: np.ndarray, y: pd.Series, cv_folds: int = 5) -> Dict[str, Any]:
    """
    Entrena Ridge Regression con búsqueda de hiperparámetros.
    
    Args:
        X: Features escaladas
        y: Target
        cv_folds: Número de folds para CV
        
    Returns:
        Diccionario con modelo entrenado y métricas
    """
    logger.info("=" * 80)
    logger.info("MODELO 2: Ridge Regression con GridSearchCV")
    logger.info("=" * 80)
    
    # Definir grid de hiperparámetros
    param_grid = {
        'alpha': [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]
    }
    
    model = Ridge(random_state=42)
    
    # GridSearchCV
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        cv=cv_folds,
        scoring='neg_mean_squared_error',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    best_model = grid_search.best_estimator_
    
    logger.info(f"Mejores hiperparámetros: {grid_search.best_params_}")
    
    # Predecir
    y_pred = best_model.predict(X)
    
    # Métricas
    metrics = {
        'model_name': 'Ridge Regression',
        'model': best_model,
        'best_params': grid_search.best_params_,
        'cv_rmse_mean': np.sqrt(-grid_search.best_score_),
        'cv_rmse_std': np.sqrt(-grid_search.cv_results_['std_test_score'][grid_search.best_index_]),
        'train_rmse': np.sqrt(mean_squared_error(y, y_pred)),
        'train_mae': mean_absolute_error(y, y_pred),
        'train_r2': r2_score(y, y_pred),
        'train_mape': mean_absolute_percentage_error(y, y_pred) * 100
    }
    
    logger.info(f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}")
    logger.info(f"Train RMSE: {metrics['train_rmse']:.2f}")
    logger.info(f"Train R²: {metrics['train_r2']:.4f}")
    
    return metrics


def train_random_forest(X: np.ndarray, y: pd.Series, cv_folds: int = 5) -> Dict[str, Any]:
    """
    Entrena Random Forest con búsqueda de hiperparámetros.
    
    Args:
        X: Features escaladas
        y: Target
        cv_folds: Número de folds para CV
        
    Returns:
        Diccionario con modelo entrenado y métricas
    """
    logger.info("=" * 80)
    logger.info("MODELO 3: Random Forest Regressor con GridSearchCV")
    logger.info("=" * 80)
    
    # Grid de hiperparámetros
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4]
    }
    
    model = RandomForestRegressor(random_state=42, n_jobs=-1)
    
    # GridSearchCV
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        cv=cv_folds,
        scoring='neg_mean_squared_error',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    best_model = grid_search.best_estimator_
    
    logger.info(f"Mejores hiperparámetros: {grid_search.best_params_}")
    
    # Predecir
    y_pred = best_model.predict(X)
    
    # Métricas
    metrics = {
        'model_name': 'Random Forest',
        'model': best_model,
        'best_params': grid_search.best_params_,
        'cv_rmse_mean': np.sqrt(-grid_search.best_score_),
        'cv_rmse_std': np.sqrt(-grid_search.cv_results_['std_test_score'][grid_search.best_index_]),
        'train_rmse': np.sqrt(mean_squared_error(y, y_pred)),
        'train_mae': mean_absolute_error(y, y_pred),
        'train_r2': r2_score(y, y_pred),
        'train_mape': mean_absolute_percentage_error(y, y_pred) * 100,
        'feature_importance': best_model.feature_importances_
    }
    
    logger.info(f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}")
    logger.info(f"Train RMSE: {metrics['train_rmse']:.2f}")
    logger.info(f"Train R²: {metrics['train_r2']:.4f}")
    
    return metrics


def train_gradient_boosting(X: np.ndarray, y: pd.Series, cv_folds: int = 5) -> Dict[str, Any]:
    """
    Entrena Gradient Boosting con búsqueda de hiperparámetros.
    
    Args:
        X: Features escaladas
        y: Target
        cv_folds: Número de folds para CV
        
    Returns:
        Diccionario con modelo entrenado y métricas
    """
    logger.info("=" * 80)
    logger.info("MODELO 4: Gradient Boosting Regressor con GridSearchCV")
    logger.info("=" * 80)
    
    # Grid de hiperparámetros
    param_grid = {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 5, 7],
        'min_samples_split': [2, 5, 10]
    }
    
    model = GradientBoostingRegressor(random_state=42)
    
    # GridSearchCV
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        cv=cv_folds,
        scoring='neg_mean_squared_error',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    best_model = grid_search.best_estimator_
    
    logger.info(f"Mejores hiperparámetros: {grid_search.best_params_}")
    
    # Predecir
    y_pred = best_model.predict(X)
    
    # Métricas
    metrics = {
        'model_name': 'Gradient Boosting',
        'model': best_model,
        'best_params': grid_search.best_params_,
        'cv_rmse_mean': np.sqrt(-grid_search.best_score_),
        'cv_rmse_std': np.sqrt(-grid_search.cv_results_['std_test_score'][grid_search.best_index_]),
        'train_rmse': np.sqrt(mean_squared_error(y, y_pred)),
        'train_mae': mean_absolute_error(y, y_pred),
        'train_r2': r2_score(y, y_pred),
        'train_mape': mean_absolute_percentage_error(y, y_pred) * 100,
        'feature_importance': best_model.feature_importances_
    }
    
    logger.info(f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}")
    logger.info(f"Train RMSE: {metrics['train_rmse']:.2f}")
    logger.info(f"Train R²: {metrics['train_r2']:.4f}")
    
    return metrics


def train_xgboost(X: np.ndarray, y: pd.Series, cv_folds: int = 5) -> Dict[str, Any]:
    """
    Entrena XGBoost con búsqueda de hiperparámetros.
    
    Args:
        X: Features escaladas
        y: Target
        cv_folds: Número de folds para CV
        
    Returns:
        Diccionario con modelo entrenado y métricas
    """
    logger.info("=" * 80)
    logger.info("MODELO 5: XGBoost Regressor con GridSearchCV")
    logger.info("=" * 80)
    
    # Grid de hiperparámetros
    param_grid = {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.3],
        'max_depth': [3, 5, 7],
        'subsample': [0.8, 1.0],
        'colsample_bytree': [0.8, 1.0]
    }
    
    model = XGBRegressor(random_state=42, n_jobs=-1)
    
    # GridSearchCV
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        cv=cv_folds,
        scoring='neg_mean_squared_error',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    best_model = grid_search.best_estimator_
    
    logger.info(f"Mejores hiperparámetros: {grid_search.best_params_}")
    
    # Predecir
    y_pred = best_model.predict(X)
    
    # Métricas
    metrics = {
        'model_name': 'XGBoost',
        'model': best_model,
        'best_params': grid_search.best_params_,
        'cv_rmse_mean': np.sqrt(-grid_search.best_score_),
        'cv_rmse_std': np.sqrt(-grid_search.cv_results_['std_test_score'][grid_search.best_index_]),
        'train_rmse': np.sqrt(mean_squared_error(y, y_pred)),
        'train_mae': mean_absolute_error(y, y_pred),
        'train_r2': r2_score(y, y_pred),
        'train_mape': mean_absolute_percentage_error(y, y_pred) * 100,
        'feature_importance': best_model.feature_importances_
    }
    
    logger.info(f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}")
    logger.info(f"Train RMSE: {metrics['train_rmse']:.2f}")
    logger.info(f"Train R²: {metrics['train_r2']:.4f}")
    
    return metrics


def train_svr(X: np.ndarray, y: pd.Series, cv_folds: int = 5) -> Dict[str, Any]:
    """
    Entrena Support Vector Regression con búsqueda de hiperparámetros.
    
    Args:
        X: Features escaladas
        y: Target
        cv_folds: Número de folds para CV
        
    Returns:
        Diccionario con modelo entrenado y métricas
    """
    logger.info("=" * 80)
    logger.info("MODELO 6: Support Vector Regression con GridSearchCV")
    logger.info("=" * 80)
    
    # Grid de hiperparámetros
    param_grid = {
        'kernel': ['linear', 'rbf', 'poly'],
        'C': [0.1, 1.0, 10.0],
        'epsilon': [0.01, 0.1, 0.5]
    }
    
    model = SVR()
    
    # GridSearchCV
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        cv=cv_folds,
        scoring='neg_mean_squared_error',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    best_model = grid_search.best_estimator_
    
    logger.info(f"Mejores hiperparámetros: {grid_search.best_params_}")
    
    # Predecir
    y_pred = best_model.predict(X)
    
    # Métricas
    metrics = {
        'model_name': 'SVR',
        'model': best_model,
        'best_params': grid_search.best_params_,
        'cv_rmse_mean': np.sqrt(-grid_search.best_score_),
        'cv_rmse_std': np.sqrt(-grid_search.cv_results_['std_test_score'][grid_search.best_index_]),
        'train_rmse': np.sqrt(mean_squared_error(y, y_pred)),
        'train_mae': mean_absolute_error(y, y_pred),
        'train_r2': r2_score(y, y_pred),
        'train_mape': mean_absolute_percentage_error(y, y_pred) * 100
    }
    
    logger.info(f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}")
    logger.info(f"Train RMSE: {metrics['train_rmse']:.2f}")
    logger.info(f"Train R²: {metrics['train_r2']:.4f}")
    
    return metrics


def train_lightgbm(X: np.ndarray, y: pd.Series, cv_folds: int = 5) -> Dict[str, Any]:
    """
    Entrena LightGBM con búsqueda de hiperparámetros.
    
    Args:
        X: Features escaladas
        y: Target
        cv_folds: Número de folds para CV
        
    Returns:
        Diccionario con modelo entrenado y métricas
    """
    logger.info("=" * 80)
    logger.info("MODELO 7: LightGBM Regressor con GridSearchCV")
    logger.info("=" * 80)
    
    # Grid de hiperparámetros
    param_grid = {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.3],
        'max_depth': [3, 5, 7],
        'num_leaves': [31, 50, 100],
        'min_child_samples': [20, 30, 50]
    }
    
    model = LGBMRegressor(random_state=42, n_jobs=-1, verbose=-1)
    
    # GridSearchCV
    grid_search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        cv=cv_folds,
        scoring='neg_mean_squared_error',
        n_jobs=-1,
        verbose=1
    )
    
    grid_search.fit(X, y)
    best_model = grid_search.best_estimator_
    
    logger.info(f"Mejores hiperparámetros: {grid_search.best_params_}")
    
    # Predecir
    y_pred = best_model.predict(X)
    
    # Métricas
    metrics = {
        'model_name': 'LightGBM',
        'model': best_model,
        'best_params': grid_search.best_params_,
        'cv_rmse_mean': np.sqrt(-grid_search.best_score_),
        'cv_rmse_std': np.sqrt(-grid_search.cv_results_['std_test_score'][grid_search.best_index_]),
        'train_rmse': np.sqrt(mean_squared_error(y, y_pred)),
        'train_mae': mean_absolute_error(y, y_pred),
        'train_r2': r2_score(y, y_pred),
        'train_mape': mean_absolute_percentage_error(y, y_pred) * 100,
        'feature_importance': best_model.feature_importances_
    }
    
    logger.info(f"CV RMSE: {metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}")
    logger.info(f"Train RMSE: {metrics['train_rmse']:.2f}")
    logger.info(f"Train R²: {metrics['train_r2']:.4f}")
    
    return metrics


def compare_regression_models(all_metrics: list) -> pd.DataFrame:
    """
    Compara todos los modelos de regresión y genera tabla resumen.
    
    Args:
        all_metrics: Lista de diccionarios con métricas de cada modelo
        
    Returns:
        DataFrame con comparación de modelos
    """
    logger.info("=" * 80)
    logger.info("COMPARACIÓN FINAL DE MODELOS DE REGRESIÓN")
    logger.info("=" * 80)
    
    # Crear DataFrame comparativo
    comparison_data = []
    
    for metrics in all_metrics:
        comparison_data.append({
            'Modelo': metrics['model_name'],
            'CV RMSE (mean±std)': f"{metrics['cv_rmse_mean']:.2f} ± {metrics['cv_rmse_std']:.2f}",
            'Train RMSE': f"{metrics['train_rmse']:.2f}",
            'Train MAE': f"{metrics['train_mae']:.2f}",
            'Train R²': f"{metrics['train_r2']:.4f}",
            'Train MAPE (%)': f"{metrics['train_mape']:.2f}",
            'Best Params': str(metrics.get('best_params', 'N/A'))
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # Ordenar por CV RMSE (extraer el valor numérico)
    comparison_df['cv_rmse_value'] = comparison_df['CV RMSE (mean±std)'].str.split(' ').str[0].astype(float)
    comparison_df = comparison_df.sort_values('cv_rmse_value').drop(columns=['cv_rmse_value'])
    comparison_df = comparison_df.reset_index(drop=True)
    
    logger.info("\n" + comparison_df.to_string())
    
    # Identificar mejor modelo
    best_model_name = comparison_df.iloc[0]['Modelo']
    logger.info(f"\n🏆 MEJOR MODELO: {best_model_name}")
    
    return comparison_df


def train_all_regression_models(df: pd.DataFrame, target_column: str = 'days_to_70_percent', cv_folds: int = 5) -> Tuple[pd.DataFrame, list]:
    """
    Función orquestadora que entrena todos los modelos de regresión.
    
    Args:
        df: DataFrame con datos preparados
        target_column: Nombre de la columna target
        cv_folds: Número de folds para validación cruzada
        
    Returns:
        Tuple con (DataFrame comparativo, lista de métricas)
    """
    logger.info("🚀 INICIANDO ENTRENAMIENTO DE MODELOS DE REGRESIÓN")
    logger.info(f"Dataset shape: {df.shape}")
    logger.info(f"Target column: {target_column}")
    logger.info(f"CV folds: {cv_folds}")
    
    # Preparar datos
    X, y = prepare_regression_data(df, target_column)
    X_scaled, scaler = scale_features(X)
    
    # Lista para almacenar métricas
    all_metrics = []
    
    # Entrenar cada modelo
    try:
        all_metrics.append(train_linear_regression(X_scaled, y, cv_folds))
    except Exception as e:
        logger.error(f"Error entrenando Linear Regression: {e}")
    
    try:
        all_metrics.append(train_ridge_regression(X_scaled, y, cv_folds))
    except Exception as e:
        logger.error(f"Error entrenando Ridge: {e}")
    
    try:
        all_metrics.append(train_random_forest(X_scaled, y, cv_folds))
    except Exception as e:
        logger.error(f"Error entrenando Random Forest: {e}")
    
    try:
        all_metrics.append(train_gradient_boosting(X_scaled, y, cv_folds))
    except Exception as e:
        logger.error(f"Error entrenando Gradient Boosting: {e}")
    
    try:
        all_metrics.append(train_xgboost(X_scaled, y, cv_folds))
    except Exception as e:
        logger.error(f"Error entrenando XGBoost: {e}")
    
    try:
        all_metrics.append(train_svr(X_scaled, y, cv_folds))
    except Exception as e:
        logger.error(f"Error entrenando SVR: {e}")
    
    try:
        all_metrics.append(train_lightgbm(X_scaled, y, cv_folds))
    except Exception as e:
        logger.error(f"Error entrenando LightGBM: {e}")
    
    # Comparar modelos
    comparison_df = compare_regression_models(all_metrics)
    
    logger.info("✅ ENTRENAMIENTO COMPLETADO")
    
    return comparison_df, all_metrics
"""
Nodos para el pipeline de modelos de clasificación.
Incluye 7+ modelos con GridSearchCV y CrossValidation.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Tuple, Any, List
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    classification_report, confusion_matrix, roc_auc_score
)
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (
    RandomForestClassifier,
    GradientBoostingClassifier
)
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
import xgboost as xgb
import lightgbm as lgb

logger = logging.getLogger(__name__)


def prepare_classification_data(
    df: pd.DataFrame,
    target_column: str,
    test_size: float = 0.2,
    random_state: int = 42
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    Prepara los datos para clasificación.
    
    Args:
        df: DataFrame con features y target
        target_column: Nombre de la columna target
        test_size: Proporción del conjunto de prueba
        random_state: Semilla para reproducibilidad
        
    Returns:
        Tuple con (X_train, X_test, y_train, y_test)
    """
    logger.info(f"Preparando datos para clasificación con target: {target_column}")
    
    # Verificar que existe la columna target
    if target_column not in df.columns:
        raise ValueError(f"Columna target '{target_column}' no encontrada en el DataFrame")
    
    # Separar features y target
    X = df.drop(columns=[target_column, 'country'])
    y = df[target_column]
    
    logger.info(f"Datos antes de split - X shape: {X.shape}, y shape: {y.shape}")
    logger.info(f"Distribución de clases:\n{y.value_counts()}")
    
    # ============================================================================
    # ✅ CODIFICAR LABELS PARA XGBOOST/LIGHTGBM
    # ============================================================================
    # XGBoost y LightGBM requieren clases numéricas (0, 1, 2...)
    # mientras que sklearn puede trabajar con strings
    label_encoder = LabelEncoder()
    y_encoded = label_encoder.fit_transform(y)
    
    logger.info(f"✅ Clases originales: {label_encoder.classes_}")
    logger.info(f"✅ Clases codificadas: {sorted(set(y_encoded))}")
    logger.info(f"✅ Mapeo: {dict(zip(label_encoder.classes_, label_encoder.transform(label_encoder.classes_)))}")
    
    # Split train/test con clases codificadas
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_encoded, test_size=test_size, random_state=random_state, stratify=y_encoded
    )
    
    logger.info(f"Train set: X={X_train.shape}, y={y_train.shape}")
    logger.info(f"Test set: X={X_test.shape}, y={y_test.shape}")
    
    return X_train, X_test, y_train, y_test


def scale_classification_features(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame
) -> Tuple[np.ndarray, np.ndarray, StandardScaler]:
    """
    Escala las features usando StandardScaler.
    
    Args:
        X_train: Features de entrenamiento
        X_test: Features de prueba
        
    Returns:
        Tuple con (X_train_scaled, X_test_scaled, scaler)
    """
    logger.info("Escalando features para clasificación")
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    logger.info("Features escaladas exitosamente")
    
    return X_train_scaled, X_test_scaled, scaler


def train_logistic_regression(
    X_train: np.ndarray,
    y_train: pd.Series,
    cv_folds: int = 5,
    random_state: int = 42
) -> Dict[str, Any]:
    """
    Entrena Logistic Regression con GridSearchCV.
    
    Args:
        X_train: Features de entrenamiento
        y_train: Target de entrenamiento
        cv_folds: Número de folds para CV
        random_state: Semilla
        
    Returns:
        Diccionario con modelo y métricas
    """
    logger.info("Entrenando Logistic Regression con GridSearchCV")
    
    param_grid = {
        'C': [0.01, 0.1, 1.0, 10.0],
        'penalty': ['l2'],
        'solver': ['lbfgs', 'saga'],
        'max_iter': [1000]
    }
    
    lr = LogisticRegression(random_state=random_state)
    
    grid_search = GridSearchCV(
        lr, param_grid, cv=cv_folds, 
        scoring='f1_weighted', n_jobs=-1, verbose=1
    )
    
    grid_search.fit(X_train, y_train)
    
    # Cross-validation scores
    cv_scores = cross_val_score(
        grid_search.best_estimator_, X_train, y_train,
        cv=cv_folds, scoring='f1_weighted'
    )
    
    result = {
        'model_name': 'Logistic Regression',
        'model': grid_search.best_estimator_,
        'best_params': grid_search.best_params_,
        'cv_f1_mean': cv_scores.mean(),
        'cv_f1_std': cv_scores.std(),
        'cv_scores': cv_scores
    }
    
    logger.info(f"LR - CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f}")
    logger.info(f"Best params: {result['best_params']}")
    
    return result


def train_random_forest_classifier(
    X_train: np.ndarray,
    y_train: pd.Series,
    cv_folds: int = 5,
    random_state: int = 42
) -> Dict[str, Any]:
    """
    Entrena Random Forest Classifier con GridSearchCV.
    """
    logger.info("Entrenando Random Forest Classifier con GridSearchCV")
    
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5],
        'min_samples_leaf': [1, 2],
        'max_features': ['sqrt', 'log2']
    }
    
    rf = RandomForestClassifier(random_state=random_state, n_jobs=-1)
    
    grid_search = GridSearchCV(
        rf, param_grid, cv=cv_folds,
        scoring='f1_weighted', n_jobs=-1, verbose=1
    )
    
    grid_search.fit(X_train, y_train)
    
    cv_scores = cross_val_score(
        grid_search.best_estimator_, X_train, y_train,
        cv=cv_folds, scoring='f1_weighted'
    )
    
    result = {
        'model_name': 'Random Forest',
        'model': grid_search.best_estimator_,
        'best_params': grid_search.best_params_,
        'cv_f1_mean': cv_scores.mean(),
        'cv_f1_std': cv_scores.std(),
        'cv_scores': cv_scores
    }
    
    logger.info(f"RF - CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f}")
    
    return result


def train_gradient_boosting_classifier(
    X_train: np.ndarray,
    y_train: pd.Series,
    cv_folds: int = 5,
    random_state: int = 42
) -> Dict[str, Any]:
    """
    Entrena Gradient Boosting Classifier con GridSearchCV.
    """
    logger.info("Entrenando Gradient Boosting Classifier con GridSearchCV")
    
    param_grid = {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 5, 7],
        'subsample': [0.8, 1.0]
    }
    
    gb = GradientBoostingClassifier(random_state=random_state)
    
    grid_search = GridSearchCV(
        gb, param_grid, cv=cv_folds,
        scoring='f1_weighted', n_jobs=-1, verbose=1
    )
    
    grid_search.fit(X_train, y_train)
    
    cv_scores = cross_val_score(
        grid_search.best_estimator_, X_train, y_train,
        cv=cv_folds, scoring='f1_weighted'
    )
    
    result = {
        'model_name': 'Gradient Boosting',
        'model': grid_search.best_estimator_,
        'best_params': grid_search.best_params_,
        'cv_f1_mean': cv_scores.mean(),
        'cv_f1_std': cv_scores.std(),
        'cv_scores': cv_scores
    }
    
    logger.info(f"GB - CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f}")
    
    return result


def train_xgboost_classifier(
    X_train: np.ndarray,
    y_train: pd.Series,
    cv_folds: int = 5,
    random_state: int = 42
) -> Dict[str, Any]:
    """
    Entrena XGBoost Classifier con GridSearchCV.
    """
    logger.info("Entrenando XGBoost Classifier con GridSearchCV")
    
    param_grid = {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 5, 7],
        'subsample': [0.8, 1.0],
        'colsample_bytree': [0.8, 1.0]
    }
    
    xgb_clf = xgb.XGBClassifier(
        random_state=random_state,
        n_jobs=-1,
        eval_metric='logloss'
    )
    
    grid_search = GridSearchCV(
        xgb_clf, param_grid, cv=cv_folds,
        scoring='f1_weighted', n_jobs=-1, verbose=1
    )
    
    grid_search.fit(X_train, y_train)
    
    cv_scores = cross_val_score(
        grid_search.best_estimator_, X_train, y_train,
        cv=cv_folds, scoring='f1_weighted'
    )
    
    result = {
        'model_name': 'XGBoost',
        'model': grid_search.best_estimator_,
        'best_params': grid_search.best_params_,
        'cv_f1_mean': cv_scores.mean(),
        'cv_f1_std': cv_scores.std(),
        'cv_scores': cv_scores
    }
    
    logger.info(f"XGB - CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f}")
    
    return result


def train_lightgbm_classifier(
    X_train: np.ndarray,
    y_train: pd.Series,
    cv_folds: int = 5,
    random_state: int = 42
) -> Dict[str, Any]:
    """
    Entrena LightGBM Classifier con GridSearchCV.
    """
    logger.info("Entrenando LightGBM Classifier con GridSearchCV")
    
    param_grid = {
        'n_estimators': [50, 100, 200],
        'learning_rate': [0.01, 0.1, 0.2],
        'max_depth': [3, 5, 7],
        'num_leaves': [31, 50, 100],
        'subsample': [0.8, 1.0]
    }
    
    lgb_clf = lgb.LGBMClassifier(
        random_state=random_state,
        n_jobs=-1,
        verbose=-1
    )
    
    grid_search = GridSearchCV(
        lgb_clf, param_grid, cv=cv_folds,
        scoring='f1_weighted', n_jobs=-1, verbose=1
    )
    
    grid_search.fit(X_train, y_train)
    
    cv_scores = cross_val_score(
        grid_search.best_estimator_, X_train, y_train,
        cv=cv_folds, scoring='f1_weighted'
    )
    
    result = {
        'model_name': 'LightGBM',
        'model': grid_search.best_estimator_,
        'best_params': grid_search.best_params_,
        'cv_f1_mean': cv_scores.mean(),
        'cv_f1_std': cv_scores.std(),
        'cv_scores': cv_scores
    }
    
    logger.info(f"LGB - CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f}")
    
    return result


def train_svc_classifier(
    X_train: np.ndarray,
    y_train: pd.Series,
    cv_folds: int = 5,
    random_state: int = 42
) -> Dict[str, Any]:
    """
    Entrena Support Vector Classifier con GridSearchCV.
    """
    logger.info("Entrenando SVC con GridSearchCV")
    
    param_grid = {
        'C': [0.1, 1.0, 10.0],
        'kernel': ['rbf', 'linear'],
        'gamma': ['scale', 'auto']
    }
    
    svc = SVC(random_state=random_state, probability=True)
    
    grid_search = GridSearchCV(
        svc, param_grid, cv=cv_folds,
        scoring='f1_weighted', n_jobs=-1, verbose=1
    )
    
    grid_search.fit(X_train, y_train)
    
    cv_scores = cross_val_score(
        grid_search.best_estimator_, X_train, y_train,
        cv=cv_folds, scoring='f1_weighted'
    )
    
    result = {
        'model_name': 'SVC',
        'model': grid_search.best_estimator_,
        'best_params': grid_search.best_params_,
        'cv_f1_mean': cv_scores.mean(),
        'cv_f1_std': cv_scores.std(),
        'cv_scores': cv_scores
    }
    
    logger.info(f"SVC - CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f}")
    
    return result


def train_knn_classifier(
    X_train: np.ndarray,
    y_train: pd.Series,
    cv_folds: int = 5
) -> Dict[str, Any]:
    """
    Entrena K-Nearest Neighbors Classifier con GridSearchCV.
    """
    logger.info("Entrenando KNN Classifier con GridSearchCV")
    
    param_grid = {
        'n_neighbors': [3, 5, 7, 9, 11],
        'weights': ['uniform', 'distance'],
        'metric': ['euclidean', 'manhattan']
    }
    
    knn = KNeighborsClassifier(n_jobs=-1)
    
    grid_search = GridSearchCV(
        knn, param_grid, cv=cv_folds,
        scoring='f1_weighted', n_jobs=-1, verbose=1
    )
    
    grid_search.fit(X_train, y_train)
    
    cv_scores = cross_val_score(
        grid_search.best_estimator_, X_train, y_train,
        cv=cv_folds, scoring='f1_weighted'
    )
    
    result = {
        'model_name': 'KNN',
        'model': grid_search.best_estimator_,
        'best_params': grid_search.best_params_,
        'cv_f1_mean': cv_scores.mean(),
        'cv_f1_std': cv_scores.std(),
        'cv_scores': cv_scores
    }
    
    logger.info(f"KNN - CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f}")
    
    return result


def train_all_classification_models(
    X_train: np.ndarray,
    y_train: pd.Series,
    X_test: np.ndarray,
    y_test: pd.Series,
    cv_folds: int = 5,
    random_state: int = 42
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Entrena todos los modelos de clasificación y retorna resultados.
    
    Args:
        X_train: Features de entrenamiento
        y_train: Target de entrenamiento
        X_test: Features de prueba
        y_test: Target de prueba
        cv_folds: Número de folds para CV
        random_state: Semilla
        
    Returns:
        Tuple con (lista de resultados, mejor modelo)
    """
    logger.info("=" * 80)
    logger.info("ENTRENANDO TODOS LOS MODELOS DE CLASIFICACIÓN")
    logger.info("=" * 80)
    
    all_results = []
    
    # 1. Logistic Regression
    lr_result = train_logistic_regression(X_train, y_train, cv_folds, random_state)
    y_pred = lr_result['model'].predict(X_test)
    lr_result['test_accuracy'] = accuracy_score(y_test, y_pred)
    lr_result['test_f1'] = f1_score(y_test, y_pred, average='weighted')
    lr_result['test_precision'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    lr_result['test_recall'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    all_results.append(lr_result)
    
    # 2. Random Forest
    rf_result = train_random_forest_classifier(X_train, y_train, cv_folds, random_state)
    y_pred = rf_result['model'].predict(X_test)
    rf_result['test_accuracy'] = accuracy_score(y_test, y_pred)
    rf_result['test_f1'] = f1_score(y_test, y_pred, average='weighted')
    rf_result['test_precision'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    rf_result['test_recall'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    all_results.append(rf_result)
    
    # 3. Gradient Boosting
    gb_result = train_gradient_boosting_classifier(X_train, y_train, cv_folds, random_state)
    y_pred = gb_result['model'].predict(X_test)
    gb_result['test_accuracy'] = accuracy_score(y_test, y_pred)
    gb_result['test_f1'] = f1_score(y_test, y_pred, average='weighted')
    gb_result['test_precision'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    gb_result['test_recall'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    all_results.append(gb_result)
    
    # 4. XGBoost
    xgb_result = train_xgboost_classifier(X_train, y_train, cv_folds, random_state)
    y_pred = xgb_result['model'].predict(X_test)
    xgb_result['test_accuracy'] = accuracy_score(y_test, y_pred)
    xgb_result['test_f1'] = f1_score(y_test, y_pred, average='weighted')
    xgb_result['test_precision'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    xgb_result['test_recall'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    all_results.append(xgb_result)
    
    # 5. LightGBM
    lgb_result = train_lightgbm_classifier(X_train, y_train, cv_folds, random_state)
    y_pred = lgb_result['model'].predict(X_test)
    lgb_result['test_accuracy'] = accuracy_score(y_test, y_pred)
    lgb_result['test_f1'] = f1_score(y_test, y_pred, average='weighted')
    lgb_result['test_precision'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    lgb_result['test_recall'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    all_results.append(lgb_result)
    
    # 6. SVC
    svc_result = train_svc_classifier(X_train, y_train, cv_folds, random_state)
    y_pred = svc_result['model'].predict(X_test)
    svc_result['test_accuracy'] = accuracy_score(y_test, y_pred)
    svc_result['test_f1'] = f1_score(y_test, y_pred, average='weighted')
    svc_result['test_precision'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    svc_result['test_recall'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    all_results.append(svc_result)
    
    # 7. KNN
    knn_result = train_knn_classifier(X_train, y_train, cv_folds)
    y_pred = knn_result['model'].predict(X_test)
    knn_result['test_accuracy'] = accuracy_score(y_test, y_pred)
    knn_result['test_f1'] = f1_score(y_test, y_pred, average='weighted')
    knn_result['test_precision'] = precision_score(y_test, y_pred, average='weighted', zero_division=0)
    knn_result['test_recall'] = recall_score(y_test, y_pred, average='weighted', zero_division=0)
    all_results.append(knn_result)
    
    # Encontrar mejor modelo
    best_model_result = max(all_results, key=lambda x: x['cv_f1_mean'])
    
    logger.info("=" * 80)
    logger.info("RESUMEN DE RESULTADOS")
    logger.info("=" * 80)
    for result in all_results:
        logger.info(
            f"{result['model_name']:25s} | "
            f"CV F1: {result['cv_f1_mean']:.4f} ± {result['cv_f1_std']:.4f} | "
            f"Test F1: {result['test_f1']:.4f} | "
            f"Test Acc: {result['test_accuracy']:.4f}"
        )
    
    logger.info("=" * 80)
    logger.info(f"MEJOR MODELO: {best_model_result['model_name']}")
    logger.info("=" * 80)
    
    return all_results, best_model_result


def create_classification_comparison_table(
    all_results: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Crea tabla comparativa de todos los modelos.
    
    Args:
        all_results: Lista con resultados de todos los modelos
        
    Returns:
        DataFrame con comparación
    """
    logger.info("Creando tabla comparativa de clasificación")
    
    comparison_data = []
    for result in all_results:
        comparison_data.append({
            'Model': result['model_name'],
            'CV_F1_mean': result['cv_f1_mean'],
            'CV_F1_std': result['cv_f1_std'],
            'Test_Accuracy': result['test_accuracy'],
            'Test_F1': result['test_f1'],
            'Test_Precision': result['test_precision'],
            'Test_Recall': result['test_recall']
        })
    
    df_comparison = pd.DataFrame(comparison_data)
    df_comparison = df_comparison.sort_values('CV_F1_mean', ascending=False)
    
    logger.info(f"Tabla comparativa creada con {len(df_comparison)} modelos")
    return df_comparison
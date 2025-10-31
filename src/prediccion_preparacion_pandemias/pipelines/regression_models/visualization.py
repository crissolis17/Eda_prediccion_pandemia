"""
Script de visualización para comparación de modelos de regresión.
Genera gráficos comparativos de métricas y feature importance.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Configurar estilo de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10


def plot_models_comparison(comparison_df: pd.DataFrame, save_path: str = None) -> plt.Figure:
    """
    Genera gráfico comparativo de modelos de regresión.
    
    Args:
        comparison_df: DataFrame con comparación de modelos
        save_path: Ruta para guardar el gráfico (opcional)
        
    Returns:
        Figura de matplotlib
    """
    logger.info("Generando gráfico comparativo de modelos")
    
    # Extraer valores numéricos
    models = comparison_df['Modelo'].values
    
    # Extraer RMSE y R² como valores numéricos
    rmse_values = comparison_df['Train RMSE'].astype(float).values
    r2_values = comparison_df['Train R²'].astype(float).values
    mae_values = comparison_df['Train MAE'].astype(float).values
    
    # Extraer CV RMSE mean
    cv_rmse_mean = comparison_df['CV RMSE (mean±std)'].str.split(' ').str[0].astype(float).values
    cv_rmse_std = comparison_df['CV RMSE (mean±std)'].str.split('±').str[1].str.strip().astype(float).values
    
    # Crear figura con subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Comparación de Modelos de Regresión - Predicción Cobertura Vacunal', 
                 fontsize=16, fontweight='bold')
    
    # 1. CV RMSE con barras de error
    ax1 = axes[0, 0]
    bars = ax1.bar(range(len(models)), cv_rmse_mean, yerr=cv_rmse_std, 
                    capsize=5, color='skyblue', edgecolor='navy', alpha=0.7)
    ax1.set_xlabel('Modelo', fontweight='bold')
    ax1.set_ylabel('CV RMSE (días)', fontweight='bold')
    ax1.set_title('Cross-Validation RMSE (k=5)', fontweight='bold')
    ax1.set_xticks(range(len(models)))
    ax1.set_xticklabels(models, rotation=45, ha='right')
    ax1.grid(axis='y', alpha=0.3)
    
    # Anotar valores
    for i, (bar, val, std) in enumerate(zip(bars, cv_rmse_mean, cv_rmse_std)):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + std,
                f'{val:.1f}±{std:.1f}',
                ha='center', va='bottom', fontsize=8)
    
    # 2. Train RMSE vs Train MAE
    ax2 = axes[0, 1]
    x = np.arange(len(models))
    width = 0.35
    bars1 = ax2.bar(x - width/2, rmse_values, width, label='RMSE', 
                     color='coral', edgecolor='darkred', alpha=0.7)
    bars2 = ax2.bar(x + width/2, mae_values, width, label='MAE',
                     color='lightgreen', edgecolor='darkgreen', alpha=0.7)
    ax2.set_xlabel('Modelo', fontweight='bold')
    ax2.set_ylabel('Error (días)', fontweight='bold')
    ax2.set_title('Train RMSE vs MAE', fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(models, rotation=45, ha='right')
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)
    
    # 3. R² Score
    ax3 = axes[1, 0]
    colors = ['green' if r2 > 0.8 else 'orange' if r2 > 0.6 else 'red' for r2 in r2_values]
    bars = ax3.barh(models, r2_values, color=colors, edgecolor='black', alpha=0.7)
    ax3.set_xlabel('R² Score', fontweight='bold')
    ax3.set_ylabel('Modelo', fontweight='bold')
    ax3.set_title('Coeficiente de Determinación (R²)', fontweight='bold')
    ax3.set_xlim([0, 1.0])
    ax3.grid(axis='x', alpha=0.3)
    
    # Líneas de referencia
    ax3.axvline(x=0.8, color='green', linestyle='--', alpha=0.5, label='Excelente (>0.8)')
    ax3.axvline(x=0.6, color='orange', linestyle='--', alpha=0.5, label='Bueno (>0.6)')
    ax3.legend(loc='lower right', fontsize=8)
    
    # Anotar valores
    for i, (bar, val) in enumerate(zip(bars, r2_values)):
        width = bar.get_width()
        ax3.text(width + 0.02, bar.get_y() + bar.get_height()/2.,
                f'{val:.4f}',
                ha='left', va='center', fontsize=9, fontweight='bold')
    
    # 4. Ranking general
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Crear tabla de ranking
    ranking_data = []
    for i, model in enumerate(models):
        ranking_data.append([
            i+1,
            model,
            f"{cv_rmse_mean[i]:.2f}",
            f"{r2_values[i]:.4f}",
            f"{mae_values[i]:.2f}"
        ])
    
    table = ax4.table(cellText=ranking_data,
                      colLabels=['#', 'Modelo', 'CV RMSE', 'R²', 'MAE'],
                      cellLoc='center',
                      loc='center',
                      bbox=[0, 0, 1, 1])
    
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)
    
    # Colorear header
    for i in range(5):
        table[(0, i)].set_facecolor('#4CAF50')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Colorear mejor modelo
    for i in range(5):
        table[(1, i)].set_facecolor('#FFD700')
        table[(1, i)].set_text_props(weight='bold')
    
    ax4.set_title('Ranking de Modelos (Ordenado por CV RMSE)', 
                  fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Gráfico guardado en: {save_path}")
    
    return fig


def plot_feature_importance(all_metrics: List[Dict[str, Any]], 
                            feature_names: List[str],
                            top_n: int = 15,
                            save_path: str = None) -> plt.Figure:
    """
    Genera gráfico de feature importance para modelos basados en árboles.
    
    Args:
        all_metrics: Lista de diccionarios con métricas de modelos
        feature_names: Lista con nombres de features
        top_n: Número de features más importantes a mostrar
        save_path: Ruta para guardar el gráfico (opcional)
        
    Returns:
        Figura de matplotlib
    """
    logger.info("Generando gráfico de feature importance")
    
    # Filtrar modelos que tienen feature_importance
    models_with_importance = [m for m in all_metrics if 'feature_importance' in m]
    
    if not models_with_importance:
        logger.warning("No hay modelos con feature importance disponible")
        return None
    
    n_models = len(models_with_importance)
    fig, axes = plt.subplots(1, n_models, figsize=(6*n_models, 8))
    
    if n_models == 1:
        axes = [axes]
    
    fig.suptitle('Feature Importance - Modelos Basados en Árboles', 
                 fontsize=16, fontweight='bold')
    
    for idx, (metrics, ax) in enumerate(zip(models_with_importance, axes)):
        importance = metrics['feature_importance']
        model_name = metrics['model_name']
        
        # Crear DataFrame y ordenar
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importance
        }).sort_values('importance', ascending=False).head(top_n)
        
        # Plot
        bars = ax.barh(importance_df['feature'], importance_df['importance'],
                       color='steelblue', edgecolor='navy', alpha=0.7)
        ax.set_xlabel('Importancia', fontweight='bold')
        ax.set_title(f'{model_name}\n(Top {top_n} Features)', fontweight='bold')
        ax.invert_yaxis()
        ax.grid(axis='x', alpha=0.3)
        
        # Anotar valores
        for i, (bar, val) in enumerate(zip(bars, importance_df['importance'])):
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2.,
                   f'{val:.4f}',
                   ha='left', va='center', fontsize=8)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Gráfico de feature importance guardado en: {save_path}")
    
    return fig


def plot_cv_scores_distribution(all_metrics: List[Dict[str, Any]], 
                                 save_path: str = None) -> plt.Figure:
    """
    Genera gráfico de distribución de scores de validación cruzada.
    
    Args:
        all_metrics: Lista de diccionarios con métricas de modelos
        save_path: Ruta para guardar el gráfico (opcional)
        
    Returns:
        Figura de matplotlib
    """
    logger.info("Generando gráfico de distribución de CV scores")
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    models = [m['model_name'] for m in all_metrics]
    cv_means = [m['cv_rmse_mean'] for m in all_metrics]
    cv_stds = [m['cv_rmse_std'] for m in all_metrics]
    
    # Boxplot simulado con mean y std
    positions = range(len(models))
    
    for pos, mean, std, model in zip(positions, cv_means, cv_stds, models):
        # Simular distribución normal
        samples = np.random.normal(mean, std, 100)
        
        violin = ax.violinplot([samples], positions=[pos], 
                               widths=0.7, showmeans=True, showmedians=True)
        
        # Colorear
        for pc in violin['bodies']:
            pc.set_facecolor('lightblue')
            pc.set_alpha(0.7)
    
    ax.set_xticks(positions)
    ax.set_xticklabels(models, rotation=45, ha='right')
    ax.set_ylabel('CV RMSE (días)', fontweight='bold')
    ax.set_xlabel('Modelo', fontweight='bold')
    ax.set_title('Distribución de Scores de Validación Cruzada (k=5)', 
                 fontweight='bold', fontsize=14)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Gráfico de distribución CV guardado en: {save_path}")
    
    return fig


def generate_all_plots(comparison_df: pd.DataFrame, 
                       all_metrics: List[Dict[str, Any]],
                       feature_names: List[str] = None,
                       output_dir: str = 'data/08_reporting/') -> None:
    """
    Genera todos los gráficos de comparación de modelos.
    
    Args:
        comparison_df: DataFrame con comparación de modelos
        all_metrics: Lista de métricas de todos los modelos
        feature_names: Lista de nombres de features (opcional)
        output_dir: Directorio de salida para gráficos
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info("Generando todos los gráficos de comparación")
    
    # 1. Comparación general
    plot_models_comparison(
        comparison_df, 
        save_path=f"{output_dir}regression_models_comparison.png"
    )
    
    # 2. Feature importance (si hay features disponibles)
    if feature_names:
        plot_feature_importance(
            all_metrics, 
            feature_names,
            save_path=f"{output_dir}regression_feature_importance.png"
        )
    
    # 3. Distribución de CV scores
    plot_cv_scores_distribution(
        all_metrics,
        save_path=f"{output_dir}regression_cv_distribution.png"
    )
    
    logger.info(f"✅ Todos los gráficos generados en: {output_dir}")
    plt.close('all')


if __name__ == "__main__":
    # Ejemplo de uso
    print("Script de visualización para modelos de regresión")
    print("Usar desde Kedro pipeline o importar las funciones")
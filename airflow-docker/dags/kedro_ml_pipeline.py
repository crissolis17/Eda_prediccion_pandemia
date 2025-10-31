"""
DAG de Airflow - Consolidación de Resultados ML
Evaluación Parcial 2

NOTA: Ejecuta primero los pipelines de Kedro manualmente
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json
import os

default_args = {
    'owner': 'ml_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'kedro_ml_pipeline',
    default_args=default_args,
    description='Consolidación ML: Clasificación + Regresión',
    schedule_interval=None,
    start_date=datetime(2025, 10, 29),
    catchup=False,
    tags=['kedro', 'ml'],
)

BASE_PATH = r'D:\Maching Learnign\Prediccion Pandemias\prediccion-preparacion-pandemias'

def verify_results(**context):
    """Verifica existencia de resultados"""
    print("=" * 80)
    print("VERIFICANDO RESULTADOS")
    print("=" * 80)
    
    clf_path = os.path.join(BASE_PATH, 'data/07_reporting/classification_comparison_table.csv')
    reg_path = os.path.join(BASE_PATH, 'data/07_reporting/regression_comparison_table.csv')
    
    clf_exists = os.path.exists(clf_path)
    reg_exists = os.path.exists(reg_path)
    
    print(f"✅ Clasificación: {clf_exists}")
    print(f"✅ Regresión: {reg_exists}")
    
    if not (clf_exists and reg_exists):
        raise FileNotFoundError("Ejecuta primero: kedro run")
    
    return True

def consolidate_results(**context):
    """Consolida todos los resultados"""
    print("=" * 80)
    print("CONSOLIDANDO RESULTADOS")
    print("=" * 80)
    
    # Leer clasificación
    clf_path = os.path.join(BASE_PATH, 'data/07_reporting/classification_comparison_table.csv')
    clf_df = pd.read_csv(clf_path)
    
    # Leer regresión
    reg_path = os.path.join(BASE_PATH, 'data/07_reporting/regression_comparison_table.csv')
    reg_df = pd.read_csv(reg_path)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'classification': {
            'n_models': len(clf_df),
            'best_model': clf_df.loc[clf_df['CV_F1_mean'].idxmax(), 'Model'],
            'best_cv_f1': float(clf_df['CV_F1_mean'].max())
        },
        'regression': {
            'n_models': len(reg_df),
            'best_model': reg_df.loc[reg_df['CV_R2_mean'].idxmax(), 'Model'],
            'best_cv_r2': float(reg_df['CV_R2_mean'].max())
        }
    }
    
    # Guardar
    output_path = os.path.join(BASE_PATH, 'data/07_reporting/airflow_results.json')
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Resultados guardados en: {output_path}")
    print(f"   Clasificación: {results['classification']['n_models']} modelos")
    print(f"   Regresión: {results['regression']['n_models']} modelos")
    
    return results

# Tasks
t1 = PythonOperator(
    task_id='verify',
    python_callable=verify_results,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='consolidate',
    python_callable=consolidate_results,
    provide_context=True,
    dag=dag,
)

t1 >> t2
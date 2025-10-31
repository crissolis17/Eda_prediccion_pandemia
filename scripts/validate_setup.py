"""
Script de Validación Inicial del Proyecto
==========================================

Este script verifica que todo está configurado correctamente:
1. Los datasets existen y se pueden cargar
2. Las columnas esperadas están presentes
3. Los tipos de datos son correctos
4. Genera un reporte inicial de calidad

Ejecutar desde la raíz del proyecto con:
    python scripts/validate_setup.py
"""

import sys
import os
from pathlib import Path

# Añadir el src al path para poder importar
project_path = Path(__file__).resolve().parents[1]
sys.path.append(str(project_path / "src"))

import pandas as pd
import numpy as np
from datetime import datetime


def print_header(text):
    """Imprime un header bonito para organizar el output."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def check_file_exists(filepath):
    """Verifica si un archivo existe."""
    path = project_path / filepath
    if path.exists():
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"✅ Encontrado: {filepath} ({size_mb:.2f} MB)")
        return True
    else:
        print(f"❌ NO encontrado: {filepath}")
        return False


def load_and_inspect_dataset(filepath, name):
    """Carga un dataset y muestra información básica."""
    print(f"\n📊 Inspeccionando: {name}")
    print("-" * 50)

    try:
        # Cargar dataset
        df = pd.read_csv(
            project_path / filepath, nrows=1000
        )  # Solo primeras 1000 filas para velocidad

        print(f"Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
        print(f"\nPrimeras columnas: {', '.join(df.columns[:5].tolist())}...")
        print(f"\nTipos de datos únicos: {df.dtypes.value_counts().to_dict()}")

        # Verificar columna de fecha si existe
        if "date" in df.columns:
            print(f"\nRango de fechas:")
            try:
                df["date"] = pd.to_datetime(df["date"])
                print(f"  Desde: {df['date'].min()}")
                print(f"  Hasta: {df['date'].max()}")
            except Exception as e:
                print(f"  ⚠️  Error al parsear fechas: {e}")

        # Verificar columna de país
        if "country" in df.columns:
            n_countries = df["country"].nunique()
            print(f"\nNúmero de países: {n_countries}")
            if n_countries > 0:
                print(f"Ejemplos: {', '.join(df['country'].unique()[:5].tolist())}...")

        # Missing values
        missing_pct = (df.isnull().sum() / len(df) * 100).round(2)
        cols_with_missing = missing_pct[missing_pct > 0].sort_values(ascending=False)

        if len(cols_with_missing) > 0:
            print(f"\nColumnas con datos faltantes (top 5):")
            for col, pct in cols_with_missing.head(5).items():
                print(f"  {col}: {pct}%")
        else:
            print("\n✅ No hay datos faltantes!")

        return True, df

    except FileNotFoundError:
        print(f"❌ Error: Archivo no encontrado")
        return False, None
    except Exception as e:
        print(f"❌ Error al cargar: {str(e)}")
        return False, None


def validate_covid_compact(df):
    """Valida el dataset COVID Compact."""
    print("\n🔍 Validando COVID Compact...")

    required_cols = ["country", "date", "total_cases", "new_cases", "total_deaths"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        print(f"❌ Columnas requeridas faltantes: {missing_cols}")
        return False

    print("✅ Todas las columnas requeridas presentes")

    # Verificar que hay datos numéricos válidos
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    print(f"Columnas numéricas encontradas: {len(numeric_cols)}")

    return True


def validate_vaccination_global(df):
    """Valida el dataset de Vacunación Global."""
    print("\n🔍 Validando Vacunación Global...")

    required_cols = ["country", "date", "total_vaccinations", "people_vaccinated"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        print(f"❌ Columnas requeridas faltantes: {missing_cols}")
        return False

    print("✅ Todas las columnas requeridas presentes")
    return True


def main():
    """Función principal de validación."""
    print("\n" + "🚀 " * 20)
    print("VALIDACIÓN INICIAL DEL PROYECTO")
    print("Predicción de Preparación ante Pandemias")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🚀 " * 20)

    # =================================================================
    # 1. VERIFICAR QUE LOS ARCHIVOS EXISTEN
    # =================================================================
    print_header("1. Verificando Existencia de Archivos")

    datasets = {
        "covid_compact.csv": "data/01_raw/covid_compact.csv",
        "vaccination_global.csv": "data/01_raw/vaccination_global.csv",
        "vaccination_age.csv": "data/01_raw/vaccination_age.csv",
        "vaccination_manufacturer.csv": "data/01_raw/vaccination_manufacturer.csv",
    }

    files_ok = True
    for name, path in datasets.items():
        if not check_file_exists(path):
            files_ok = False

    if not files_ok:
        print("\n❌ Algunos archivos no se encontraron.")
        print("Por favor, copia tus datasets a la carpeta data/01_raw/")
        print("Los nombres deben ser exactamente:")
        for name in datasets.keys():
            print(f"  - {name}")
        sys.exit(1)

    # =================================================================
    # 2. INSPECCIONAR CADA DATASET
    # =================================================================
    print_header("2. Inspeccionando Datasets")

    results = {}

    # COVID Compact
    success, df_covid = load_and_inspect_dataset(
        "data/01_raw/covid_compact.csv", "COVID Compact"
    )
    results["covid"] = (success, df_covid)

    # Vaccination Global
    success, df_vacc = load_and_inspect_dataset(
        "data/01_raw/vaccination_global.csv", "Vacunación Global"
    )
    results["vaccination"] = (success, df_vacc)

    # Vaccination Age
    success, df_age = load_and_inspect_dataset(
        "data/01_raw/vaccination_age.csv", "Vacunación por Edad"
    )
    results["age"] = (success, df_age)

    # Vaccination Manufacturer
    success, df_manu = load_and_inspect_dataset(
        "data/01_raw/vaccination_manufacturer.csv", "Vacunación por Fabricante"
    )
    results["manufacturer"] = (success, df_manu)

    # =================================================================
    # 3. VALIDAR ESTRUCTURA DE DATOS
    # =================================================================
    print_header("3. Validando Estructura de Datos")

    validations_ok = True

    if results["covid"][0] and results["covid"][1] is not None:
        if not validate_covid_compact(results["covid"][1]):
            validations_ok = False

    if results["vaccination"][0] and results["vaccination"][1] is not None:
        if not validate_vaccination_global(results["vaccination"][1]):
            validations_ok = False

    # =================================================================
    # 4. REPORTE FINAL
    # =================================================================
    print_header("4. Reporte Final")

    total_datasets = len(results)
    successful_loads = sum(1 for success, _ in results.values() if success)

    print(f"\n📊 Datasets cargados exitosamente: {successful_loads}/{total_datasets}")

    if successful_loads == total_datasets and validations_ok:
        print("\n✅ ¡VALIDACIÓN EXITOSA!")
        print("Todos los datasets están listos para ser procesados.")
        print("\nPróximos pasos:")
        print("  1. Revisar el notebook 01_business_understanding.ipynb")
        print("  2. Crear el notebook 02_data_understanding.ipynb para EDA")
        print(
            "  3. Ejecutar el pipeline de validación con: kedro run --pipeline=validation"
        )
    else:
        print("\n⚠️  VALIDACIÓN PARCIAL")
        print("Algunos datasets tienen problemas. Revisa los mensajes arriba.")
        print("Corrige los problemas antes de continuar con los pipelines.")

    print("\n" + "=" * 70)
    print("Fin de la validación")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()

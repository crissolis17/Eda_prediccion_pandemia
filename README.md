# 🦠 Predicción y Preparación para Pandemias

Proyecto de Machine Learning para predecir y prepararse ante pandemias utilizando Kedro, Airflow y Docker.

## 📊 Descripción del Proyecto

Sistema de ML que analiza datos de COVID-19 para:
- **Clasificación**: Predecir niveles de riesgo de pandemia
- **Regresión**: Predecir casos nuevos basados en datos históricos

## 🏗️ Arquitectura
```
┌─────────────┐
│   Datasets  │ → 3+ fuentes de datos COVID-19
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Kedro    │ → Pipelines modulares (DE, DC, ML)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Airflow   │ → Orquestación de pipelines
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Docker    │ → Containerización
└─────────────┘
```

## 🛠️ Tecnologías

- **Python 3.12**
- **Kedro** - Framework para pipelines ML
- **Scikit-learn** - Machine Learning
- **Pandas** - Manipulación de datos
- **Apache Airflow** - Orquestación
- **Docker** - Containerización
- **DVC** - Versionado de datos (opcional)

## 📦 Modelos Implementados

### Clasificación (7 modelos)
- Logistic Regression
- Random Forest
- Gradient Boosting
- XGBoost
- LightGBM
- SVM
- KNN

### Regresión (7 modelos)
- Linear Regression
- Ridge
- Random Forest
- Gradient Boosting
- XGBoost
- LightGBM
- SVR

## 🚀 Instalación

### Requisitos
- Python 3.8+
- Docker Desktop
- Git

### Setup
```bash
# Clonar repositorio
git clone https://github.com/TU_USUARIO/prediccion-preparacion-pandemias.git
cd prediccion-preparacion-pandemias

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# Instalar Kedro
pip install kedro
```

## 📊 Datos

Los datasets utilizados incluyen:
- **OWID COVID-19 Data**: Datos globales de COVID-19
- **Vaccination Global**: Datos de vacunación mundial
- **COVID Compact**: Datos consolidados de la pandemia

**Nota**: Los datos no se incluyen en el repositorio. Descargar de las fuentes originales.

## ▶️ Ejecución

### Ejecutar Pipelines de Kedro
```bash
# Data Engineering
kedro run --pipeline=de

# Data Cleaning
kedro run --pipeline=dc

# Regresión
kedro run --pipeline=regression

# Clasificación
kedro run --pipeline=classification

# Todo completo
kedro run
```

### Visualizar Pipeline
```bash
kedro viz
```

### Ejecutar con Airflow
```bash
cd airflow-docker
docker-compose up -d

# Acceder a: http://localhost:8080
# Usuario: admin / Password: admin
```

## 📈 Resultados

Los resultados se guardan en:
- `data/07_reporting/classification_comparison_table.csv`
- `data/07_reporting/regression_comparison_table.csv`
- `data/07_reporting/FINAL_REPORT.md`

## 📂 Estructura del Proyecto
```
prediccion-preparacion-pandemias/
├── conf/                      # Configuración de Kedro
├── data/                      # Datos (no versionados)
├── notebooks/                 # Análisis exploratorio
├── src/                       # Código fuente
│   └── prediccion_preparacion_pandemias/
│       └── pipelines/         # Pipelines de Kedro
├── airflow-docker/            # Configuración de Airflow
├── requirements.txt           # Dependencias Python
└── README.md                  # Este archivo
```





## 🎓 Contexto Académico

**Asignatura**: Machine Learning (MLY0100)  
**Evaluación**: Parcial 2 (40%)  

**Fecha**: Octubre 2025
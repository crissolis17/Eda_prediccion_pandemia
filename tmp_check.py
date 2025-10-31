import pandas as pd
from pathlib import Path
p=Path('data/04_feature/classification_data.parquet')
print('exists:', p.exists(), 'size:', p.stat().st_size if p.exists() else None)
if p.exists():
    df=pd.read_parquet(p)
    print('shape:', df.shape)
    print('columns:', list(df.columns))
    col='total_deaths_per_million'
    print(col, 'in columns?', col in df.columns)
    if col in df.columns:
        print('non-null count:', df[col].notna().sum(), 'null count:', df[col].isna().sum())
        print('describe:', df[col].describe())

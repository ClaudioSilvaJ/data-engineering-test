import pandas as pd
import numpy as np
import os

tolerancia = 1e-5

def verify_data(df, df_ref):
    years = df_ref["Mes"]
    for year in years:
        for month_red, month_num in zip(df_ref.columns, range(1, 13)):
            value_ref = df_ref[df_ref["Mes"] == year][month_red].values[0]
            value_df = df.loc[(df["year_month"].dt.year == year) & (df["year_month"].dt.month == month_num), "volume"].sum()
            if np.isclose(value_ref, value_df, rtol=tolerancia):
                print(f"The values ​​are the same for {year}-{month_red}: {value_ref} ≈ {value_df}")
            else:
                raise ValueError(f"The values ​​do not match {year}-{month_red}: {value_ref} ≠ {value_df}")


def verify_main_data(file_path, file_path_ref, **kwargs):
    if(os.path.exists(file_path)):
        df = pd.read_csv(file_path, parse_dates=['year_month'])
    else:
        df = kwargs['ti'].xcom_pull(task_ids='transform_data')
    if(os.path.exists(file_path_ref)):
        df_ref = pd.read_csv(file_path_ref)
    else:
        df_ref = kwargs['ti'].xcom_pull(task_ids='extract_data')[2]
    verify_data(df, df_ref)
    return df

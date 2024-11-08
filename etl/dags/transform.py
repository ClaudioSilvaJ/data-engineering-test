import pandas as pd
import os

MESES_MAP = {
        'Jan': 'Jan', 'Fev': 'Feb', 'Mar': 'Mar', 'Abr': 'Apr', 
        'Mai': 'May', 'Jun': 'Jun', 'Jul': 'Jul', 'Ago': 'Aug', 
        'Set': 'Sep', 'Out': 'Oct', 'Nov': 'Nov', 'Dez': 'Dec'
    }

def transform_data(file_path, diesel_path, output_path, **kwargs):
    if(os.path.exists(file_path)):
        df_cons = pd.read_csv(file_path)
        df_diesel = pd.read_csv(diesel_path)
        df = read_and_obtain_diesel_extra_data(df_cons, df_diesel)
    else:
        result = kwargs['ti'].xcom_pull(task_ids='extract_data')
        df = read_and_obtain_diesel_extra_data(result[0], result[1])
    df_melted = df.melt(
        id_vars=["ANO", "COMBUSTÍVEL", "ESTADO", "UNIDADE"], 
        value_vars=list(MESES_MAP.keys()), 
        var_name="Mes", 
        value_name="Volume"
    )
    df_melted['Mes'] = df_melted['Mes'].map(MESES_MAP)
    df_melted['Ano_Mes'] = df_melted['ANO'].astype(int).astype(str) + '-' + df_melted['Mes']
    df_melted = df_melted.drop(columns=['Mes'])
    df_melted = df_melted[['Ano_Mes', 'ESTADO', 'COMBUSTÍVEL', 'UNIDADE', 'Volume']]
    df_melted.columns = ['year_month', 'uf', 'product', 'unit', 'volume']
    df_melted['year_month'] = pd.to_datetime(df_melted['year_month'], format='%Y-%b', errors='coerce')
    df_melted.fillna(0, inplace=True)
    df_melted['created_at'] = pd.Timestamp.now().timestamp()
    df_melted.to_csv(output_path, index=False)
    return df_melted

def read_and_obtain_diesel_extra_data(file_path, diesel_path):
    df = file_path
    df_diesel = diesel_path
    df = df[~((df["ANO"] >= df_diesel["ANO"].min()) & (df["COMBUSTÍVEL"].str.contains("DIESEL")))]
    df_concat = pd.concat([df, df_diesel], ignore_index=True)
    df_concat.sort_values(by=["ANO"], inplace=True)
    df_concat.reset_index(drop=True, inplace=True)
    return df_concat

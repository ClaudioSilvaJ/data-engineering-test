import pandas as pd
import numpy as np
from openpyxl import load_workbook
from openpyxl.pivot.fields import Missing

original_data_coluns = "B:W"
original_data_header = 52

def extract_original_data(file, cols, header):
    df = pd.read_excel(file, usecols=cols, header=header).iloc[:13, ]
    df.loc[:, df.columns != 'Dados'] = df.loc[:, df.columns != 'Dados'].apply(pd.to_numeric, errors='coerce')
    df['Dados'] = df['Dados'].str.slice(0, 3)
    df = df.fillna(0)
    df = df.set_index('Dados').T
    years = df.index.to_list()
    years = [int(year) for year in years]
    df['Mes'] = years
    df = df.reset_index(drop=True)
    return df

def load_excel_data(file_path):
    workbook = load_workbook(file_path)
    worksheet = workbook['Plan1']
    pivot_names = [p.name for p in worksheet._pivots]
    pivot_names.sort()
    dict_list = []
    for pivot_name in pivot_names:
        pivot_table = [p for p in worksheet._pivots if p.name == pivot_name][0]
        fields_map = get_fields_map(pivot_table)
        column_names = [field.name for field in pivot_table.cache.cacheFields]
        rows = get_data_rows(pivot_table, fields_map, column_names)
        dict_list.append(pd.DataFrame.from_dict(rows))
    
    dict_list = remove_duplicates_dataframes(dict_list)
    dict_list = dict_list[:2] # Only the main and diesel cache tables are of interest
    return dict_list


def get_fields_map(pivot_table):
    fields_map = {}
    for field in pivot_table.cache.cacheFields:
        if field.sharedItems.count > 0:
            fields_map[field.name] = [f.v for f in field.sharedItems._fields]
    return fields_map

def get_data_rows(pivot_table, fields_map, column_names):
    rows = []
    for record in pivot_table.cache.records.r:
        record_values = [
            field.v if not isinstance(field, Missing) else np.nan for field in record._fields
        ]
        row_dict = {k: v for k, v in zip(column_names, record_values)}
        for key in fields_map:
            row_dict[key] = fields_map[key][row_dict[key]]
        rows.append(row_dict)
    return rows

def remove_duplicates_dataframes(dict_list):
    unique_dfs = []
    unique_representations = set()
    for df in dict_list:
        df_sorted = df.sort_index(axis=1).sort_values(by=list(df.columns))
        representation = df_sorted.to_string(index=False)
        if representation not in unique_representations:
            unique_representations.add(representation)
            unique_dfs.append(df)
    return unique_dfs

def extract_task(file_path, output_original_path, output_diesel_path, ref_path):
    df_ref = extract_original_data(file_path, original_data_coluns, original_data_header)
    list_df = load_excel_data(file_path)
    df_original = list_df[0]
    df_diesel = list_df[1]
    list_df.append(df_ref)
    df_original.to_csv(output_original_path, index=False)
    df_diesel.to_csv(output_diesel_path, index=False)
    df_ref.to_csv(ref_path, index=False)
    return list_df



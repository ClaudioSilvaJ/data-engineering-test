import pandas as pd
from sqlalchemy import create_engine
import os

def insert_data_to_sqlite(csv_file: str, db_url: str, **kwargs):
    try:
        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file)
        else:
            df = kwargs['ti'].xcom_pull(task_ids='transform_data')
        df['year_month'] = pd.to_datetime(df['year_month'])
        df['created_at'] = pd.to_datetime(df['created_at'], unit='s')
        engine = create_engine(db_url, echo=True)
        with engine.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS fuel_sales")
            conn.execute("""
                CREATE TABLE fuel_sales (
                    year_month DATE,
                    uf VARCHAR(50),
                    product VARCHAR(50),
                    unit VARCHAR(50),
                    volume FLOAT,
                    created_at TIMESTAMP
                )
            """)
            batch_size = 10000
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                values = [tuple(x) for x in batch_df.to_numpy()]
                conn.execute(
                    "INSERT INTO fuel_sales (year_month, uf, product, unit, volume, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
                    values
                )
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        raise 
-# duckdb

import time
import duckdb
import pyarrow as pa
import pandas as pd
from sqlalchemy import create_engine
import datetime
import os

from creds import userdb, passworddb

row_group_size = 10000
table_name = "your table name to query"
columns_list = "your column list to query from your db"

# DuckDB Connection
duckdb_conn = duckdb.connect()

def main():
    start_time = time.time()
    schema = generate_schema()
    output_folder = r'C:\your_path_goes_here'
    total_partitions = 0

    dst_mssql_engine = create_engine(f"mssql+pyodbc://{userdb}:{passworddb}@your_odbc_name")
    query_min_max = f"SELECT MIN([Date Field]) AS min_date, MAX([Date Field]) AS max_date FROM {table_name}"
    min_max_df = pd.read_sql(query_min_max, dst_mssql_engine)
    min_date = min_max_df['min_date'][0]
    max_date = min_max_df['max_date'][0]
    current_date = min_date

    while current_date <= max_date:
        year_month = current_date.strftime('%Y-%m')
        output_file_path = os.path.join(output_folder, f"{year_month}.parquet")

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        df = get_data(table_name, year_month)
        if len(df) > 0:
            total_partitions += 1
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            copy_to_duckdb(table)
            export_to_parquet_file('temp_table', output_file_path)

        current_date += pd.DateOffset(months=1)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time: {execution_time:.2f} seconds")
    print(f"Total partitions: {total_partitions}")

def get_data(table_name, year_month):
    dst_mssql_engine = create_engine(f"mssql+pyodbc://{userdb}:{passworddb}@your_odbc_name")

    start_date = pd.to_datetime(f"{year_month}-01")
    end_date = start_date + pd.DateOffset(months=1)

    query_read = f"SELECT {columns_list} FROM {table_name} WHERE [Date Field] >= '{start_date}' AND [Date Field] < '{end_date}'"
    df = pd.read_sql(query_read, dst_mssql_engine)

    return df

def generate_schema():
    dst_mssql_engine = create_engine(f"mssql+pyodbc://{userdb}:{passworddb}@your_odbc_name")

    query_generate = f"SELECT TOP 1 {columns_list} FROM {table_name}"
    df = pd.read_sql(query_generate, dst_mssql_engine)
    fields = []

    for column_name, column_type in df.dtypes.items():
        if column_type == 'datetime64[ns]':
            field_type = pa.timestamp('ms', tz=None)  # Specify the timezone as None
        elif column_type == 'object':
            field_type = pa.string()
        else:
            field_type = pa.from_numpy_dtype(column_type)

        fields.append((column_name, field_type))

    return pa.schema(fields)

def copy_to_duckdb(table):
    duckdb_conn.register('temp_table', table)

def export_to_parquet_file(table_name, file_path):
    query = f"COPY (SELECT * FROM {table_name}) TO '{file_path}' (FORMAT 'parquet')"
    duckdb_conn.execute(query)

if __name__ == '__main__':
    main()

import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine
import datetime

from creds import userdb, passworddb

def main():
    df = get_data()
    schema = generate_schema(df)
    row_group_size = 10000
    output_file_path = r'C:\your_path_goes_here\file.parquet'

    with pq.ParquetWriter(output_file_path, schema) as writer:
        start_row = 0

        while start_row < len(df):
            row_group_data = df.iloc[start_row:start_row+row_group_size]

            table = pa.Table.from_pandas(row_group_data, schema=schema)
            writer.write_table(table)

            start_row += row_group_size

def get_data():
    dst_mssql_engine = create_engine(f"mssql+pyodbc://{userdb}:{passworddb}@your_odbc_name")
    table_name = "core.nav_item"

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, dst_mssql_engine)

    return df

def generate_schema(df):
    fields = []
    for column_name, column_type in df.dtypes.items():
        if column_type == 'datetime64[ns]':
            field_type = pa.timestamp('ms', tz=None)
        elif column_type == 'object': 
            df[column_name] = df[column_name].astype(str).str.encode('utf-8')
            field_type = pa.string()
        elif column_type == 'object':
            field_type = pa.string()
        else:
            field_type = pa.from_numpy_dtype(column_type)

        fields.append((column_name, field_type))

    return pa.schema(fields)

if __name__ == '__main__':
    main()
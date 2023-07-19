---
title: "Two approaches to generate Parquet files from on-prem databases"
date: 2023-07-18T11:30:03+00:00
weight: 2
#aliases: ["/fabric"]
tags: ["fabric"]
author: "Riccardo Capelli"
# author: ["Me", "You"] # multiple authors
showToc: true
TocOpen: false
draft: false
hidemeta: false
comments: false
description: "Exporting Parquet files using Pandas, Apache Arrows, Duckdb"
canonicalURL: "https://canonical.url/to/page"
disableHLJS: true # to disable highlightjs
disableShare: false
disableHLJS: false
hideSummary: false
searchHidden: true
ShowReadingTime: true
ShowBreadCrumbs: true
ShowPostNavLinks: true
ShowWordCount: true
ShowRssButtonInSectionTermList: true
UseHugoToc: true
cover:
    image: "<image path/url>" # image path/url
    alt: "<alt text>" # alt text
    caption: "<text>" # display caption under cover
    relative: false # when using page bundles set this to true
    hidden: true # only hide on current single page
editPost:
    URL: "https://github.com/<path_to_repo>/content"
    Text: "Suggest Changes" # edit text
    appendFilePath: true # to append file path to Edit link
---
In this guide, firstly I will share my experience trying several approaches to create Parquet files from SQL Server tables, then we'll explore in detail how to efficiently extract and convert data using PyArrow.

All my attemps with the code is available here: https://github.com/Riccardocapelli1/my_blog/tree/main/python

### My experience

It took me several attempts to define the approach that suited me better. Here are mine to generat Parquet files:

1. without clustering the output
2. clustering results with the standard Apache Arrow library
3. clustering results with concurrent threads (using ThreadPoolExecutor() from concurrent) 
4. clustering result with with DuckDB

### Without clustering the output
It is effective with small tables (less than 2/3GB size). RAM and CPUs of your workstation won't get stressed for too long. It fits perfectly dimension table (i.e. items, vendors, location) of small size.

### Clustering result with the standard apache arrow library
The script result pretty effective with bigger tables (>5GB). It clusters effectively the resultset by providing a field (generally dates fit well) and distribut the workload without stessing RAM and CPUs. It can be done by a single workstation (I tried with a 25GB table). 

### Clustering result with concurrent threads (using ThreadPoolExecutor() from concurrent) 
I expected the outcome to improve performance while converting data to the Parquet format, but that wasn't the case. Unfortunately, I didn't have luck.

### Clustering result with DuckDB
Despite loving DuckDB, the performance improvement was very little overall (6 seconds), and the concurrent threads isn't exploited. I noticed that the Parquet file size was slightly bigger.


### Overview of the Code:

The provided Python script demonstrates a method to extract data from a facts table on Microsoft SQL Server database, cluster the result based on a date column that distributes equally over the rows (more or less), and then store it as Parquet files using PyArrow. Here's a step-by-step explanation of the script's technical aspects:

Importing Dependencies: The script begins by importing essential libraries like time, pyarrow, pandas, sqlalchemy, datetime, tqdm, and os.

Credentials and Configuration: The script imports the userdb and passworddb from a separate creds.py file. These credentials are used to create a connection with the Microsoft SQL Server database using the create_engine function from sqlalchemy. Note that importing credential from an unecrypted source is discouraged in production-ready environment for security reasons.

Schema Generation (generate_schema): This function retrieves a sample row from the database table specified in table_name and uses it to generate the Parquet schema. The schema defines the data types for each column, considering various data types, including datetime and object types.

Data Extraction and Parquet Conversion (main): The main function is responsible for the entire process of data extraction, conversion, and storage as Parquet files. It performs the following steps:

1. Fetches the minimum and maximum date from the database table to determine the range of data to process.
2. Iterates over each month's data, starting from the minimum date to the maximum date.
3. For each month, it queries the database for the data within that specific month and stores it in a pandas DataFrame.
4. If the DataFrame is not empty, it converts it to a PyArrow table and writes it to a Parquet file named by the year and month.
5. Data Retrieval (get_data): This function queries the database for data within a specific year and month, as determined by the year_month parameter.

### Pros of the Approach:

Columnar Storage: Parquet files store data in a columnar format, which significantly reduces disk I/O and improves query performance for analytical workloads.

Compression Efficiency: Parquet's compression algorithms result in smaller file sizes, reducing storage costs and improving data transfer speeds.

Optimized for Analytics: Parquet is optimized for analytics, making it an ideal choice for large-scale data processing and analytics workloads.

Incremental Processing: The script efficiently processes data on a monthly basis, allowing for incremental updates, which is useful for handling large datasets.

Compatibility: Parquet files are compatible with various data processing frameworks like Apache Spark and Hadoop.

### Cons:

Read-only Format: Parquet files are designed for efficient read operations, but updates and deletions can be challenging and require a full rewrite of the file. To overcome this problem, I will explain how to do this in the most efficient way.

Schema Evolution: Modifying the schema of existing Parquet files can be complex and might require data migration.

Remember to tailor this code to your specific needs and database configuration. Always consider the pros and cons before implementing any data extraction and storage approach.

### The script: clustering results with the standard Apache Arrow library

 ```py
import time
import pyarrow.Parquet as pq
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine
import datetime
from tqdm import tqdm
import os

from creds import userdb, passworddb
row_group_size = 10000
table_name = "your table name to query"
columns_list = "your column list to query from your db"

def main():
    start_time = time.time()
    schema = generate_schema()
    output_folder = r'C:\your_path_goes_here'
    total_partitions = 0

    dst_mssql_engine = create_engine(f"mssql+pyodbc://{userdb}:{passworddb}@NavTest")
    query_min_max = f"SELECT MIN([Date Field]) AS min_date, MAX([Date Field]) AS max_date FROM {table_name}"
    min_max_df = pd.read_sql(query_min_max, dst_mssql_engine)
    min_date = min_max_df['min_date'][0]
    max_date = min_max_df['max_date'][0]
    current_date = min_date

    while current_date <= max_date:
        year_month = current_date.strftime('%Y-%m')
        output_file_path = os.path.join(output_folder, f"{year_month}.Parquet")

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        df = get_data(table_name, year_month)
        if len(df) > 0:
            total_partitions += 1
            with pq.ParquetWriter(output_file_path, schema) as writer:
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
                writer.write_table(table)

        current_date += pd.DateOffset(months=1)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time: {execution_time:.2f} seconds")
    print(f"Total partitions: {total_partitions}")

def get_data(table_name, year_month):
    dst_mssql_engine = create_engine(f"mssql+pyodbc://{userdb}:{passworddb}@NavTest")

    start_date = pd.to_datetime(f"{year_month}-01")
    end_date = start_date + pd.DateOffset(months=1)

    query_read = f"SELECT {columns_list} FROM {table_name} WHERE [Date Field] >= '{start_date}' AND [Date Field] < '{end_date}'"
    df = pd.read_sql(query_read, dst_mssql_engine)

    return df

def generate_schema():
    dst_mssql_engine = create_engine(f"mssql+pyodbc://{userdb}:{passworddb}@NavTest")
    
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

if __name__ == '__main__':
    main()
 ```
Happy coding and data engineering! 🚀💻
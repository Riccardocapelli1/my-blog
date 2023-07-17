---
title: "The fourth way: Ingesting data in Fabric using Parquet files"
date: 2023-07-17T11:30:03+00:00
weight: 2
#aliases: ["/fabric"]
tags: ["fabric"]
author: "Me"
# author: ["Me", "You"] # multiple authors
showToc: true
TocOpen: false
draft: false
hidemeta: false
comments: false
description: "Load Data to Ms Fabric Local SQL Server"
canonicalURL: "https://canonical.url/fabric/"
disableHLJS: true # to disable highlightjs
disableShare: false
disableHLJS: false
hideSummary: false
searchHidden: true
ShowReadingTime: true
ShowBreadCrumbs: true
ShowPostNavLinks: true
ShowWordCount: true
ShowRssButtonInSectionTermList: false
UseHugoToc: true
cover:
    image: "<image path/url>" # image path/url
    alt: "<alt text>" # alt text
    caption: "<text>" # display caption under cover
    relative: false # when using page bundles set this to true
    hidden: true # only hide on current single page
editPost:
    URL: "https://github.com/Riccardocapelli1/data-stack-wall/issues"
    Text: "Suggest Changes" # edit text
    appendFilePath: true # to append file path to Edit link
---
### Introduction

Welcome to my new blog, where I share my experience over data and analytics engineering and technologies. Today, I'm going to show you a fourth approach to load on-prem data onto Microsoft Fabric Lakehouse other than the standard one provided:
1. Dataflow Gen2 Pipelines
2. Data Pipelines
3. Jupiter Notebooks

I show you how to use python to migrate a Sql Server database unexposed to the global network. This is a great way to leverage the power of OneLake access to file explorer, ingesting and loading data on the Fabric datawarehouse.

### What are parquet files and why are they useful?

Parquet files are a columnar storage format that allows for efficient compression and encoding of data. They are especially suited for analytical queries, as they can reduce the amount of data scanned and improve performance. Parquet files are also compatible with many data processing frameworks, such as Spark, Hive, and Presto.

Parquet files can be are generated with little resources (a powerful workstation can fit the workload). The script I wrote is the beginning and an exploratory approach to this new Microsoft product. You can find the script here: https://github.com/Riccardocapelli1/my_blog/tree/main/python 

### How to load data onto Microsoft Fabric using parquet files?

The process defined in the scripts consists of three main steps:

1. Extract data from the local Sql Server database to a Pandas dataframe.
2. Convert the dataframe to a parquet files.
3. write the parquet file into a folder.


To load data into Microsoft Fabric using Parquet files from a local SQL Server database, we will use Python with the pyarrow, pandas, and sqlalchemy libraries. This method ensures that the data remains unexposed to the global network, providing enhanced security. Additionally, we will leverage the OneLake access function with file explorer, making the process efficient and straightforward.

#### Prerequisites
Before proceeding, ensure that you have the following prerequisites in place:

#### Python 
installed on your system. You can download Python from the official website: python.org.
Required Python Packages: pyarrow, pandas, sqlalchemy, and tqdm. Install them using pip:

 ```py
pip install pyarrow 
pip install pandas 
pip install sqlalchemy 
pip install tqdm
 ```

#### A SQL Server instance 
hosting a database containing the data you want to load into Microsoft Fabric.
Microsoft Fabric is at the moment I write this post in trial-only access.

#### The Python Script
Below is the Python script to load data into Microsoft Fabric using Parquet files:

 ```py
Copy code
import time
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine

from creds import userdb, passworddb
row_group_size = 5000
table_name = "your_table_name_to_query"
columns_list = "your_column_list_to_query_from_your_db"

# Rest of the script...
 ```

Replace the placeholders with your specific details:

your_table_name_to_query: The name of the table you want to query from your database.
your_column_list_to_query_from_your_db: The list of columns you want to query from your database.

### Conclusion
In this guide, we introduced a new exploratory approach to load data into Microsoft Fabric using Parquet files from a local SQL Server database (I tried with a 25GB table with very good performances). By following these steps and leveraging Python libraries, you can ensure secure data loading without exposing your data to the global network. The use of Parquet files and the OneLake access function with file explorer provides an efficient and robust solution for your data loading needs. Moreover, in case computational cost are added to converting SQL Server tables to Parquet files, that's dodged by doing it in local environment.

This is the first blog of a series, where I will describe further how to explore and customize the script according to your specific requirements, how I would make them work and orchestrate in a local/hybrid data architecture. 
Happy data loading!
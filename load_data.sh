#!/bin/bash

# Generate CSV files
python3 ./pythonFiles/generate_customers.py
python3 ./pythonFiles/generate_products.py
python3 ./pythonFiles/generate_orders.py

# Create HDFS directory and upload files
hdfs dfs -mkdir -p /user/itv017008/ecommerce
hdfs dfs -put customers.csv /user/itv017008/ecommerce/
hdfs dfs -put products.csv /user/itv017008/ecommerce/
hdfs dfs -put orders.csv /user/itv017008/ecommerce/

# Change permissions to 777 (rwxrwxrwx)
hdfs dfs -chmod 777 /user/itv017008/ecommerce/*.csv

# Run SQL file to create tables and load data
hive -f ./sql_files/create_tables.sql
# eCommerce Data Pipeline Project

This project demonstrates a complete data engineering pipeline for an eCommerce scenario using Python, Hive, and Spark on the ITversity platform. It includes data generation, storage in HDFS, table creation in Hive, and data analysis using Spark. The project is designed for beginners to practice key data engineering concepts and tools.

---

## Objectives

- Generate sample data for customers, products, and orders using Python.
- Create Hive tables to store the generated data, skipping header rows.
- Automate the process of generating data, uploading to HDFS, and loading into Hive using a shell script.
- Perform data cleaning and transformations using Spark to derive insights such as top customers, best-selling products, and more.
- Automate the Spark job execution and verify its success using another shell script.

---

## Tools and Technologies

- **Python 3.x**: For data generation and Spark transformations.
- **Apache Hive**: For table creation and data storage.
- **Apache Spark (PySpark)**: For data cleaning and analysis.
- **Hadoop HDFS**: For distributed storage of CSV files.
- **Bash**: For automating tasks via shell scripts.
- **ITversity Platform**: Execution environment (can be adapted to other Hadoop/Spark setups).

---

## Project Structure

- **`generate_customers.py`**: Generates `customers.csv` with 100 customer records.
- **`generate_products.py`**: Generates `products.csv` with 100 product records.
- **`generate_orders.py`**: Generates `orders.csv` with 100 order records, linking to customers and products.
- **`create_tables.sql`**: SQL script to create Hive tables and load data from HDFS.
- **`load_data.sh`**: Shell script to generate CSVs, upload to HDFS, set permissions, and load into Hive.
- **`spark_analysis.py`**: PySpark script for data cleaning and transformations to derive insights.
- **`run_analysis.sh`**: Shell script to run the Spark job and check for success.
- **`README.md`**: This file.

---

## Setup Instructions

### Prerequisites

- Access to the ITversity platform or a similar Hadoop/Spark environment.
- Python 3.x installed.
- Necessary Python libraries: `faker`, `pyspark`.
- Hive and HDFS configured and accessible.

*Note: If running outside the ITversity platform, ensure you have a Hadoop cluster with Hive and Spark installed and properly configured.*

### Installation

1. Clone the repository to your local machine or ITversity environment:
   ```bash
   git clone https://github.com/karthickpalanivel/eCommerce_analysis.git


2. Navigate to the project directory:
    cd ecommerce-data-pipeline

3. Install required Python libraries (if not already installed):
    pip install faker pyspark


## Make the shell scripts executable
    chmod +x load_data.sh run_analysis.sh

## Generate data and load into Hive
    ./load_data.sh

## Run the Spark analysis
    ./run_analysis.sh

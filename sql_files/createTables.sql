-- Customers table
CREATE TABLE customers (
    customer_id STRING,
    name STRING,
    phone STRING,
    address STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- Products table
CREATE TABLE products (
    product_id STRING,
    product_name STRING,
    product_category STRING,
    product_price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");


-- Orders table
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    products STRING,
    total_order_amount DOUBLE,
    order_date STRING,
    returned STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/itv017008/ecommerce/customers.csv' INTO TABLE customers;
LOAD DATA INPATH '/user/itv017008/ecommerce/products.csv' INTO TABLE products;
LOAD DATA INPATH '/user/itv017008/ecommerce/orders.csv' INTO TABLE orders;
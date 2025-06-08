import csv
import random

categories = ['Electronics', 'Clothing', 'Books', 'Home']
with open('products.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['product_id', 'product_name', 'product_category', 'product_price'])
    for i in range(1, 101):  # 100 rows
        product_id = f'p{i}'
        product_name = f'Product {i}'
        category = random.choice(categories)
        price = round(random.uniform(5.0, 100.0), 2)
        writer.writerow([product_id, product_name, category, price])
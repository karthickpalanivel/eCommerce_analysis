import csv
import random
import json
from datetime import datetime, timedelta

# Read products.csv to get product IDs and prices
products = {}
with open('products.csv', 'r') as f:
    reader = csv.DictReader(f)
    
    for row in reader:
        products[row['product_id']] = float(row['product_price'])

with open('orders.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['order_id', 'customer_id', 'products', 'total_order_amount', 'date', 'returned'])
    
    for i in range(1, 101):  # 100 rows
        order_id = str(i)
        customer_id = str(random.randint(1, 100))  # Assuming customer_ids are 1-100
        num_items = random.randint(1, 5)  # 1-5 products per order
        order_items = []
        total_amount = 0.0
        
        for _ in range(num_items):
            product_id = random.choice(list(products.keys()))
            quantity = random.randint(1, 10)
            total_amount += products[product_id] * quantity
            order_items.append({'product_id': product_id, 'quantity': quantity})
        
        products_json = json.dumps(order_items)
        date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%M-%d')
        returned = random.choice(['True', 'False'])
        writer.writerow([order_id, customer_id, products_json, round(total_amount, 2), date, returned])
import csv
import random
from faker import Faker # type: ignore

fake = Faker()
with open('customers.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['customer_id', 'name', 'phone', 'address'])
    for i in range(1, 101):  # 100 rows
        customer_id = str(i)
        name = fake.name()
        phone = fake.phone_number()
        address = f'[{fake.building_number()}, {fake.street_name()}, {fake.city()}, {fake.postcode()}]'
        writer.writerow([customer_id, name, phone, address])
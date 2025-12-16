from pymongo import MongoClient
from datetime import datetime
import random

# Mongo connection
client = MongoClient("mongodb://localhost:27017/?directConnection=true")
db = client["ecom"]
products = db.products

categories = ["clothing", "home", "electronics", "sports", "beauty"]

product_names = [
    "T-shirt", "Mug", "Shoes", "Backpack", "Headphones",
    "Bottle", "Notebook", "Lamp", "Watch", "Sunglasses"
]

docs = []

for i in range(1, 201):
    doc = {
        "product_id": i,
        "name": random.choice(product_names),
        "category": random.choice(categories),
        "price": round(random.uniform(5, 150), 2),
        "created_at": datetime.utcnow()
    }
    docs.append(doc)

products.insert_many(docs)

print(f"Inserted {len(docs)} product records into MongoDB.")

import csv
import random
from datetime import date, timedelta

# Dataset size and output location
OUTPUT_FILE = "sales_data.csv"
NUM_ROWS = 1_000_000

# Date range used for synthetic transactions
START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 12, 31)

# Domain sizes and product category values used in the generated data
NUM_STORES = 50
NUM_PRODUCTS = 1000
CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home",
    "Beauty",
    "Sports",
    "Toys",
    "Books",
    "Grocery"
]

random.seed(551)


def random_date(start_date, end_date):
    """Return a random date within the provided inclusive date range"""
    delta_days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, delta_days))


def weighted_category():
    """Select a product category using predefined sales distribution weights"""
    return random.choices(
        CATEGORIES,
        weights=[18, 16, 14, 10, 12, 8, 7, 15],
        k=1
    )[0]


def category_price_range(category):
    """Return the unit price range assigned to a product category"""
    ranges = {
        "Electronics": (80, 1500),
        "Clothing": (10, 200),
        "Home": (20, 500),
        "Beauty": (8, 150),
        "Sports": (15, 400),
        "Toys": (5, 120),
        "Books": (5, 60),
        "Grocery": (2, 40)
    }
    return ranges[category]


def generate_row(i):
    """Generate one synthetic sales transaction row"""
    transaction_date = random_date(START_DATE, END_DATE).isoformat()
    store_id = random.randint(1, NUM_STORES)
    product_id = random.randint(1, NUM_PRODUCTS)
    category = weighted_category()
    quantity = random.randint(1, 8)

    low, high = category_price_range(category)
    unit_price = round(random.uniform(low, high), 2)
    sales_amount = round(unit_price * quantity, 2)

    return [
        i,
        transaction_date,
        store_id,
        product_id,
        category,
        quantity,
        unit_price,
        sales_amount
    ]


# Stream rows directly to avoid keeping the full dataset in memory
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "transaction_id",
        "transaction_date",
        "store_id",
        "product_id",
        "category",
        "quantity",
        "unit_price",
        "sales_amount"
    ])

    for i in range(1, NUM_ROWS + 1):
        writer.writerow(generate_row(i))

print("Dataset generated")

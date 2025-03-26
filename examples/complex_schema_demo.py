import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path.cwd()))

from src.beardantic.schema import parse_yaml_schema, validate_dataframe

# Path to the complex schema YAML file
schema_path = Path(__file__).parent / "complex_schema_example.yaml"

# Parse the schema from the YAML file
dataset_schema = parse_yaml_schema(schema_path)
print(f"Loaded schema for dataset: {dataset_schema.name}")
print(f"Dataset description: {dataset_schema.description}")
print(f"Number of tables: {len(dataset_schema.tables)}")

# Get the schema for the products table
products_schema = next(
    table for table in dataset_schema.tables if table.name == "products"
)
print(f"\nTable: {products_schema.name}")
print(f"Description: {products_schema.description}")
print("Columns:")
for col in products_schema.columns:
    print(f"  - {col.name}: {col.type}")
    if col.type == "struct" and col.fields:
        print("    Struct fields:")
        for field in col.fields:
            print(f"      - {field.name}: {field.type}")
            if field.type == "struct" and field.fields:
                print("        Nested fields:")
                for nested_field in field.fields:
                    print(f"          - {nested_field.name}: {nested_field.type}")
    elif col.type == "list":
        print(f"    List of {col.element_type}")
        if col.element_type == "struct" and col.fields:
            print("    Struct fields in list:")
            for field in col.fields:
                print(f"      - {field.name}: {field.type}")

# Create a sample DataFrame with nested structures
products_df = pl.DataFrame(
    [
        {
            "id": 1,
            "name": "Ergonomic Chair",
            "price": 299.99,
            "attributes": {
                "color": "Black",
                "weight": 15.5,
                "dimensions": {"length": 60.0, "width": 60.0, "height": 120.0},
            },
            "tags": ["office", "furniture", "ergonomic"],
            "variants": [
                {"sku": "EC-BLK-001", "stock": 25, "price_adjustment": 0.0},
                {"sku": "EC-RED-001", "stock": 10, "price_adjustment": 10.0},
            ],
        },
        {
            "id": 2,
            "name": "Standing Desk",
            "price": 499.99,
            "attributes": {
                "color": "White",
                "weight": 45.0,
                "dimensions": {"length": 160.0, "width": 80.0, "height": 110.0},
            },
            "tags": ["office", "furniture", "standing"],
            "variants": [
                {"sku": "SD-WHT-001", "stock": 15, "price_adjustment": 0.0},
                {"sku": "SD-BLK-001", "stock": 5, "price_adjustment": 0.0},
            ],
        },
    ]
)

print("\nSample Products DataFrame:")
print(products_df)

# Get the schema for the orders table
orders_schema = next(table for table in dataset_schema.tables if table.name == "orders")
print(f"\nTable: {orders_schema.name}")
print(f"Description: {orders_schema.description}")
print("Columns:")
for col in orders_schema.columns:
    print(f"  - {col.name}: {col.type}")
    if col.type == "struct" and col.fields:
        print("    Struct fields:")
        for field in col.fields:
            print(f"      - {field.name}: {field.type}")
    elif col.type == "list":
        print(f"    List of {col.element_type}")
        if col.element_type == "struct" and col.fields:
            print("    Struct fields in list:")
            for field in col.fields:
                print(f"      - {field.name}: {field.type}")
                if (
                    field.type == "list"
                    and field.element_type == "struct"
                    and field.fields
                ):
                    print(f"        Nested list of {field.element_type}:")
                    for nested_field in field.fields:
                        print(f"          - {nested_field.name}: {nested_field.type}")

# Create a sample orders DataFrame with nested structures
# Create orders DataFrame with proper datetime type
orders_data = [
    {
        "order_id": "ORD-12345",
        "customer_id": 1001,
        "order_date": "2025-03-26 10:30:00",
        "shipping_address": {
            "street": "123 Main St",
            "city": "San Francisco",
            "state": "CA",
            "postal_code": "94105",
            "country": "USA",
        },
        "line_items": [
            {
                "product_id": 1,
                "quantity": 2,
                "unit_price": 299.99,
                "discounts": [{"code": "SPRING25", "amount": 25.0}],
            },
            {"product_id": 2, "quantity": 1, "unit_price": 499.99, "discounts": []},
        ],
        "payment_info": {
            "method": "credit_card",
            "status": "completed",
            "transaction_id": "TXN-987654",
        },
    },
    {
        "order_id": "ORD-67890",
        "customer_id": 1002,
        "order_date": "2025-03-26 14:45:00",
        "shipping_address": {
            "street": "456 Oak Ave",
            "city": "Seattle",
            "state": "WA",
            "postal_code": "98101",
            "country": "USA",
        },
        "line_items": [
            {
                "product_id": 1,
                "quantity": 1,
                "unit_price": 299.99,
                "discounts": [{"code": "WELCOME10", "amount": 10.0}],
            }
        ],
        "payment_info": {
            "method": "paypal",
            "status": "completed",
            "transaction_id": "PP-123456",
        },
    },
]

# Convert the order_date column to datetime type
orders_df = pl.DataFrame(orders_data).with_columns(
    pl.col("order_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
)

print("\nSample Orders DataFrame:")
print(orders_df)

# Demonstrate validation with nested structures
print("\nValidating Products DataFrame...")
validation_errors = validate_dataframe(products_df, products_schema)
if validation_errors:
    print("Validation errors:")
    for error in validation_errors:
        print(f"  - {error}")
else:
    print("Products DataFrame is valid according to the schema!")

print("\nValidating Orders DataFrame...")
validation_errors = validate_dataframe(orders_df, orders_schema)
if validation_errors:
    print("Validation errors:")
    for error in validation_errors:
        print(f"  - {error}")
else:
    print("Orders DataFrame is valid according to the schema!")

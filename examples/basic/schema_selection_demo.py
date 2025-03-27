"""
Example demonstrating the new schema selection and conversion features.

This example shows how to:
1. Parse a YAML schema
2. Select a specific table schema using the select() method
3. Convert the schema to a dictionary or Polars Schema object for use with Polars DataFrame
"""

import logging
from pathlib import Path
import polars as pl

from beardantic import (
    configure_logging,
    parse_yaml_schema,
)

# Configure logging to show debug messages
configure_logging(level=logging.DEBUG)

# Path to the schema file
schema_path = Path(__file__).parent.parent / "schemas" / "schema_example.yaml"

# Parse the schema
print(f"Loading schema from {schema_path}")
dataset_schema = parse_yaml_schema(schema_path)
print(f"Loaded schema for dataset: {dataset_schema.name}")
print(f"Dataset description: {dataset_schema.description}")
print(f"Number of tables: {len(dataset_schema.tables)}")
print()

# Select a specific table schema
users_schema = dataset_schema.select("users")
print(f"Selected table: {users_schema.name}")
print(f"Table description: {users_schema.description}")
print(f"Number of columns: {len(users_schema.columns)}")
print()

# Display column information
print("Columns:")
for col in users_schema.columns:
    print(f"  - {col.name}: {col.type} (nullable: {col.nullable})")
print()

# Create a sample DataFrame with the schema
data = [
    {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30, "active": True},
    {"id": 2, "name": "Jane Smith", "email": None, "age": 25, "active": False},
    {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": None, "active": True},
]

# Method 1: Create DataFrame with schema dictionary from to_dict()
print("Method 1: Creating DataFrame with to_dict()")
schema_dict = users_schema.to_dict()
print(f"Schema dictionary: {schema_dict}")
df1 = pl.DataFrame(data, schema=schema_dict)
print(df1)
print()

# Method 2: Create DataFrame with pl.Schema from to_polars_schema()
print("Method 2: Creating DataFrame with to_polars_schema()")
schema_polars = users_schema.to_polars_schema()
print(f"Polars schema: {schema_polars}")
df2 = pl.DataFrame(data, schema=schema_polars)
print(df2)
print()

# Try selecting a non-existent table (should raise an error)
print("Attempting to select a non-existent table:")
try:
    dataset_schema.select("non_existent_table")
except ValueError as e:
    print(f"Error: {e}")

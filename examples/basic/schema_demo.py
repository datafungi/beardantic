import sys
from pathlib import Path

import polars as pl

sys.path.append(str(Path.cwd()))

from src.beardantic.schema import parse_yaml_schema, validate_dataframe

# Path to the example schema YAML file
schema_path = Path(__file__).parent.parent / "schemas" / "schema_example.yaml"

# Parse the schema from the YAML file
dataset_schema = parse_yaml_schema(schema_path)
print(f"Loaded schema for dataset: {dataset_schema.name}")
print(f"Dataset description: {dataset_schema.description}")
print(f"Number of tables: {len(dataset_schema.tables)}")

# Get the schema for the users table
users_schema = next(table for table in dataset_schema.tables if table.name == "users")
print(f"\nTable: {users_schema.name}")
print(f"Description: {users_schema.description}")
print("Columns:")
for col in users_schema.columns:
    print(f"  - {col.name}: {col.type} (nullable: {col.nullable})")

# Create a sample DataFrame that matches the schema
users_df = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["John Doe", "Jane Smith", "Bob Johnson"],
        "email": ["john@example.com", None, "bob@example.com"],
        "age": [30, 25, None],
        "active": [True, False, True],
    }
)

print("\nSample DataFrame:")
print(users_df)

# Validate the DataFrame against the schema
validation_errors = validate_dataframe(users_df, users_schema)
if validation_errors:
    print("\nValidation errors:")
    for error in validation_errors:
        print(f"  - {error}")
else:
    print("\nDataFrame is valid according to the schema!")

# Create an invalid DataFrame with missing columns and wrong types
invalid_df = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["John Doe", "Jane Smith", "Bob Johnson"],
        "email": ["john@example.com", None, "bob@example.com"],
        # Missing 'age' column
        "active": ["yes", "no", "yes"],  # Wrong type (string instead of boolean)
        "extra_column": [1, 2, 3],  # Extra column not in schema
    }
)

print("\nInvalid DataFrame:")
print(invalid_df)

# Validate the invalid DataFrame
validation_errors = validate_dataframe(invalid_df, users_schema)
if validation_errors:
    print("\nValidation errors:")
    for error in validation_errors:
        print(f"  - {error}")
else:
    print("\nDataFrame is valid according to the schema!")

"""
Example demonstrating the usage of the refactored beardantic package.

This example shows how to:
1. Configure logging
2. Parse a YAML schema
3. Validate a Polars DataFrame against the schema
"""

import logging
import polars as pl

from beardantic import (
    configure_logging,
    parse_yaml_schema,
    validate_dataframe,
    SchemaValidationError,
)

# Configure logging to show debug messages
configure_logging(level=logging.DEBUG)

# Create a sample DataFrame
df = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 35, 40, None],  # Note: Contains a null value
        "is_active": [True, False, True, True, False],
        "score": [95.5, 87.2, 99.0, 82.5, 91.0],
    }
)

# Path to the schema file
schema_path = "examples/schema.yaml"

try:
    # Parse the schema from YAML
    dataset_schema = parse_yaml_schema(schema_path)

    # Get the table schema for "users"
    users_schema = next(
        (table for table in dataset_schema.tables if table.name == "users"), None
    )

    if users_schema:
        # Validate the DataFrame against the schema
        print("\n=== Validating DataFrame ===")
        errors = validate_dataframe(df, users_schema)

        if errors:
            print(f"\nValidation failed with {len(errors)} errors:")
            for i, error in enumerate(errors, 1):
                print(f"{i}. {error}")
        else:
            print("\nValidation successful! DataFrame conforms to the schema.")

        # Try with raise_exception=True
        try:
            print("\n=== Validating with raise_exception=True ===")
            validate_dataframe(df, users_schema, raise_exception=True)
            print("Validation successful!")
        except SchemaValidationError as e:
            print(f"Caught SchemaValidationError: {e}")
            if e.errors:
                print("Errors:")
                for error in e.errors:
                    print(f"- {error}")
    else:
        print("Table 'users' not found in the schema")

except FileNotFoundError:
    print(f"Schema file not found: {schema_path}")
except SchemaValidationError as e:
    print(f"Schema validation error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

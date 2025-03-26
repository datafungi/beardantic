# Beardantic

A Python library for parsing Polars schema from YAML files and validating DataFrames with Pydantic.

## Features

- Define Polars DataFrame schemas in YAML format
- Validate Polars DataFrames against schema definitions
- Type checking and validation using Pydantic
- Support for all Polars data types

## Installation

```bash
pip install beardantic
```

## Usage

### Define a schema in YAML

Create a YAML file that defines your dataset schema:

```yaml
name: sample_dataset
description: A sample dataset with tables

tables:
  - name: users
    description: User information table
    columns:
      - name: id
        type: int64
        nullable: false
        description: Unique identifier for users
      - name: name
        type: string
        nullable: false
        description: User's full name
      - name: email
        type: string
        nullable: true
        description: User's email address
```

### Parse the schema

```python
from pathlib import Path
from beardantic.schema import parse_yaml_schema

# Parse the schema from a YAML file
schema_path = Path("path/to/schema.yaml")
dataset_schema = parse_yaml_schema(schema_path)

# Access the schema information
print(f"Dataset: {dataset_schema.name}")
for table in dataset_schema.tables:
    print(f"Table: {table.name}")
    for column in table.columns:
        print(f"  - {column.name}: {column.type} (nullable: {column.nullable})")
```

### Validate a DataFrame

```python
import polars as pl
from beardantic.schema import parse_yaml_schema, validate_dataframe

# Parse the schema
schema_path = Path("path/to/schema.yaml")
dataset_schema = parse_yaml_schema(schema_path)

# Get a specific table schema
users_schema = next(table for table in dataset_schema.tables if table.name == "users")

# Create a DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["John", "Jane", "Bob"],
    "email": ["john@example.com", None, "bob@example.com"]
})

# Validate the DataFrame against the schema
errors = validate_dataframe(df, users_schema)
if errors:
    print("Validation errors:")
    for error in errors:
        print(f"  - {error}")
else:
    print("DataFrame is valid!")
```

## Supported Data Types

Beardantic supports all Polars data types, including:

- `boolean`: Boolean values (True/False)
- `integer`, `int8`, `int16`, `int32`, `int64`: Integer types
- `uint8`, `uint16`, `uint32`, `uint64`: Unsigned integer types
- `float`, `float32`, `float64`: Floating-point types
- `string`: String type
- `date`, `time`, `datetime`: Date and time types
- `list`: List type
- `struct`: Struct type
- And more...

## License

MIT

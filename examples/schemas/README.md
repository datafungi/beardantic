# Schema Definition Files

This directory contains YAML schema definition files used by the examples.

## Files

- **schema_example.yaml**: A simple schema definition with basic data types for users and orders tables
- **complex_schema_example.yaml**: A complex schema definition demonstrating nested structures, lists, and advanced data types

## Schema Structure

The schema files follow this general structure:

```yaml
name: dataset_name
description: Dataset description

tables:
  - name: table_name
    description: Table description
    columns:
      - name: column_name
        type: data_type
        nullable: true/false
        description: Column description
```

Supported data types include all Polars data types such as:
- Basic types: int8, int16, int32, int64, float32, float64, string, boolean
- Complex types: struct, list, datetime
- Nested types: structs containing other structs or lists

These schema files are used by the example scripts to demonstrate how to validate Polars DataFrames against defined schemas.

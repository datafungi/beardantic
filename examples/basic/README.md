# Basic Examples

This directory contains simple examples demonstrating the core functionality of Beardantic.

## Files

- **schema_demo.py**: Demonstrates how to parse a YAML schema and validate a Polars DataFrame against it
- **modular_example.py**: Shows how to use the refactored modular API with proper error handling and logging

## Running the Examples

To run these examples, make sure you have installed the Beardantic package:

```bash
# Install in development mode
pip install -e .
```

Then run the examples:

```bash
# From the project root
python examples/basic/schema_demo.py
python examples/basic/modular_example.py
```

These examples demonstrate:
- Loading schema definitions from YAML files
- Creating Polars DataFrames
- Validating DataFrames against schemas
- Handling validation errors
- Using the logging functionality

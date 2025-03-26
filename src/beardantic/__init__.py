"""Beardantic: A Polars DataFrame schema validation library.

This package provides tools for defining, parsing, and validating schemas for Polars DataFrames.
"""

# Import key components to make them available at the package level
from .exceptions import SchemaValidationError
from .models import ColumnSchema, DatasetSchema, NestedField, SchemaField, TableSchema
from .schema import parse_yaml_schema
from .validators import validate_dataframe

# Configure package logging
from .logging import configure_logging

# Define public API
__all__ = [
    'parse_yaml_schema',
    'validate_dataframe',
    'SchemaField',
    'NestedField',
    'ColumnSchema',
    'TableSchema',
    'DatasetSchema',
    'SchemaValidationError',
    'configure_logging'
]

__version__ = "0.1.0"
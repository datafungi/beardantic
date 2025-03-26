"""Main module for beardantic schema validation.

This module provides functions for parsing YAML schema files and validating
Polars DataFrames against the schema.
"""
from pathlib import Path
from typing import Union

import yaml
from pydantic import ValidationError

from .exceptions import SchemaValidationError
from .logging_config import get_logger
from .models import ColumnSchema, DatasetSchema, NestedField, SchemaField, TableSchema
from .validators import validate_dataframe

# Define public API
__all__ = [
    'parse_yaml_schema',
    'validate_dataframe',
    'SchemaField',
    'NestedField',
    'ColumnSchema',
    'TableSchema',
    'DatasetSchema',
    'SchemaValidationError'
]

# Configure logger
logger = get_logger(__name__)


def parse_yaml_schema(yaml_path: Union[str, Path]) -> DatasetSchema:
    """
    Parse a YAML file into a DatasetSchema.

    Args:
        yaml_path: Path to the YAML file

    Returns:
        DatasetSchema object

    Raises:
        FileNotFoundError: If the YAML file does not exist
        SchemaValidationError: If the YAML file does not conform to the expected schema
        yaml.YAMLError: If the YAML file is malformed
    """
    yaml_path = Path(yaml_path)
    logger.info(f"Parsing schema from {yaml_path}")
    
    if not yaml_path.exists():
        error_msg = f"YAML file not found: {yaml_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    try:
        with open(yaml_path, "r") as f:
            try:
                schema_dict = yaml.safe_load(f)
                logger.debug(f"Loaded YAML content from {yaml_path}")
            except yaml.YAMLError as e:
                error_msg = f"Invalid YAML format in {yaml_path}"
                logger.error(error_msg, exc_info=True)
                raise yaml.YAMLError(f"{error_msg}: {str(e)}")

        try:
            dataset_schema = DatasetSchema(**schema_dict)
            logger.info(f"Successfully parsed schema for dataset '{dataset_schema.name}'")
            return dataset_schema
        except ValidationError as e:
            error_msg = f"Invalid schema in {yaml_path}"
            logger.error(f"{error_msg}: {str(e)}")
            raise SchemaValidationError(error_msg, errors=[str(e)])
    except Exception as e:
        # Catch any other unexpected errors
        error_msg = f"Error parsing schema from {yaml_path}"
        logger.error(f"{error_msg}: {str(e)}", exc_info=True)
        raise


# The module re-exports the key components for backward compatibility
# These are explicitly declared in the __all__ list

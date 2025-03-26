"""
Validation functions for Polars DataFrames against schema definitions.
"""
from typing import List

import polars as pl

from .exceptions import SchemaValidationError
from .logging import get_logger
from .models import SchemaField, TableSchema

# Configure logger
logger = get_logger(__name__)


def validate_dataframe(
    df: pl.DataFrame, 
    table_schema: TableSchema, 
    raise_exception: bool = False
) -> List[str]:
    """
    Validate a Polars DataFrame against a table schema.

    Args:
        df: Polars DataFrame to validate
        table_schema: TableSchema to validate against
        raise_exception: If True, raises a SchemaValidationError when validation fails

    Returns:
        List of validation errors, empty if validation passes
        
    Raises:
        SchemaValidationError: If validation fails and raise_exception is True
    """
    logger.debug(f"Validating DataFrame against schema '{table_schema.name}'")
    errors = []

    # Check column existence
    try:
        schema_columns = {col.name for col in table_schema.columns}
        df_columns = set(df.columns)

        missing_columns = schema_columns - df_columns
        if missing_columns:
            error_msg = f"Missing columns: {', '.join(missing_columns)}"
            logger.warning(error_msg)
            errors.append(error_msg)

        extra_columns = df_columns - schema_columns
        if extra_columns:
            error_msg = f"Extra columns: {', '.join(extra_columns)}"
            logger.info(error_msg)  # This is just informational
            errors.append(error_msg)
    except Exception as e:
        error_msg = f"Error checking column existence: {str(e)}"
        logger.error(error_msg, exc_info=True)
        errors.append(error_msg)

    # Check column types and nullability
    for col in table_schema.columns:
        if col.name in df.columns:
            try:
                validate_column(df, col, errors)
            except Exception as e:
                error_msg = f"Error validating column '{col.name}': {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)

    if errors and raise_exception:
        logger.error(f"Validation failed for DataFrame against schema '{table_schema.name}'")
        raise SchemaValidationError(f"DataFrame validation failed for schema '{table_schema.name}'.", errors)
    
    if not errors:
        logger.info(f"DataFrame successfully validated against schema '{table_schema.name}'")
    else:
        logger.warning(f"Found {len(errors)} validation errors for schema '{table_schema.name}'")
    
    return errors


def validate_column(df: pl.DataFrame, col: SchemaField, errors: List[str]) -> None:
    """
    Validate a single column in a DataFrame against its schema definition.
    
    Args:
        df: Polars DataFrame containing the column
        col: Column schema to validate against
        errors: List to append errors to
    """
    # Get the actual type from the DataFrame
    df_type = df.schema[col.name]
    expected_type = col.to_polars_type()

    # Compare data types by their string representation to handle Polars type system
    expected_type_str = str(expected_type)
    df_type_str = str(df_type)
    
    logger.debug(f"Checking column '{col.name}' - expected: {expected_type_str}, actual: {df_type_str}")
    
    # Basic type checking for simple types
    if "List" not in expected_type_str and "Struct" not in expected_type_str:
        validate_simple_type(col.name, expected_type_str, df_type_str, df_type, expected_type, errors)
    
    # Simple check for struct type existence
    elif "Struct" in expected_type_str and "Struct" not in df_type_str:
        error_msg = f"Column '{col.name}' has type {df_type}, expected a struct type"
        logger.warning(error_msg)
        errors.append(error_msg)
    
    # Simple check for list type existence
    elif "List" in expected_type_str and "List" not in df_type_str:
        error_msg = f"Column '{col.name}' has type {df_type}, expected a list type"
        logger.warning(error_msg)
        errors.append(error_msg)
    
    # For struct types, check top-level field names if possible
    elif "Struct" in expected_type_str and hasattr(expected_type, "fields") and hasattr(df_type, "fields"):
        validate_struct_fields(col.name, expected_type, df_type, errors)
    
    # Check for nulls in non-nullable columns
    if not col.nullable and df[col.name].null_count() > 0:
        error_msg = f"Column '{col.name}' contains null values but is not nullable"
        logger.warning(error_msg)
        errors.append(error_msg)


def validate_simple_type(
    col_name: str, 
    expected_type_str: str, 
    df_type_str: str, 
    df_type: pl.DataType, 
    expected_type: pl.DataType, 
    errors: List[str]
) -> None:
    """
    Validate a simple (non-nested) data type.
    
    Args:
        col_name: Name of the column being validated
        expected_type_str: String representation of expected type
        df_type_str: String representation of actual type
        df_type: Actual Polars data type
        expected_type: Expected Polars data type
        errors: List to append errors to
    """
    # Special case for integers
    if ("Int" in expected_type_str and "Int" in df_type_str) or \
       ("UInt" in expected_type_str and "UInt" in df_type_str):
        pass  # Types match
    # Special case for floats
    elif "Float" in expected_type_str and "Float" in df_type_str:
        pass  # Types match
    # Special case for datetime
    elif "Datetime" in expected_type_str and "Datetime" in df_type_str:
        pass  # Types match
    # Check exact match for other simple types
    elif expected_type_str != df_type_str:
        error_msg = f"Column '{col_name}' has type {df_type}, expected {expected_type}"
        logger.warning(error_msg)
        errors.append(error_msg)


def validate_struct_fields(
    col_name: str, 
    expected_type: pl.DataType, 
    df_type: pl.DataType, 
    errors: List[str]
) -> None:
    """
    Validate the fields in a struct type.
    
    Args:
        col_name: Name of the column being validated
        expected_type: Expected Polars struct type
        df_type: Actual Polars struct type
        errors: List to append errors to
    """
    try:
        expected_fields = set(expected_type.fields.keys())
        actual_fields = set(df_type.fields.keys())
        
        missing_fields = expected_fields - actual_fields
        if missing_fields:
            error_msg = f"Column '{col_name}' is missing struct fields: {', '.join(missing_fields)}"
            logger.warning(error_msg)
            errors.append(error_msg)
    except (AttributeError, TypeError) as e:
        # If we can't access fields, just do a basic type check
        logger.debug(f"Could not check struct fields for column '{col_name}': {str(e)}")

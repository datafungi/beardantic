from pathlib import Path
from typing import Dict, List, Optional, Union

import polars as pl
import yaml
from pydantic import BaseModel, ValidationError, field_validator

from .constants import POLARS_DATA_TYPES


class SchemaField(BaseModel):
    """Schema definition for a field in a Polars DataFrame or nested structure."""

    name: str
    type: str
    nullable: bool = False
    description: Optional[str] = None
    fields: Optional[List['SchemaField']] = None  # For struct columns or list of structs
    element_type: Optional[str] = None  # For list columns

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate that the type is a valid Polars data type."""
        if v.lower() not in POLARS_DATA_TYPES and v.lower() not in ["struct", "list"]:
            valid_types = ", ".join(list(POLARS_DATA_TYPES.keys()) + ["struct", "list"])
            raise ValueError(
                f"Invalid Polars data type: {v}. Valid types are: {valid_types}"
            )
        return v.lower()
    
    def to_polars_type(self) -> pl.DataType:
        """Convert the schema type to a Polars data type."""
        if self.type.lower() == "struct" and self.fields:
            # Create a struct type with nested fields
            field_dict = {}
            for field in self.fields:
                field_dict[field.name] = field.to_polars_type()
            return pl.Struct(field_dict)
        elif self.type.lower() == "list" and self.element_type:
            # Create a list type with the specified element type
            if self.element_type.lower() == "struct" and self.fields:
                # Handle list of structs
                field_dict = {}
                for field in self.fields:
                    field_dict[field.name] = field.to_polars_type()
                return pl.List(pl.Struct(field_dict))
            else:
                # Handle list of primitive types
                return pl.List(POLARS_DATA_TYPES[self.element_type.lower()])
        else:
            # Handle primitive types
            return POLARS_DATA_TYPES[self.type.lower()]


# Type aliases for backward compatibility
NestedField = SchemaField
ColumnSchema = SchemaField


class TableSchema(BaseModel):
    """Schema definition for a Polars DataFrame."""

    name: str
    description: Optional[str] = None
    columns: List[SchemaField]

    def to_polars_schema(self) -> Dict[str, pl.DataType]:
        """Convert the table schema to a Polars schema dictionary."""
        return {col.name: col.to_polars_type() for col in self.columns}


class DatasetSchema(BaseModel):
    """Schema definition for a collection of Polars DataFrames."""

    name: str
    description: Optional[str] = None
    tables: List[TableSchema]


def parse_yaml_schema(yaml_path: Union[str, Path]) -> DatasetSchema:
    """
    Parse a YAML file into a DatasetSchema.

    Args:
        yaml_path: Path to the YAML file

    Returns:
        DatasetSchema object

    Raises:
        FileNotFoundError: If the YAML file does not exist
        ValidationError: If the YAML file does not conform to the expected schema
    """
    yaml_path = Path(yaml_path)
    if not yaml_path.exists():
        raise FileNotFoundError(f"YAML file not found: {yaml_path}")

    with open(yaml_path, "r") as f:
        schema_dict = yaml.safe_load(f)

    try:
        return DatasetSchema(**schema_dict)
    except ValidationError as e:
        raise ValidationError(f"Invalid schema in {yaml_path}: {e}")


def validate_dataframe(df: pl.DataFrame, table_schema: TableSchema) -> List[str]:
    """
    Validate a Polars DataFrame against a table schema.

    Args:
        df: Polars DataFrame to validate
        table_schema: TableSchema to validate against

    Returns:
        List of validation errors, empty if validation passes
    """
    errors = []

    # Check column existence
    schema_columns = {col.name for col in table_schema.columns}
    df_columns = set(df.columns)

    missing_columns = schema_columns - df_columns
    if missing_columns:
        errors.append(f"Missing columns: {', '.join(missing_columns)}")

    extra_columns = df_columns - schema_columns
    if extra_columns:
        errors.append(f"Extra columns: {', '.join(extra_columns)}")

    # Check column types and nullability
    for col in table_schema.columns:
        if col.name in df_columns:
            # Get the actual type from the DataFrame
            df_type = df.schema[col.name]
            expected_type = col.to_polars_type()

            # Compare data types by their string representation to handle Polars type system
            expected_type_str = str(expected_type)
            df_type_str = str(df_type)
            
            # Basic type checking for simple types
            if "List" not in expected_type_str and "Struct" not in expected_type_str:
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
                    errors.append(f"Column '{col.name}' has type {df_type}, expected {expected_type}")
            
            # Simple check for struct type existence
            elif "Struct" in expected_type_str and "Struct" not in df_type_str:
                errors.append(f"Column '{col.name}' has type {df_type}, expected a struct type")
            
            # Simple check for list type existence
            elif "List" in expected_type_str and "List" not in df_type_str:
                errors.append(f"Column '{col.name}' has type {df_type}, expected a list type")
            
            # For struct types, just check top-level field names if possible
            elif "Struct" in expected_type_str and hasattr(expected_type, "fields") and hasattr(df_type, "fields"):
                try:
                    expected_fields = set(expected_type.fields.keys())
                    actual_fields = set(df_type.fields.keys())
                    
                    missing_fields = expected_fields - actual_fields
                    if missing_fields:
                        errors.append(f"Column '{col.name}' is missing struct fields: {', '.join(missing_fields)}")
                except (AttributeError, TypeError):
                    # If we can't access fields, just do a basic type check
                    pass
            
            # Check for nulls in non-nullable columns
            if not col.nullable and df[col.name].null_count() > 0:
                errors.append(f"Column '{col.name}' contains null values but is not nullable")

    return errors

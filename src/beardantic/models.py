"""
Pydantic models for schema definitions.
"""
from typing import Dict, List, Optional

import polars as pl
from pydantic import BaseModel, field_validator

from .constants import POLARS_DATA_TYPES
from .logging_config import get_logger

# Configure logger
logger = get_logger(__name__)


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
            error_msg = f"Invalid Polars data type: {v}. Valid types are: {valid_types}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        return v.lower()
    
    def to_polars_type(self) -> pl.DataType:
        """Convert the schema type to a Polars data type."""
        try:
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
                    if self.element_type.lower() not in POLARS_DATA_TYPES:
                        error_msg = f"Invalid element type for list: {self.element_type}"
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    return pl.List(POLARS_DATA_TYPES[self.element_type.lower()])
            else:
                # Handle primitive types
                return POLARS_DATA_TYPES[self.type.lower()]
        except KeyError as e:
            error_msg = f"Unknown data type: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Error converting schema type to Polars type: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg)


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
        schema = {}
        for col in self.columns:
            schema[col.name] = col.to_polars_type()
        return schema


class DatasetSchema(BaseModel):
    """Schema definition for a collection of Polars DataFrames."""

    name: str
    description: Optional[str] = None
    tables: List[TableSchema]

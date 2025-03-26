"""
Custom exceptions for the beardantic package.
"""
from typing import List, Optional


class SchemaValidationError(Exception):
    """Exception raised for schema validation errors."""
    
    def __init__(self, message: str, errors: Optional[List[str]] = None):
        self.message = message
        self.errors = errors or []
        super().__init__(self.message)
    
    def __str__(self) -> str:
        if not self.errors:
            return self.message
        return f"{self.message}\n - " + "\n - ".join(self.errors)

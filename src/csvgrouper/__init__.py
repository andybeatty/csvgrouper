"""CSV Grouper - Group CSV files by field structure similarity."""

from .grouper import CSVGrouper, CSVFile, CSVGroup, FieldType

__all__ = ["CSVGrouper", "CSVFile", "CSVGroup", "FieldType"]
__version__ = "0.1.0"

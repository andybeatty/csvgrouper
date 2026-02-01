# csvgrouper

A Python library for grouping CSV files by field structure similarity.

## Project Structure
```
src/csvgrouper/
├── __init__.py      # Exports: CSVGrouper, CSVFile, CSVGroup, FieldType
└── grouper.py       # Core implementation
tests/
├── test_unit.py     # Unit tests for core functionality
└── test_integration.py  # Integration tests with test_data/
test_data/           # Sample CSV files for testing
```

## Development Commands
```bash
uv sync              # Install dependencies
uv run pytest        # Run tests
uv run pre-commit run --all-files  # Run linting
```

## Key Classes
- `CSVGrouper`: Main class for scanning directories and grouping files
- `CSVFile`: Represents a CSV file with headers, sample rows, and inferred types
- `CSVGroup`: A collection of similar CSV files
- `FieldType`: Enum for inferred field types (string, integer, float, date, etc.)

## Grouping Logic
- Uses Jaccard similarity on field names (column headers)
- Column order is ignored - files with same fields in different order are 100% similar
- `group_by_exact_match()`: Groups files with identical field sets
- `group_by_similarity(threshold)`: Groups files above similarity threshold (0.0-1.0)

## Testing
- Test data in `test_data/` includes material science, chemistry, and weather CSV files
- Unit tests cover type inference, similarity computation, serialization
- Integration tests verify grouping behavior with real files

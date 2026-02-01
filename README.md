# csvgrouper

A Python library for grouping CSV files by field structure similarity. Useful for organizing large collections of CSV files (experimental data, logs, exports) into processable subsets.

## Installation

Install directly from GitHub:

```bash
uv add git+https://github.com/andybeatty/csvgrouper.git
```

Or with pip:

```bash
pip install git+https://github.com/andybeatty/csvgrouper.git
```

For local development:

```bash
git clone https://github.com/andybeatty/csvgrouper.git
cd csvgrouper
uv sync
```

## Quick Start

```python
from csvgrouper import CSVGrouper

# Initialize (only reads headers + sample rows, not entire files)
grouper = CSVGrouper(sample_rows=5)

# Scan a directory for CSV files
grouper.scan_directory("/path/to/csv/files", recursive=True)

# Group by exact field match
groups = grouper.group_by_exact_match()

# Or group by similarity threshold (0.0 to 1.0)
groups = grouper.group_by_similarity(threshold=0.8)

# View summary
print(grouper.summary())
```

## Core Concepts

### Field Matching

Files are grouped based on their **field names** (column headers), not values. Column order doesn't matter—files with the same fields in different orders will match.

### Similarity Threshold

Similarity is calculated using the Jaccard index:

```
similarity = |intersection of fields| / |union of fields|
```

Examples:
- Files with identical fields: `1.0` (100%)
- File A has 8 fields, File B has 5 of those 8: `5/8 = 0.625` (62.5%)
- Files with no common fields: `0.0` (0%)

## API Reference

### CSVGrouper

```python
grouper = CSVGrouper(sample_rows=5)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sample_rows` | int | 5 | Number of data rows to read for type inference |

### Scanning Files

```python
files = grouper.scan_directory(
    directory="/path/to/csvs",
    recursive=False,
    pattern="*.csv"
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `directory` | str \| Path | required | Directory to scan |
| `recursive` | bool | False | Search subdirectories |
| `pattern` | str | "*.csv" | Glob pattern for matching files |

Returns a list of `CSVFile` objects.

### Grouping

```python
# Exact match (100% similarity)
groups = grouper.group_by_exact_match()

# Custom threshold
groups = grouper.group_by_similarity(threshold=0.75)
```

### Accessing Groups

```python
# Get all groups
groups = grouper.get_groups()  # dict[str, CSVGroup]

# Get specific group
group = grouper.get_group("group_1")  # CSVGroup | None

# Get file paths in a group
paths = grouper.get_files_in_group("group_1")  # list[str]

# Compute similarity between two files
similarity = grouper.compute_similarity(file1, file2)  # float
```

### Persistence

```python
# Save groupings to JSON
grouper.save_groupings("groupings.json")

# Load groupings from JSON
grouper.load_groupings("groupings.json")
```

### Processing Groups

Register a processor function to handle all files in a group:

```python
def process_sales_data(file_paths: list[str]):
    for path in file_paths:
        # Your processing logic
        print(f"Processing: {path}")

grouper.register_processor("group_1", process_sales_data)
grouper.process_group("group_1")
```

### Iterating Over Rows

Iterate over all rows across all files in a group:

```python
for file_path, row in grouper.iter_group_rows("group_1"):
    print(f"{file_path}: {row}")
```

## Data Classes

### CSVFile

```python
@dataclass
class CSVFile:
    path: str                      # File path
    headers: list[str]             # Column names
    sample_rows: list[list[str]]   # Sample data rows
    field_types: dict[str, str]    # Inferred types per field
    delimiter: str                 # Detected delimiter
```

### CSVGroup

```python
@dataclass
class CSVGroup:
    name: str                      # Group identifier
    canonical_headers: list[str]   # Headers from first file
    files: list[CSVFile]           # Files in this group
    similarity_threshold: float    # Threshold used for grouping
```

### FieldType

Inferred types for fields based on sample values:

| Type | Description |
|------|-------------|
| `string` | Text data |
| `integer` | Whole numbers |
| `float` | Decimal numbers |
| `boolean` | true/false, yes/no, 1/0 |
| `date` | YYYY-MM-DD format |
| `datetime` | YYYY-MM-DD HH:MM:SS format |
| `empty` | All values empty |
| `mixed` | Multiple incompatible types |

## Examples

### Organizing Experimental Data

```python
from csvgrouper import CSVGrouper

grouper = CSVGrouper()
grouper.scan_directory("./experiments", recursive=True)

# Find files with identical schemas
exact_groups = grouper.group_by_exact_match()

print(f"Found {len(exact_groups)} distinct file schemas")
for name, group in exact_groups.items():
    print(f"\n{name}: {len(group.files)} files")
    print(f"  Fields: {group.canonical_headers}")
```

### Merging Similar Datasets

```python
import csv
from csvgrouper import CSVGrouper

grouper = CSVGrouper()
grouper.scan_directory("./data")
grouper.group_by_similarity(threshold=0.9)

# Merge all files in a group
def merge_to_single_file(file_paths: list[str]):
    all_rows = []
    headers = None

    for path in file_paths:
        with open(path) as f:
            reader = csv.DictReader(f)
            if headers is None:
                headers = reader.fieldnames
            all_rows.extend(reader)

    with open("merged_output.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Merged {len(file_paths)} files into merged_output.csv")

grouper.register_processor("group_1", merge_to_single_file)
grouper.process_group("group_1")
```

### Finding Related Files

```python
from csvgrouper import CSVGrouper

grouper = CSVGrouper()
files = grouper.scan_directory("./reports")

# Find files similar to a reference file
reference = files[0]
print(f"Finding files similar to: {reference.path}")
print(f"Reference fields: {reference.headers}\n")

for other in files[1:]:
    similarity = grouper.compute_similarity(reference, other)
    if similarity > 0.5:
        print(f"{similarity:.0%} similar: {other.path}")
```

### Workflow: Scan, Group, Save, Process Later

```python
from csvgrouper import CSVGrouper

# Session 1: Scan and save
grouper = CSVGrouper()
grouper.scan_directory("./incoming_data")
grouper.group_by_similarity(threshold=0.8)
grouper.save_groupings("data_groups.json")
print(grouper.summary())

# Session 2: Load and process
grouper = CSVGrouper()
grouper.load_groupings("data_groups.json")

for group_name in grouper.get_groups():
    paths = grouper.get_files_in_group(group_name)
    print(f"{group_name}: {len(paths)} files ready for processing")
```

## License

MIT

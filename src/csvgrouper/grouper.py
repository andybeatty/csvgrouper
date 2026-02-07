"""Core CSV grouping functionality."""

import csv
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Iterator


class FieldType(Enum):
    """Inferred type for a CSV field based on sample values."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    EMPTY = "empty"
    MIXED = "mixed"


@dataclass
class CSVFile:
    """Represents a CSV file with its header and sample data."""

    path: str
    headers: list[str]
    sample_rows: list[list[str]] = field(default_factory=list)
    field_types: dict[str, str] = field(default_factory=dict)
    delimiter: str = ","

    @property
    def field_set(self) -> frozenset[str]:
        """Return normalized headers as a frozen set for comparison."""
        return frozenset(self._normalize_header(header) for header in self.headers)

    @staticmethod
    def _normalize_header(header: str) -> str:
        """Normalize a header for case-insensitive matching."""
        return header.strip().casefold()

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "path": self.path,
            "headers": self.headers,
            "sample_rows": self.sample_rows,
            "field_types": self.field_types,
            "delimiter": self.delimiter,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CSVFile":
        """Create from dictionary."""
        return cls(
            path=data["path"],
            headers=data["headers"],
            sample_rows=data.get("sample_rows", []),
            field_types=data.get("field_types", {}),
            delimiter=data.get("delimiter", ","),
        )


@dataclass
class CSVGroup:
    """A group of CSV files with similar field structures."""

    name: str
    canonical_headers: list[str]
    files: list[CSVFile] = field(default_factory=list)
    similarity_threshold: float = 1.0  # 1.0 = exact match

    @property
    def file_paths(self) -> list[str]:
        """Return list of file paths in this group."""
        return [f.path for f in self.files]

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "canonical_headers": self.canonical_headers,
            "files": [f.to_dict() for f in self.files],
            "similarity_threshold": self.similarity_threshold,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CSVGroup":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            canonical_headers=data["canonical_headers"],
            files=[CSVFile.from_dict(f) for f in data.get("files", [])],
            similarity_threshold=data.get("similarity_threshold", 1.0),
        )


class CSVGrouper:
    """Main class for grouping CSV files by field structure."""

    # Patterns for type inference
    _INT_PATTERN = re.compile(r"^-?\d+$")
    _FLOAT_PATTERN = re.compile(r"^-?\d+\.\d+$")
    _BOOL_PATTERN = re.compile(r"^(true|false|yes|no|1|0)$", re.IGNORECASE)
    _DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    _DATETIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")

    def __init__(self, sample_rows: int = 5):
        """
        Initialize the grouper.

        Args:
            sample_rows: Number of data rows to read for type inference.
        """
        self.sample_rows = sample_rows
        self._files: dict[str, CSVFile] = {}
        self._groups: dict[str, CSVGroup] = {}
        self._processors: dict[str, Callable[[list[str]], None]] = {}

    def scan_directory(
        self, directory: str | Path, recursive: bool = False, pattern: str = "*.csv"
    ) -> list[CSVFile]:
        """
        Scan a directory for CSV files and extract their metadata.

        Args:
            directory: Path to the directory to scan.
            recursive: Whether to scan subdirectories.
            pattern: Glob pattern for matching files.

        Returns:
            List of CSVFile objects discovered.
        """
        directory = Path(directory)
        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")

        glob_method = directory.rglob if recursive else directory.glob
        discovered = []

        for csv_path in glob_method(pattern):
            if csv_path.is_file():
                try:
                    csv_file = self._read_csv_metadata(csv_path)
                    self._files[str(csv_path)] = csv_file
                    discovered.append(csv_file)
                except Exception as e:
                    # Skip files that can't be parsed
                    print(f"Warning: Could not parse {csv_path}: {e}")

        return discovered

    def _detect_delimiter(self, file_path: Path) -> str:
        """Detect the delimiter used in a CSV file."""
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            sample = f.read(4096)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                return dialect.delimiter
            except csv.Error:
                return ","

    def _read_csv_metadata(self, file_path: Path) -> CSVFile:
        """Read only the header and sample rows from a CSV file."""
        delimiter = self._detect_delimiter(file_path)

        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=delimiter)

            # Read header
            headers = next(reader, [])
            if not headers:
                raise ValueError(f"Empty CSV file: {file_path}")

            # Read sample rows
            sample_rows = []
            for i, row in enumerate(reader):
                if i >= self.sample_rows:
                    break
                sample_rows.append(row)

        # Infer field types from samples
        field_types = self._infer_field_types(headers, sample_rows)

        return CSVFile(
            path=str(file_path),
            headers=headers,
            sample_rows=sample_rows,
            field_types=field_types,
            delimiter=delimiter,
        )

    def _infer_type(self, value: str) -> FieldType:
        """Infer the type of a single value."""
        if not value or value.isspace():
            return FieldType.EMPTY
        if self._DATETIME_PATTERN.match(value):
            return FieldType.DATETIME
        if self._DATE_PATTERN.match(value):
            return FieldType.DATE
        if self._BOOL_PATTERN.match(value):
            return FieldType.BOOLEAN
        if self._INT_PATTERN.match(value):
            return FieldType.INTEGER
        if self._FLOAT_PATTERN.match(value):
            return FieldType.FLOAT
        return FieldType.STRING

    def _infer_field_types(
        self, headers: list[str], sample_rows: list[list[str]]
    ) -> dict[str, str]:
        """Infer types for each field based on sample values."""
        field_types = {}

        for i, header in enumerate(headers):
            values = [row[i] for row in sample_rows if i < len(row)]
            if not values:
                field_types[header] = FieldType.EMPTY.value
                continue

            types_seen = set()
            for value in values:
                inferred = self._infer_type(value)
                if inferred != FieldType.EMPTY:
                    types_seen.add(inferred)

            if not types_seen:
                field_types[header] = FieldType.EMPTY.value
            elif len(types_seen) == 1:
                field_types[header] = types_seen.pop().value
            else:
                # Multiple types seen - check for compatible numeric types
                if types_seen <= {FieldType.INTEGER, FieldType.FLOAT}:
                    field_types[header] = FieldType.FLOAT.value
                else:
                    field_types[header] = FieldType.MIXED.value

        return field_types

    def compute_similarity(self, file1: CSVFile, file2: CSVFile) -> float:
        """
        Compute Jaccard similarity between two files' field sets.

        Returns:
            Float between 0.0 and 1.0 (1.0 = identical fields).
        """
        set1 = file1.field_set
        set2 = file2.field_set

        if not set1 and not set2:
            return 1.0
        if not set1 or not set2:
            return 0.0

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union

    def group_by_exact_match(self) -> dict[str, CSVGroup]:
        """
        Group files by exact field match.

        Returns:
            Dictionary mapping group names to CSVGroup objects.
        """
        return self.group_by_similarity(threshold=1.0)

    def group_by_similarity(self, threshold: float = 0.8) -> dict[str, CSVGroup]:
        """
        Group files by field similarity above a threshold.

        Args:
            threshold: Minimum Jaccard similarity (0.0 to 1.0).

        Returns:
            Dictionary mapping group names to CSVGroup objects.
        """
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("Threshold must be between 0.0 and 1.0")

        self._groups.clear()
        ungrouped = list(self._files.values())
        group_counter = 0

        while ungrouped:
            # Start a new group with the first ungrouped file
            seed = ungrouped.pop(0)
            group_counter += 1
            group_name = f"group_{group_counter}"

            group = CSVGroup(
                name=group_name,
                canonical_headers=list(seed.headers),
                files=[seed],
                similarity_threshold=threshold,
            )

            # Find all files similar enough to the seed
            still_ungrouped = []
            for candidate in ungrouped:
                similarity = self.compute_similarity(seed, candidate)
                if similarity >= threshold:
                    group.files.append(candidate)
                else:
                    still_ungrouped.append(candidate)

            ungrouped = still_ungrouped
            self._groups[group_name] = group

        return self._groups

    def get_groups(self) -> dict[str, CSVGroup]:
        """Return current groups."""
        return self._groups

    def get_group(self, name: str) -> CSVGroup | None:
        """Get a specific group by name."""
        return self._groups.get(name)

    def get_files_in_group(self, group_name: str) -> list[str]:
        """Get file paths for a specific group."""
        group = self._groups.get(group_name)
        return group.file_paths if group else []

    def save_groupings(self, output_path: str | Path) -> None:
        """
        Save current groupings to a JSON file.

        Args:
            output_path: Path to save the JSON file.
        """
        data = {
            "groups": {name: g.to_dict() for name, g in self._groups.items()},
            "sample_rows": self.sample_rows,
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def load_groupings(self, input_path: str | Path) -> dict[str, CSVGroup]:
        """
        Load groupings from a JSON file.

        Args:
            input_path: Path to the JSON file.

        Returns:
            Dictionary of loaded groups.
        """
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._groups = {
            name: CSVGroup.from_dict(g) for name, g in data.get("groups", {}).items()
        }

        # Rebuild files index from groups
        self._files.clear()
        for group in self._groups.values():
            for csv_file in group.files:
                self._files[csv_file.path] = csv_file

        return self._groups

    def register_processor(
        self, group_name: str, processor: Callable[[list[str]], None]
    ) -> None:
        """
        Register a processor function for a group.

        Args:
            group_name: Name of the group.
            processor: Callable that takes a list of file paths.
        """
        self._processors[group_name] = processor

    def process_group(self, group_name: str) -> None:
        """
        Run the registered processor for a group.

        Args:
            group_name: Name of the group to process.
        """
        if group_name not in self._groups:
            raise ValueError(f"Unknown group: {group_name}")
        if group_name not in self._processors:
            raise ValueError(f"No processor registered for group: {group_name}")

        file_paths = self.get_files_in_group(group_name)
        self._processors[group_name](file_paths)

    def iter_group_rows(self, group_name: str) -> Iterator[tuple[str, dict[str, str]]]:
        """
        Iterate over all rows in all files of a group.

        Yields:
            Tuples of (file_path, row_dict) where row_dict maps headers to values.
        """
        group = self._groups.get(group_name)
        if not group:
            raise ValueError(f"Unknown group: {group_name}")

        for csv_file in group.files:
            with open(csv_file.path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=csv_file.delimiter)
                for row in reader:
                    yield csv_file.path, row

    def summary(self) -> str:
        """Return a human-readable summary of current groupings."""
        lines = [
            "CSV Grouper Summary",
            "==================",
            f"Total files: {len(self._files)}",
            f"Total groups: {len(self._groups)}",
            "",
        ]

        for name, group in self._groups.items():
            lines.append(f"Group: {name}")
            lines.append(f"  Files: {len(group.files)}")
            lines.append(f"  Similarity threshold: {group.similarity_threshold:.0%}")
            lines.append(f"  Headers: {', '.join(group.canonical_headers[:5])}")
            if len(group.canonical_headers) > 5:
                lines.append(f"    ... and {len(group.canonical_headers) - 5} more")
            lines.append("")

        return "\n".join(lines)

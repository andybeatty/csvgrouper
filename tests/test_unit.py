"""Unit tests for csvgrouper core functionality."""

import json
import tempfile
from pathlib import Path

import pytest

from csvgrouper import CSVGrouper, CSVFile, CSVGroup, FieldType


class TestCSVFile:
    """Tests for CSVFile dataclass."""

    def test_field_set_returns_frozenset(self):
        csv_file = CSVFile(
            path="/test/file.csv",
            headers=["a", "b", "c"],
        )
        assert csv_file.field_set == frozenset(["a", "b", "c"])

    def test_field_set_ignores_order(self):
        file1 = CSVFile(path="/test/1.csv", headers=["a", "b", "c"])
        file2 = CSVFile(path="/test/2.csv", headers=["c", "a", "b"])
        assert file1.field_set == file2.field_set

    def test_field_set_normalizes_case_and_whitespace(self):
        csv_file = CSVFile(
            path="/test/file.csv",
            headers=[" Name ", "EMAIL", " Value"],
        )
        assert csv_file.field_set == frozenset(["name", "email", "value"])

    def test_to_dict_roundtrip(self):
        original = CSVFile(
            path="/test/file.csv",
            headers=["id", "name", "value"],
            sample_rows=[["1", "foo", "10"], ["2", "bar", "20"]],
            field_types={"id": "integer", "name": "string", "value": "integer"},
            delimiter=",",
        )
        data = original.to_dict()
        restored = CSVFile.from_dict(data)

        assert restored.path == original.path
        assert restored.headers == original.headers
        assert restored.sample_rows == original.sample_rows
        assert restored.field_types == original.field_types
        assert restored.delimiter == original.delimiter

    def test_from_dict_with_defaults(self):
        data = {"path": "/test.csv", "headers": ["a", "b"]}
        csv_file = CSVFile.from_dict(data)

        assert csv_file.sample_rows == []
        assert csv_file.field_types == {}
        assert csv_file.delimiter == ","


class TestCSVGroup:
    """Tests for CSVGroup dataclass."""

    def test_file_paths_returns_list_of_paths(self):
        group = CSVGroup(
            name="test_group",
            canonical_headers=["a", "b"],
            files=[
                CSVFile(path="/test/1.csv", headers=["a", "b"]),
                CSVFile(path="/test/2.csv", headers=["a", "b"]),
            ],
        )
        assert group.file_paths == ["/test/1.csv", "/test/2.csv"]

    def test_file_paths_empty_group(self):
        group = CSVGroup(name="empty", canonical_headers=["a"])
        assert group.file_paths == []

    def test_to_dict_roundtrip(self):
        original = CSVGroup(
            name="test_group",
            canonical_headers=["id", "value"],
            files=[
                CSVFile(path="/test/1.csv", headers=["id", "value"]),
            ],
            similarity_threshold=0.8,
        )
        data = original.to_dict()
        restored = CSVGroup.from_dict(data)

        assert restored.name == original.name
        assert restored.canonical_headers == original.canonical_headers
        assert len(restored.files) == len(original.files)
        assert restored.similarity_threshold == original.similarity_threshold


class TestFieldType:
    """Tests for FieldType enum."""

    def test_all_types_have_string_values(self):
        for field_type in FieldType:
            assert isinstance(field_type.value, str)

    def test_expected_types_exist(self):
        expected = {
            "string",
            "integer",
            "float",
            "boolean",
            "date",
            "datetime",
            "empty",
            "mixed",
        }
        actual = {ft.value for ft in FieldType}
        assert actual == expected


class TestCSVGrouperTypeInference:
    """Tests for type inference functionality."""

    def test_infer_integer(self):
        grouper = CSVGrouper()
        assert grouper._infer_type("123") == FieldType.INTEGER
        assert grouper._infer_type("-456") == FieldType.INTEGER
        assert grouper._infer_type("42") == FieldType.INTEGER
        # Note: "0" and "1" match boolean pattern first

    def test_infer_float(self):
        grouper = CSVGrouper()
        assert grouper._infer_type("123.45") == FieldType.FLOAT
        assert grouper._infer_type("-0.5") == FieldType.FLOAT

    def test_infer_boolean(self):
        grouper = CSVGrouper()
        assert grouper._infer_type("true") == FieldType.BOOLEAN
        assert grouper._infer_type("FALSE") == FieldType.BOOLEAN
        assert grouper._infer_type("yes") == FieldType.BOOLEAN
        assert grouper._infer_type("No") == FieldType.BOOLEAN

    def test_infer_date(self):
        grouper = CSVGrouper()
        assert grouper._infer_type("2024-01-15") == FieldType.DATE
        assert grouper._infer_type("2024-12-31") == FieldType.DATE

    def test_infer_datetime(self):
        grouper = CSVGrouper()
        assert grouper._infer_type("2024-01-15T10:30:00") == FieldType.DATETIME
        assert grouper._infer_type("2024-01-15 10:30:00") == FieldType.DATETIME

    def test_infer_string(self):
        grouper = CSVGrouper()
        assert grouper._infer_type("hello") == FieldType.STRING
        assert grouper._infer_type("foo bar") == FieldType.STRING
        assert grouper._infer_type("123abc") == FieldType.STRING

    def test_infer_empty(self):
        grouper = CSVGrouper()
        assert grouper._infer_type("") == FieldType.EMPTY
        assert grouper._infer_type("   ") == FieldType.EMPTY

    def test_infer_field_types_single_type(self):
        grouper = CSVGrouper()
        headers = ["count"]
        # Use values that don't match boolean pattern (0, 1)
        sample_rows = [["10"], ["20"], ["30"]]
        types = grouper._infer_field_types(headers, sample_rows)
        assert types["count"] == "integer"

    def test_infer_field_types_mixed_numeric(self):
        grouper = CSVGrouper()
        headers = ["value"]
        # Use values that don't match boolean pattern
        sample_rows = [["10"], ["2.5"], ["30"]]
        types = grouper._infer_field_types(headers, sample_rows)
        assert types["value"] == "float"

    def test_infer_field_types_mixed_incompatible(self):
        grouper = CSVGrouper()
        headers = ["data"]
        sample_rows = [["123"], ["hello"], ["456"]]
        types = grouper._infer_field_types(headers, sample_rows)
        assert types["data"] == "mixed"


class TestCSVGrouperSimilarity:
    """Tests for similarity computation."""

    def test_identical_files_similarity_is_one(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=["a", "b", "c"])
        file2 = CSVFile(path="/2.csv", headers=["a", "b", "c"])
        assert grouper.compute_similarity(file1, file2) == 1.0

    def test_identical_files_different_order(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=["a", "b", "c"])
        file2 = CSVFile(path="/2.csv", headers=["c", "a", "b"])
        assert grouper.compute_similarity(file1, file2) == 1.0

    def test_no_overlap_similarity_is_zero(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=["a", "b"])
        file2 = CSVFile(path="/2.csv", headers=["x", "y"])
        assert grouper.compute_similarity(file1, file2) == 0.0

    def test_partial_overlap(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=["a", "b", "c", "d"])
        file2 = CSVFile(path="/2.csv", headers=["a", "b"])
        # intersection = 2, union = 4
        assert grouper.compute_similarity(file1, file2) == 0.5

    def test_both_empty_similarity_is_one(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=[])
        file2 = CSVFile(path="/2.csv", headers=[])
        assert grouper.compute_similarity(file1, file2) == 1.0

    def test_one_empty_similarity_is_zero(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=["a", "b"])
        file2 = CSVFile(path="/2.csv", headers=[])
        assert grouper.compute_similarity(file1, file2) == 0.0

    def test_case_differences_are_treated_as_matches(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=["Name", "Email"])
        file2 = CSVFile(path="/2.csv", headers=["name", "EMAIL"])
        assert grouper.compute_similarity(file1, file2) == 1.0

    def test_surrounding_whitespace_is_ignored(self):
        grouper = CSVGrouper()
        file1 = CSVFile(path="/1.csv", headers=[" name ", "value"])
        file2 = CSVFile(path="/2.csv", headers=["name", " Value "])
        assert grouper.compute_similarity(file1, file2) == 1.0


class TestCSVGrouperGrouping:
    """Tests for grouping logic."""

    @pytest.fixture
    def grouper_with_files(self):
        grouper = CSVGrouper()
        grouper._files = {
            "/a1.csv": CSVFile(path="/a1.csv", headers=["x", "y", "z"]),
            "/a2.csv": CSVFile(path="/a2.csv", headers=["z", "x", "y"]),  # same as a1
            "/b1.csv": CSVFile(path="/b1.csv", headers=["p", "q"]),
            "/b2.csv": CSVFile(path="/b2.csv", headers=["q", "p"]),  # same as b1
            "/c1.csv": CSVFile(path="/c1.csv", headers=["m", "n", "o", "p"]),
        }
        return grouper

    def test_exact_match_groups_identical_fields(self, grouper_with_files):
        groups = grouper_with_files.group_by_exact_match()

        # Should have 3 groups: {a1, a2}, {b1, b2}, {c1}
        assert len(groups) == 3

        # Find the group with x, y, z
        xyz_group = None
        for g in groups.values():
            if set(g.canonical_headers) == {"x", "y", "z"}:
                xyz_group = g
                break
        assert xyz_group is not None
        assert len(xyz_group.files) == 2

    def test_similarity_threshold_merges_groups(self, grouper_with_files):
        # b1/b2 have {p, q}, c1 has {m, n, o, p}
        # similarity = 1/5 = 0.2
        groups = grouper_with_files.group_by_similarity(threshold=0.2)

        # At 0.2 threshold, b and c should merge
        assert len(groups) < 3

    def test_invalid_threshold_raises(self, grouper_with_files):
        with pytest.raises(ValueError):
            grouper_with_files.group_by_similarity(threshold=1.5)
        with pytest.raises(ValueError):
            grouper_with_files.group_by_similarity(threshold=-0.1)

    def test_get_group_returns_none_for_unknown(self, grouper_with_files):
        grouper_with_files.group_by_exact_match()
        assert grouper_with_files.get_group("nonexistent") is None

    def test_get_files_in_group_returns_empty_for_unknown(self, grouper_with_files):
        grouper_with_files.group_by_exact_match()
        assert grouper_with_files.get_files_in_group("nonexistent") == []


class TestCSVGrouperPersistence:
    """Tests for save/load functionality."""

    def test_save_and_load_roundtrip(self):
        grouper = CSVGrouper(sample_rows=3)
        grouper._files = {
            "/test/1.csv": CSVFile(
                path="/test/1.csv",
                headers=["a", "b"],
                field_types={"a": "string", "b": "integer"},
            ),
            "/test/2.csv": CSVFile(
                path="/test/2.csv",
                headers=["a", "b"],
                field_types={"a": "string", "b": "integer"},
            ),
        }
        grouper.group_by_exact_match()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            grouper.save_groupings(temp_path)

            # Load into new grouper
            new_grouper = CSVGrouper()
            loaded_groups = new_grouper.load_groupings(temp_path)

            assert len(loaded_groups) == 1
            group = list(loaded_groups.values())[0]
            assert len(group.files) == 2
            assert set(group.canonical_headers) == {"a", "b"}
        finally:
            Path(temp_path).unlink()

    def test_save_creates_valid_json(self):
        grouper = CSVGrouper()
        grouper._files = {
            "/test.csv": CSVFile(path="/test.csv", headers=["x"]),
        }
        grouper.group_by_exact_match()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            grouper.save_groupings(temp_path)

            with open(temp_path) as f:
                data = json.load(f)

            assert "groups" in data
            assert "sample_rows" in data
        finally:
            Path(temp_path).unlink()


class TestCSVGrouperProcessing:
    """Tests for processor registration and execution."""

    def test_register_and_process(self):
        grouper = CSVGrouper()
        grouper._files = {
            "/1.csv": CSVFile(path="/1.csv", headers=["a"]),
            "/2.csv": CSVFile(path="/2.csv", headers=["a"]),
        }
        grouper.group_by_exact_match()

        processed_files = []

        def processor(file_paths):
            processed_files.extend(file_paths)

        group_name = list(grouper.get_groups().keys())[0]
        grouper.register_processor(group_name, processor)
        grouper.process_group(group_name)

        assert len(processed_files) == 2
        assert "/1.csv" in processed_files
        assert "/2.csv" in processed_files

    def test_process_unknown_group_raises(self):
        grouper = CSVGrouper()
        grouper._groups = {"existing": CSVGroup(name="existing", canonical_headers=[])}

        with pytest.raises(ValueError, match="Unknown group"):
            grouper.process_group("nonexistent")

    def test_process_without_processor_raises(self):
        grouper = CSVGrouper()
        grouper._groups = {"test": CSVGroup(name="test", canonical_headers=[])}

        with pytest.raises(ValueError, match="No processor registered"):
            grouper.process_group("test")


class TestCSVGrouperSummary:
    """Tests for summary output."""

    def test_summary_includes_file_count(self):
        grouper = CSVGrouper()
        grouper._files = {
            "/1.csv": CSVFile(path="/1.csv", headers=["a"]),
            "/2.csv": CSVFile(path="/2.csv", headers=["b"]),
        }
        grouper.group_by_exact_match()

        summary = grouper.summary()
        assert "Total files: 2" in summary

    def test_summary_includes_group_count(self):
        grouper = CSVGrouper()
        grouper._files = {
            "/1.csv": CSVFile(path="/1.csv", headers=["a"]),
            "/2.csv": CSVFile(path="/2.csv", headers=["b"]),
        }
        grouper.group_by_exact_match()

        summary = grouper.summary()
        assert "Total groups: 2" in summary

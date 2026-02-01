"""Integration tests using real CSV test data files."""

import json
import tempfile
from pathlib import Path

import pytest

from csvgrouper import CSVGrouper, CSVFile, FieldType


# Path to test data relative to project root
TEST_DATA_DIR = Path(__file__).parent.parent / "test_data"


@pytest.fixture
def grouper():
    """Create a fresh CSVGrouper instance."""
    return CSVGrouper(sample_rows=5)


@pytest.fixture
def loaded_grouper(grouper):
    """Create a grouper with test data already scanned."""
    grouper.scan_directory(TEST_DATA_DIR)
    return grouper


class TestDirectoryScanning:
    """Tests for scanning CSV files from the test_data directory."""

    def test_scan_finds_all_csv_files(self, grouper):
        files = grouper.scan_directory(TEST_DATA_DIR)
        assert len(files) == 10

    def test_scan_returns_csv_file_objects(self, grouper):
        files = grouper.scan_directory(TEST_DATA_DIR)
        assert all(isinstance(f, CSVFile) for f in files)

    def test_scan_extracts_headers(self, grouper):
        files = grouper.scan_directory(TEST_DATA_DIR)
        for f in files:
            assert len(f.headers) > 0
            assert all(isinstance(h, str) for h in f.headers)

    def test_scan_extracts_sample_rows(self, grouper):
        files = grouper.scan_directory(TEST_DATA_DIR)
        for f in files:
            assert len(f.sample_rows) <= 5  # sample_rows=5

    def test_scan_infers_field_types(self, grouper):
        files = grouper.scan_directory(TEST_DATA_DIR)
        for f in files:
            assert len(f.field_types) == len(f.headers)

    def test_scan_nonexistent_directory_raises(self, grouper):
        with pytest.raises(ValueError, match="Not a directory"):
            grouper.scan_directory("/nonexistent/path")

    def test_scan_with_pattern(self, grouper):
        # Only match weather files
        files = grouper.scan_directory(TEST_DATA_DIR, pattern="weather*.csv")
        assert len(files) == 3
        assert all("weather" in f.path for f in files)


class TestMaterialScienceFiles:
    """Tests specific to material science test data."""

    def test_tensile_test_files_have_matching_headers(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}

        steel = files["tensile_test_steel.csv"]
        aluminum = files["tensile_test_aluminum.csv"]

        # Same fields, different order
        assert steel.field_set == aluminum.field_set
        assert loaded_grouper.compute_similarity(steel, aluminum) == 1.0

    def test_tensile_test_type_inference(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}
        steel = files["tensile_test_steel.csv"]

        # Check specific type inferences
        assert steel.field_types["temperature_c"] == "integer"
        assert steel.field_types["material"] == "string"
        assert steel.field_types["test_date"] == "date"

    def test_hardness_vs_tensile_partial_overlap(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}

        tensile = files["tensile_test_steel.csv"]
        hardness = files["hardness_test_metals.csv"]

        # They share: sample_id, material, test_date, temperature_c
        similarity = loaded_grouper.compute_similarity(tensile, hardness)
        assert 0.3 < similarity < 0.6  # Partial overlap


class TestChemistryFiles:
    """Tests specific to chemistry test data."""

    def test_reaction_kinetics_files_high_similarity(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}

        oxidation = files["reaction_kinetics_oxidation.csv"]
        hydrogenation = files["reaction_kinetics_hydrogenation.csv"]

        similarity = loaded_grouper.compute_similarity(oxidation, hydrogenation)
        # Hydrogenation has one extra field (h2_flow_sccm)
        assert similarity > 0.85

    def test_uv_vis_has_unique_fields(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}
        uv_vis = files["uv_vis_spectroscopy.csv"]

        # UV-Vis has unique fields like wavelength_nm, absorbance
        assert "wavelength_nm" in uv_vis.headers
        assert "absorbance" in uv_vis.headers

    def test_spectroscopy_type_inference(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}
        uv_vis = files["uv_vis_spectroscopy.csv"]

        assert uv_vis.field_types["wavelength_nm"] == "integer"
        assert uv_vis.field_types["absorbance"] == "float"
        assert uv_vis.field_types["compound"] == "string"


class TestWeatherFiles:
    """Tests specific to weather test data."""

    def test_weather_alpha_beta_identical_fields(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}

        alpha = files["weather_station_alpha.csv"]
        beta = files["weather_station_beta.csv"]

        # Identical fields, completely different order
        assert alpha.field_set == beta.field_set
        assert loaded_grouper.compute_similarity(alpha, beta) == 1.0

        # Verify different column order in source
        assert alpha.headers != beta.headers

    def test_weather_gamma_subset_of_alpha(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}

        alpha = files["weather_station_alpha.csv"]
        gamma = files["weather_station_gamma_minimal.csv"]

        # Gamma has 5 fields, alpha has 8
        assert gamma.field_set < alpha.field_set  # proper subset
        similarity = loaded_grouper.compute_similarity(alpha, gamma)
        assert similarity == 5 / 8  # 0.625

    def test_weather_type_inference(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}
        alpha = files["weather_station_alpha.csv"]

        assert alpha.field_types["temperature_c"] == "float"
        assert alpha.field_types["humidity_pct"] == "integer"
        assert alpha.field_types["timestamp"] == "datetime"


class TestLabEnvironmentFiles:
    """Tests for lab environment monitoring data."""

    def test_lab_env_shares_fields_with_weather(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}

        lab = files["lab_environment_monitoring.csv"]
        weather = files["weather_station_gamma_minimal.csv"]

        # Shared: timestamp, temperature_c, humidity_pct, pressure_hpa
        common = lab.field_set & weather.field_set
        assert "temperature_c" in common
        assert "humidity_pct" in common
        assert "pressure_hpa" in common

    def test_lab_env_has_unique_fields(self, loaded_grouper):
        files = {Path(f.path).name: f for f in loaded_grouper._files.values()}
        lab = files["lab_environment_monitoring.csv"]

        assert "co2_ppm" in lab.headers
        assert "particle_count" in lab.headers


class TestGroupingWithTestData:
    """Tests for grouping behavior with test data."""

    def test_exact_match_creates_expected_groups(self, loaded_grouper):
        groups = loaded_grouper.group_by_exact_match()

        # Expected: tensile (2), weather_full (2), and 6 singletons
        assert len(groups) == 8

        # Find tensile group
        tensile_group = None
        for g in groups.values():
            if "sample_id" in g.canonical_headers and "yield_strength_mpa" in g.canonical_headers:
                tensile_group = g
                break

        assert tensile_group is not None
        assert len(tensile_group.files) == 2

    def test_75_percent_similarity_merges_reaction_kinetics(self, loaded_grouper):
        groups = loaded_grouper.group_by_similarity(threshold=0.75)

        # Reaction kinetics should merge (88.9% similar)
        reaction_group = None
        for g in groups.values():
            if "experiment_id" in g.canonical_headers:
                reaction_group = g
                break

        assert reaction_group is not None
        assert len(reaction_group.files) == 2

    def test_60_percent_similarity_merges_weather(self, loaded_grouper):
        groups = loaded_grouper.group_by_similarity(threshold=0.60)

        # All 3 weather files should merge (alpha/beta=100%, gamma=62.5%)
        weather_group = None
        for g in groups.values():
            if "station_id" in g.canonical_headers:
                weather_group = g
                break

        assert weather_group is not None
        assert len(weather_group.files) == 3

    def test_grouping_preserves_all_files(self, loaded_grouper):
        groups = loaded_grouper.group_by_similarity(threshold=0.5)

        total_files = sum(len(g.files) for g in groups.values())
        assert total_files == 10


class TestPersistenceWithTestData:
    """Tests for save/load with real test data."""

    def test_save_load_preserves_groupings(self, loaded_grouper):
        loaded_grouper.group_by_similarity(threshold=0.75)
        original_summary = loaded_grouper.summary()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            loaded_grouper.save_groupings(temp_path)

            new_grouper = CSVGrouper()
            new_grouper.load_groupings(temp_path)

            assert new_grouper.summary() == original_summary
        finally:
            Path(temp_path).unlink()

    def test_saved_json_is_human_readable(self, loaded_grouper):
        loaded_grouper.group_by_exact_match()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            loaded_grouper.save_groupings(temp_path)

            with open(temp_path) as f:
                data = json.load(f)

            # Check structure
            assert "groups" in data
            for group_data in data["groups"].values():
                assert "name" in group_data
                assert "canonical_headers" in group_data
                assert "files" in group_data
        finally:
            Path(temp_path).unlink()


class TestIterGroupRows:
    """Tests for iterating over rows in a group."""

    def test_iter_group_rows_yields_all_rows(self, loaded_grouper):
        loaded_grouper.group_by_exact_match()

        # Find tensile test group
        tensile_group_name = None
        for name, g in loaded_grouper.get_groups().items():
            if "yield_strength_mpa" in g.canonical_headers:
                tensile_group_name = name
                break

        assert tensile_group_name is not None

        rows = list(loaded_grouper.iter_group_rows(tensile_group_name))

        # Steel has 6 rows, aluminum has 5 rows = 11 total
        assert len(rows) == 11

    def test_iter_group_rows_returns_dicts(self, loaded_grouper):
        loaded_grouper.group_by_exact_match()

        # Get any group with files
        group_name = list(loaded_grouper.get_groups().keys())[0]

        for file_path, row in loaded_grouper.iter_group_rows(group_name):
            assert isinstance(file_path, str)
            assert isinstance(row, dict)
            break  # Just check first row

    def test_iter_unknown_group_raises(self, loaded_grouper):
        loaded_grouper.group_by_exact_match()

        with pytest.raises(ValueError, match="Unknown group"):
            list(loaded_grouper.iter_group_rows("nonexistent"))


class TestProcessorWithTestData:
    """Tests for processor functionality with real data."""

    def test_processor_receives_correct_files(self, loaded_grouper):
        loaded_grouper.group_by_exact_match()

        # Find weather group
        weather_group_name = None
        for name, g in loaded_grouper.get_groups().items():
            if "station_id" in g.canonical_headers and len(g.files) == 2:
                weather_group_name = name
                break

        assert weather_group_name is not None

        received_paths = []

        def capture_processor(paths):
            received_paths.extend(paths)

        loaded_grouper.register_processor(weather_group_name, capture_processor)
        loaded_grouper.process_group(weather_group_name)

        assert len(received_paths) == 2
        assert all("weather" in p for p in received_paths)


class TestDelimiterDetection:
    """Tests for CSV delimiter detection."""

    def test_comma_delimiter_detected(self, loaded_grouper):
        # All test files use comma delimiter
        for f in loaded_grouper._files.values():
            assert f.delimiter == ","

    def test_handles_different_delimiters(self, grouper):
        # Create a semicolon-delimited file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as f:
            f.write("a;b;c\n")
            f.write("1;2;3\n")
            temp_path = f.name

        try:
            temp_dir = Path(temp_path).parent
            files = grouper.scan_directory(temp_dir, pattern=Path(temp_path).name)

            assert len(files) == 1
            assert files[0].delimiter == ";"
            assert files[0].headers == ["a", "b", "c"]
        finally:
            Path(temp_path).unlink()

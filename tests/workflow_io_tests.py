"""
Unit tests for the catalog initialization methodology. This test suite only covers the file I/O. Tests for DAG creation
and execution, a longer-running process, are in a separate file.
"""

import unittest
# import pytest
import json
import os
import shutil

import sys; sys.path.insert(0, './../')
# noinspection PyUnresolvedReferences
from urban_physiology_toolkit.workflow_utils import (init_catalog, generate_data_package_from_glossary_entry,
                                                     finalize_catalog)


class TestGeneratingDataPackagesFromGlossaryEntries(unittest.TestCase):
    """
    Tests that glossary resource entries are processed into data packages correctly. Two cases at the moment: CSV
    files, which get marked as complete, and "other" files, which are marked incomplete.
    """
    # TODO: Mark geospatial files complete as well.

    def setUp(self):
        self.common_keys = {'preferred_mimetype', 'column_names', 'datapackage_version', 'title', 'created',
                            'contributors', 'name', 'filesize', 'licenses', 'sources', 'protocol', 'views',
                            'last_updated', 'keywords', 'description', 'dependencies', 'landing_page',
                            'topics_provided', 'page_views', 'resources', 'columns', 'available_formats', 'rows',
                            'version', 'preferred_format', 'maintainers', 'publishers', 'complete'}

    def test_csv(self):
        """
        Tests that data packages are generated for CSV files, and that the resultant package is marked complete.
        """
        with open("data/csv_glossary_entry.json", "r") as f:
            glossary_entry = json.load(f)

        result = generate_data_package_from_glossary_entry(glossary_entry)
        assert set(result.keys()) == self.common_keys
        assert result['resources'][0]['path'] == 'data.csv'
        assert 'url' in result['resources'][0]
        assert result['complete']

    # def test_geojson(self):
    #     """
    #     Tests that data packages are generated for CSV files, and that the resultant package is marked complete.
    #     """
    #     with open("data/geojson_glossary_entry.json", "r") as f:
    #         glossary_entry = json.load(f)
    #
    #     result = generate_data_package_from_glossary_entry(glossary_entry)
    #     assert set(result.keys()) == self.common_keys
    #     assert result['resources'][0]['path'] == 'data.csv'
    #     assert 'url' in result['resources'][0]
    #     assert result['complete']

    def test_xls(self):
        """
        Tests that data packages are generated for XLS files, and that the resultant package is marked incomplete.
        XLS here is a stand-in for any data type that is "generic" in the sense that no complete depositor is written
        for it by the automatic scripts.
        """
        with open("data/xls_glossary_entry.json", "r") as f:
            glossary_entry = json.load(f)

        result = generate_data_package_from_glossary_entry(glossary_entry)

        assert set(result.keys()) == self.common_keys
        assert result['resources'] == []
        assert not result['complete']


class TestBlobIO(unittest.TestCase):
    """
    Test that write behavior is as-expected when passed a glossary of entries corresponding solely with the files
    contained in a single blob resource. In this case we expect that only one catalog folder, one task folder,
    one datapackage, and one depositor be written to the file.
    """

    def setUp(self):
        os.mkdir("temp")

    def test_init(self):
        init_catalog("./data/blob_resource_glossary.json", "temp")

        with open("./data/blob_resource_glossary.json", "r") as f:
            glossary = json.load(f)

        catalog_folders = os.listdir("./temp/catalog")
        task_folders = os.listdir("./temp/tasks")

        n_catalog = len(catalog_folders)
        n_tasks = len(task_folders)
        n_expected = len(set([entry['resource'] for entry in glossary]))

        assert n_catalog == n_expected
        assert n_tasks == n_expected

    def tearDown(self):
        shutil.rmtree("temp")


class TestRepeatedResourceNameIO(unittest.TestCase):
    """
    Only the download URL for a resource is guaranteed to be unique. All other resource attributes, including the
    name, vary, and may even collide. Since our folder structures parse resources into folder names based on resource
    names, not their download URLs, we need to ascertain that the collision-avoidance mechanism written into the
    requisite process works.

    In this case we are given a glossary with two resources, each of which is given the same name. We should
    nevertheless get two separate files in our catalog folder structure output.
    """

    def setUp(self):
        os.mkdir("temp")

    def test_init(self):
        init_catalog("./data/doubled_resource_name_glossary.json", "temp")

        with open("./data/doubled_resource_name_glossary.json", "r") as f:
            glossary = json.load(f)

        catalog_folders = os.listdir("./temp/catalog")
        task_folders = os.listdir("./temp/tasks")

        n_catalog = len(catalog_folders)
        n_tasks = len(task_folders)
        n_expected = len(set([entry['resource'] for entry in glossary]))

        assert n_catalog == n_expected
        assert n_tasks == n_expected

    def tearDown(self):
        shutil.rmtree("temp")


class TestFullInitializationIO(unittest.TestCase):
    """
    Smoke test. Makes sure that folder initialization fires and works for a large glossary---in this case,
    a glossarization of the New York City open data portal dating from mid-June.
    """

    def setUp(self):
        os.mkdir("temp")

    def test_init(self):
        init_catalog("./data/full_glossary.json", "temp")

        with open("./data/full_glossary.json", "r") as f:
            glossary = json.load(f)

        catalog_folders = os.listdir("./temp/catalog")
        task_folders = os.listdir("./temp/tasks")

        n_catalog = len(catalog_folders)
        n_tasks = len(task_folders)
        n_expected = len(set([entry['resource'] for entry in glossary]))

        assert all([os.path.isfile("./temp/catalog/{0}/datapackage.json".format(cf)) for cf in
                    os.listdir("./temp/catalog")])

        assert n_catalog == n_expected
        assert n_tasks == n_expected

    def tearDown(self):
        shutil.rmtree("temp")


class TestFinalization(unittest.TestCase):
    """
    Test that finalization works as expected.
    """

    def setUp(self):
        os.mkdir("temp")

    def test_init(self):
        init_catalog("./data/double_resource_glossary.json", "temp")

        with open("./data/double_resource_glossary.json", "r") as f:
            glossary = json.load(f)

        catalog_before = len(os.listdir("./temp/catalog"))
        tasks_before = len(os.listdir("./temp/tasks"))

        finalize_catalog("temp")

        catalog_after = len(os.listdir("./temp/catalog"))
        tasks_after = len(os.listdir("./temp/tasks"))

        assert catalog_before != catalog_after
        assert tasks_before != tasks_after

    def tearDown(self):
        shutil.rmtree("temp")

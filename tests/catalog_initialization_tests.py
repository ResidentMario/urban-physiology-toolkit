"""
Unit tests for catalog initialization methodology.
"""

import unittest
# import pytest
import json
import os
import shutil

import sys; sys.path.insert(0, './../')
# noinspection PyUnresolvedReferences
from urban_physiology_toolkit.tools import init_catalog


class TestBlobPrepping(unittest.TestCase):
    """
    Tests that folder initialization for a glossary consisting only of a single's blobs dataset entries results in
    only one depositor being written to the file, the one downloading the overall ZIP/TAR/whatever.
    """

    def setUp(self):
        os.mkdir("temp")

    def test_init(self):
        # init_catalog("./data/full_glossary.json", "temp")
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


class TestRepeatedResourceNamePrepping(unittest.TestCase):
    """
    Tests that if a glossary contains two distinct resources which resolve to the same folder name,
    they are correctly placed into two different folders.
    """

    def setUp(self):
        os.mkdir("temp")

    def test_init(self):
        # init_catalog("./data/full_glossary.json", "temp")
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


class TestFullInitializationPrepping(unittest.TestCase):
    """
    Tests that folder initialization works for a large glossary---in this case, a glossarization of the New York City
    open data portal dating from mid-June.
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

        assert n_catalog == n_expected
        assert n_tasks == n_expected

    def tearDown(self):
        shutil.rmtree("temp")

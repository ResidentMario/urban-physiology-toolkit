"""
Unit tests for catalog initialization methodology.
"""

import unittest
# import pytest
import json
import os
import shutil

import sys; sys.path.insert(0, '../../')
# noinspection PyUnresolvedReferences
from urban_physiology_toolkit.tools import init_catalog


class TestCatalogInitialization(unittest.TestCase):
    def setUp(self):
        os.mkdir("temp")

    def test_init(self):
        init_catalog("./data/example_full_glossary.json", "temp")

        with open("./data/example_full_glossary.json", "r") as f:
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
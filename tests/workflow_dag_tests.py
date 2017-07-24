"""
Unit tests for the DAG generated as a part of the portal localization workflow. See also the IO tests.
"""

import os
import unittest
import shutil

import sys; sys.path.insert(0, './../')
# noinspection PyUnresolvedReferences
from urban_physiology_toolkit.workflow_utils import (init_catalog, update_dag)
# from airscooter.orchestration import configure


class TestSimpleResourceDAGSmokeTest(unittest.TestCase):
    """
    Quick smoke test. Makes sure that writing the DAG succeeds, but doesn't evaluate it.
    """

    def setUp(self):
        os.mkdir("temp")
        os.mkdir("temp/.airflow/")
        # configure(local_folder="temp/.airflow/", init=True)

    def test_dag_creation(self):
        init_catalog("./data/blob_resource_glossary.json", "temp")
        update_dag(root="./temp")

        assert set(os.listdir("./temp/.airflow")) == {'airscooter.yml', 'datablocks_dag.py'}

    def tearDown(self):
        shutil.rmtree("temp")

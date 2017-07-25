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

        assert set(os.listdir("./temp/.airflow")) == {'airscooter.yml', 'dags'}
        assert os.path.exists("./temp/.airflow/dags/airscooter_dag.py")

    def tearDown(self):
        shutil.rmtree("temp")


# The following three tests make sure that all three of the supported task types --- Python files,
# Jupyter notebooks, and Bash scripts --- work properly.
#
# These tests do NOT evaluate the resultant script files, nor do they evaluate the DAG itself. This is because
# ascertaining that the files we write actually work is fairly easy to do via visual inspection, and observing the
# external state of running these jobs without introducing network dependencies would generally be pretty tedious
# to implement.

class TestPythonFormat(unittest.TestCase):
    def setUp(self):
        os.mkdir("temp")
        os.mkdir("temp/.airflow/")
        init_catalog("./data/blob_resource_glossary.json", "temp")

    def test_python(self):
        update_dag(root="./temp")

        assert set(os.listdir("./temp/.airflow")) == {'airscooter.yml', 'dags'}
        assert os.path.exists("./temp/.airflow/dags/airscooter_dag.py")
        assert set(os.listdir("./temp/tasks/2009-school-survey")) == {'depositor.py', 'transform.py'}

    def tearDown(self):
        shutil.rmtree("temp")


class TestIPYNBFormat(unittest.TestCase):
    def setUp(self):
        os.mkdir("temp")
        os.mkdir("temp/.airflow/")
        init_catalog("./data/blob_resource_glossary.json", "temp")

    def test_ipynb(self):
        pass
        # TODO: Implement! Write in model Jupyter depositor and transform notebooks, then make sure update_dag runs.
        # update_dag(root="./temp")

        # assert set(os.listdir("./temp/.airflow")) == {'airscooter.yml', 'datablocks_dag.py'}
        # assert os.path.exists("./temp/.airflow/dags/airscooter_dag.py")
        # assert set(os.listdir("./temp/tasks/2009-school-survey")) == {'depositor.py', 'transform.py'}

    def tearDown(self):
        shutil.rmtree("temp")


class TestBashFormat(unittest.TestCase):
    def setUp(self):
        os.mkdir("temp")
        os.mkdir("temp/.airflow/")
        init_catalog("./data/blob_resource_glossary.json", "temp")

    def test_ipynb(self):
        pass
        # TODO: Implement! Write in model Bash depositor and transform shell scripts, then make sure update_dag runs.
        # update_dag(root="./temp")

        # assert set(os.listdir("./temp/.airflow")) == {'airscooter.yml', 'datablocks_dag.py'}
        # assert os.path.exists("./temp/.airflow/dags/airscooter_dag.py")
        # assert set(os.listdir("./temp/tasks/2009-school-survey")) == {'depositor.py', 'transform.py'}

    def tearDown(self):
        shutil.rmtree("temp")

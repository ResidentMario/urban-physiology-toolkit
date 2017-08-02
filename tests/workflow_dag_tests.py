"""
Unit tests for the DAG generated as a part of the portal localization workflow. See also the IO tests.
"""

import os
import unittest
import shutil

import sys; sys.path.insert(0, './../')
# noinspection PyUnresolvedReferences
from urban_physiology_toolkit.workflow import (init_catalog, update_dag)


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
# TODO: Implement those tests anyway.

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
        os.remove("temp/tasks/2009-school-survey/depositor.py")
        shutil.copy("fixtures/depositor.ipynb", "temp/tasks/2009-school-survey/depositor.ipynb")

        os.remove("temp/tasks/2009-school-survey/transform.py")
        shutil.copy("fixtures/transform.ipynb", "temp/tasks/2009-school-survey/transform.ipynb")

        update_dag(root="./temp")

        assert set(os.listdir("./temp/.airflow")) == {'airscooter.yml', 'dags'}
        assert os.path.exists("./temp/.airflow/dags/airscooter_dag.py")
        assert set(os.listdir("./temp/tasks/2009-school-survey")) == {'depositor.ipynb', 'transform.ipynb'}

    def tearDown(self):
        shutil.rmtree("temp")


class TestBashFormat(unittest.TestCase):
    def setUp(self):
        os.mkdir("temp")
        os.mkdir("temp/.airflow/")
        init_catalog("./data/blob_resource_glossary.json", "temp")

    def test_bash(self):
        # Replace the base Python depositor and transform with model Bash ones.

        os.remove("temp/tasks/2009-school-survey/depositor.py")
        with open("temp/tasks/2009-school-survey/depositor.sh", "w") as f:
            f.write("wget https://data.cityofnewyork.us/download/ens7-ac7e/application%2Fzip -O data.zip\n"
                    "OUTPUT=(data.zip)")

        os.remove("temp/tasks/2009-school-survey/transform.py")
        with open("temp/tasks/2009-school-survey/transform.sh", "w") as f:
            f.write("unzip data.zip -d .\n"
                    "OUTPUT=(foo.csv bar.csv)")

        update_dag(root="./temp")

        assert set(os.listdir("./temp/.airflow")) == {'airscooter.yml', 'dags'}
        assert os.path.exists("./temp/.airflow/dags/airscooter_dag.py")
        assert set(os.listdir("./temp/tasks/2009-school-survey")) == {'depositor.sh', 'transform.sh'}

    def tearDown(self):
        shutil.rmtree("temp")

"""
Unit tests for socrata_glossarizer module macro-level subcomponents.
"""

import unittest
import pytest
import json

import sys; sys.path.insert(0, '../')
from urban_physiology_toolkit import socrata_glossarizer
import requests


class TestGetPortalMetadata(unittest.TestCase):
    def test_get_portal_metadata(self):
        # Test whether or not the metadata returned by our processor still fits the format of the metadata that we
        # expect from Socrata. Note that similar tests are a to-do for the pysocrata test suite.

        tables = socrata_glossarizer._get_portal_metadata("data.cityofnewyork.us",
                                                          "auth/nyc-open-data.json",
                                                          "table")
        assert len(tables) > 1000

        # Take an example dataset. Note that if this example dataset ever gets deleted, this test will fail,
        # so it will need to be updated then.
        toi = next(table for table in tables if table['permalink'].split("/")[-1] == "f4rp-2kvy")

        # Now make sure the schema is still the same.
        assert set(toi.keys()) == {'permalink', 'link', 'metadata', 'resource', 'classification', 'owner'}
        assert isinstance(toi['permalink'], str)
        assert isinstance(toi['link'], str)
        assert set(toi['metadata'].keys()) == {'license', 'domain'}
        assert set(toi['classification'].keys()) == {'tags', 'categories', 'domain_category', 'domain_tags',
                                                     'domain_metadata'}
        assert set(toi['resource'].keys()) == {'parent_fxf', 'columns_description', 'columns_field_name', 'provenance',
                                               'description', 'view_count', 'name', 'id', 'type', 'updatedAt',
                                               'download_count', 'attribution', 'createdAt', 'page_views',
                                               'columns_name'}


class TestResourcify(unittest.TestCase):
    def test_resourcify_table(self):
        """
        Test that the metadata-to-resource method works and that the resource schema is what we expected it to be.
        """
        with open("data/example_metadata-f4rp-2kvy.json", "r") as fp:
            toi_metadata = json.load(fp)
        resource = socrata_glossarizer._resourcify(toi_metadata, domain="data.cityofnewyork.us", endpoint_type="table")
        assert resource.keys() == {'sources', 'flags', 'topics_provided', 'created', 'name', 'protocol', 'description',
                                   'column_names', 'page_views', 'resource', 'last_updated', 'keywords_provided',
                                   'landing_page'}


class TestGetSizings(unittest.TestCase):
    def setUp(self):
        self.zip_test_uri = "https://data.cityofnewyork.us/api/views/q68s-8qxv/files/511dbe78-65f3-470f-9cc8" \
                            "-c1415f75a6e6?filename=AnnAvg1_7_300mSurfaces.zip"
        self.csv_test_uri = "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD"
        self.zip_fail_test_uri = "https://data.cityofnewyork.us/api/views/ft4n-yqee/files/3d0f7600-f88a-4a11-8ad9" \
                                 "-707c785caa08?filename=Broadband%20Data%20Dig%20-%20Datasets.zip"

    def test_get_sizings_success_csv(self):
        sizings = socrata_glossarizer._get_sizings(self.csv_test_uri, timeout=20)
        assert sizings and len(sizings) != 0

    def test_get_sizings_success_zip(self):
        # More difficult problem than that of a single CSV file.
        sizings = socrata_glossarizer._get_sizings(self.zip_test_uri, timeout=20)
        assert sizings and len(sizings) != 0

    def test_get_sizing_fail(self):
        """
        The error being raises is not something I am comfortable with, but it is what works for the moment and what
        the module depends on. See the notes in timeout_process for more details.
        """
        with pytest.raises(requests.exceptions.ChunkedEncodingError):
            socrata_glossarizer._get_sizings(self.zip_fail_test_uri, timeout=1)


class TestGlossarize(unittest.TestCase):
    """
    Test that the resource-to-glossaries method works in isolation and that the glossaries schema is what we
    expected it to be.

    These tests are network dependent and can be a bit flaky, so if one fails, try running it again just to be sure.

    Also, to generate a new endpoint (in case one gets deleted), run something akin to the following in a REPL:

        socrata_glossarizer._get_portal_metadata("data.cityofnewyork.us", "auth/nyc-open-data.json", "links")

    And then pick and save one of the list items to a file.
    """
    def setUp(self):
        self.table_glossary_keys = {'available_formats', 'resource', 'page_views', 'sources', 'created', 'description',
                                    'protocol', 'last_updated', 'dataset', 'column_names', 'rows', 'topics_provided',
                                    'preferred_mimetype','name', 'preferred_format', 'columns', 'flags',
                                    'keywords_provided', 'landing_page'}
        self.nontable_glossary_keys = {'resource', 'column_names', 'created', 'page_views', 'landing_page', 'flags',
                                       'keywords_provided', 'name', 'description', 'last_updated', 'filesize',
                                       'dataset','preferred_format', 'protocol', 'sources', 'preferred_mimetype',
                                       'topics_provided'}

    def test_glossarize_table(self):
        with open("data/example_metadata-f4rp-2kvy.json", "r") as fp:
            resource = socrata_glossarizer._resourcify(json.load(fp), "data.cityofnewyork.us", "table")

        glossarized_resource = socrata_glossarizer._glossarize_table(resource, "opendata.cityofnewyork.us")
        assert glossarized_resource[0].keys() == self.table_glossary_keys

    def test_glossarize_nontable_blob(self):
        with open("data/example_metadata-q68s-8qxv.json", "r") as fp:
            resource = socrata_glossarizer._resourcify(json.load(fp), "data.cityofnewyork.us", "blob")

        glossarized_resource = socrata_glossarizer._glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == self.nontable_glossary_keys

    def test_glossarize_nontable_geospatial_dataset(self):
        with open("data/example_metadata-ghq4-ydq4.json", "r") as fp:
            resource = socrata_glossarizer._resourcify(json.load(fp), "data.cityofnewyork.us", "geospatial dataset")

        glossarized_resource = socrata_glossarizer._glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == self.nontable_glossary_keys

    def test_glossarize_landing_page_external_link(self):
        """
        External links pointing to landing pages should be ignored by the glossarizer.
        """
        with open("data/example_metadata-mmu8-8w8b.json", "r") as fp:
            resource = socrata_glossarizer._resourcify(json.load(fp), "data.cityofnewyork.us", "link")

        glossarized_resource = socrata_glossarizer._glossarize_nontable(resource, 20)
        assert len(glossarized_resource) == 0

    def test_glossarize_data_landing_page(self):
        """
        External links pointing to landing pages with data on them should be pulled in however.
        """
        with open("data/example_metadata-p94q-8hxh.json", "r") as fp:
            resource = socrata_glossarizer._resourcify(json.load(fp), "data.cityofnewyork.us", "link")

        glossarized_resource = socrata_glossarizer._glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == self.nontable_glossary_keys

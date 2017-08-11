"""
Unit tests for the Socrata glossarizer.
"""

import json
import sys; sys.path.append('../')
import unittest

from urban_physiology_toolkit.glossarizers import socrata


def test_get_portal_metadata():
    """
    Test whether or not the metadata returned by our processor still fits the format of the metadata that we
    expect from Socrata. Note that similar tests are a to-do for the pysocrata test suite.

    This test is network-dependent and will fail if the Socrata portal metadata stream goes down.
    """

    tables = socrata._get_portal_metadata("data.cityofnewyork.us", "auth/nyc-open-data.json")
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
    assert set(toi['resource'].keys()) == {'view_count', 'columns_field_name', 'provenance', 'id', 'download_count',
                                           'description', 'createdAt', 'page_views', 'columns_name', 'parent_fxf',
                                           'updatedAt', 'type', 'columns_datatype', 'name', 'attribution',
                                           'columns_description'}


def test_resourcify():
    """
    The resourcify method transforms the metadata we get by processing what we get from querying Socrata (above)
    into resource entries fit for inclusion in our resource list. This test checks that the schema of the result
    that we get is still what we expect it to be.
    """
    with open("data/example_metadata-f4rp-2kvy.json", "r") as fp:
        toi_metadata = json.load(fp)
    resource = socrata._resourcify(toi_metadata, domain="data.cityofnewyork.us")
    assert resource.keys() == {'sources', 'flags', 'topics_provided', 'created', 'name', 'protocol', 'description',
                               'column_names', 'page_views', 'resource', 'last_updated', 'keywords_provided',
                               'landing_page', 'resource_type'}


class TestGlossarize(unittest.TestCase):
    """
    Tests that the methods for turning resource entries into glossary entries (`_glossarize_table` and
    `_glossarize_nontable`) work as expected. These tests are network dependent and can be a bit flaky unfortunately,
    so if one fails, try running it again just to be sure.

    These tests will start to fail if one of the endpoints they rely on gets deleted (unlikely, but possible). After
    troubleshooting this, replace that resource with another one to keep going.
    """
    def setUp(self):
        self.table_glossary_keys = {'available_formats', 'resource', 'page_views', 'sources', 'created', 'description',
                                    'protocol', 'last_updated', 'dataset', 'column_names', 'rows', 'topics_provided',
                                    'preferred_mimetype','name', 'preferred_format', 'columns', 'flags',
                                    'keywords_provided', 'landing_page', 'resource_type'}
        self.nontable_glossary_keys = {'resource', 'column_names', 'created', 'page_views', 'landing_page', 'flags',
                                       'keywords_provided', 'name', 'description', 'last_updated', 'filesize',
                                       'dataset','preferred_format', 'protocol', 'sources', 'preferred_mimetype',
                                       'topics_provided', 'resource_type'}

    def test_glossarize_table(self):
        with open("data/example_metadata-f4rp-2kvy.json", "r") as fp:
            resource = socrata._resourcify(json.load(fp), "data.cityofnewyork.us")

        glossarized_resource = socrata._glossarize_table(resource, "opendata.cityofnewyork.us")
        assert glossarized_resource[0].keys() == self.table_glossary_keys

    def test_glossarize_nontable_blob(self):
        with open("data/example_metadata-q68s-8qxv.json", "r") as fp:
            resource = socrata._resourcify(json.load(fp), "data.cityofnewyork.us")

        glossarized_resource = socrata._glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == self.nontable_glossary_keys

    def test_glossarize_nontable_geospatial_dataset(self):
        with open("data/example_metadata-ghq4-ydq4.json", "r") as fp:
            resource = socrata._resourcify(json.load(fp), "data.cityofnewyork.us")

        glossarized_resource = socrata._glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == self.nontable_glossary_keys

    def test_glossarize_landing_page_external_link(self):
        """
        External links pointing to HTML landing pages should be ignored by the glossarizer.
        """
        with open("data/example_metadata-mmu8-8w8b.json", "r") as fp:
            resource = socrata._resourcify(json.load(fp), "data.cityofnewyork.us")

        glossarized_resource = socrata._glossarize_nontable(resource, 20)
        assert len(glossarized_resource) == 0

    def test_glossarize_externally_hosted_resource(self):
        """
        External links pointing to externally-hosted resources should be pulled in however.
        """
        with open("data/example_metadata-p94q-8hxh.json", "r") as fp:
            resource = socrata._resourcify(json.load(fp), "data.cityofnewyork.us")

        glossarized_resource = socrata._glossarize_nontable(resource, 20)
        assert glossarized_resource[0].keys() == self.nontable_glossary_keys

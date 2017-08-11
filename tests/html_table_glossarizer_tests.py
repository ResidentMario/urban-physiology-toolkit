"""
Unit tests for the HTML table glossarizer.
"""

import json
import sys
sys.path.append('../')

import unittest

from urban_physiology_toolkit.glossarizers import html
import requests_mock


def read_file(fp):
    """Helper function. Read a file from the /data folder as string."""
    with open('data/' + fp, 'rb') as f:
        return f.read()


class TestExtractLinks(unittest.TestCase):
    """Tests for the the `extract_links` method."""
    def test_categorical_table(self):
        """
        Tests an unfiltered fetch.
        """

        url = "http://www.mdps.gov.qa/en/statistics1/Pages/default.aspx"

        with requests_mock.Mocker() as mock:
            # Interdict network requests to retrieve data from the localized store instead.
            mock.get(url, content=read_file("example-categorical-table.html"))

            results = html._extract_links(url, "div.population-census")

            assert len(results) == 42
            assert all([isinstance(r, str) for r in results])

    def test_link_table(self):
        """
        Tests a filtered fetch.
        """
        url = "http://www.mdps.gov.qa/en/statistics1/pages/topicslisting.aspx?parent=General&child=QIF"

        def filter_func(l): return l.split(".")[-1] == "pdf"

        with requests_mock.Mocker() as mock:
            # Interdict network requests to retrieve data from the localized store instead.
            mock.get(url, content=read_file("example-link-table.html"))

            results = html._extract_links(url, "div.archive-section", filter=filter_func)

            assert len(results) == 32
            assert all([r.split(".")[-1] == "pdf" for r in results])

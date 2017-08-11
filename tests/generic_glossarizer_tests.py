"""
Tests for glossarization components work across different glossarizers. In paticular, this file tests functions in
the `urban_physiology.glossarizers.utils` namespace.
"""

import sys; sys.path.append('../')
import unittest

import pytest

import urban_physiology_toolkit.glossarizers.utils as utils
import requests


class TestGetSizings(unittest.TestCase):
    """
    Proper glossary entries require size information, either in the form of a count of the number or rows and
    columns, or in the form of an explicit filesize. Ideally, the latter information will be available in the
    download URL header or the former will be available in the API.

    However, this may not be the case. If not, then the worst-case routine for getting that information is
    downloading with a timeout. This is exactly what the `get_sizings` routine in
    `urban_physiology_toolkit.glossarizers.utils` does.
    """

    def setUp(self):
        self.zip_test_uri = "https://data.cityofnewyork.us/api/views/q68s-8qxv/files/511dbe78-65f3-470f-9cc8" \
                            "-c1415f75a6e6?filename=AnnAvg1_7_300mSurfaces.zip"
        self.csv_test_uri = "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.csv?accessType=DOWNLOAD"
        self.zip_fail_test_uri = "https://data.cityofnewyork.us/api/views/ft4n-yqee/files/3d0f7600-f88a-4a11-8ad9" \
                                 "-707c785caa08?filename=Broadband%20Data%20Dig%20-%20Datasets.zip"

    def test_get_sizings_success_csv(self):
        sizings = utils.get_sizings(self.csv_test_uri, timeout=20)
        assert sizings and len(sizings) != 0

    def test_get_sizings_success_zip(self):
        # More difficult problem than that of a single CSV file.
        sizings = utils.get_sizings(self.zip_test_uri, timeout=20)
        assert sizings and len(sizings) != 0

    def test_get_sizing_fail(self):
        """
        The error being raises is not something I am comfortable with, but it is what works for the moment and what
        the module depends on. See the notes in timeout_process for more details.
        """
        with pytest.raises(requests.exceptions.ChunkedEncodingError):
            utils.get_sizings(self.zip_fail_test_uri, timeout=1)


class TestGenericGlossarizeResource(unittest.TestCase):
    """
    The brute-force way of generating a glossary entry or entries out of a reference list entry is to use
    `generic_glossarize_resource`, which downloads the resource with a timeout and does the necessary munging to
    beat it into shape. This section tests this method (itself a wrapper of `get_sizings`, tested above).
    """
    # TODO: Implement these tests.
    # TODO: Make socrata._glossarize_nontable a wrapper over this method.
    pass
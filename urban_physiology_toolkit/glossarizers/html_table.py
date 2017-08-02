"""
Glossarizer tools for domains hosting data using (simple ones for now, meaning, non-AJAX) HTML tables.
"""

import requests
import bs4
import itertools
from urban_physiology_toolkit.glossarizers.utils import (preexisting_cache, load_glossary_todo,
                                                         write_resource_file, write_glossary_file)


####################
# HELPER FUNCTIONS #
####################

def _extract_links(url, selector, filter=None):
    """
    Extracts a list of links from a non-AJAX HTML page, optionally applying a filter to the results.

    Parameters
    ----------
    url: str, required
        The URL hosting the table.
    selector: str, required
        A CSS selector for the element links are to be extracted from.
    filter: unfunc, optional
        If provided, this function will be applied to each link in the list prior to returning.

    Returns
    -------
    A list of links extracted from the page.
    """
    soup = bs4.BeautifulSoup(requests.get(url).content, 'html.parser')
    matches = soup.select(selector)
    hrefs = itertools.chain(*[match.find_all("a") for match in matches])
    links = [a['href'] for a in hrefs]

    return [link for link in links if filter(link)] if filter else links


#######################
# USER-FACING METHODS #
#######################

def get_resource_list(domain=None):
    """
    Generates a resource list for the given domain.

    Domains are implemented by hand on a per-provider basis, so this method is a sequence of if-else statements.

    Returns
    -------
    Returns the resource list for the given input.
    """
    if domain == "http://www.mdps.gov.qa/en/statistics1/Pages/default.aspx":
        return get_qatari_ministry_of_planning_and_statistics_resource_list()
    # All other HTML grabbers have not been implemented yet.
    elif domain is None:
        raise ValueError("No domain was provided.")
    else:
        raise NotImplementedError("Glossarization has not yet been implemented for the {0} domain.".format(domain))


def write_resource_list(domain=None, out=None, use_cache=True):
    """
    TODO: This docstring.
    """
    if preexisting_cache(out, use_cache):
        return
    else:
        write_resource_file(get_resource_list(domain=domain), out)


def get_glossary(domain, resource_list, timeout=60):
    pass


# def write_glossary(domain=None, resource_filename=None, glossary_filename=None, timeout=60, use_cache=True):
#     resource_list, glossary = _load_glossary_todo(resource_filename, glossary_filename, use_cache)
#
#     try:
#         resource_list, glossary = get_glossary(resource_list, glossary, domain=domain, endpoint_type=endpoint_type,
#                                                timeout=timeout)
#
#     # Save output.
#     finally:
#         _write_resource_file(resource_list, resource_filename)
#         _write_glossary_file(glossary, glossary_filename)

#####################
# INTERNAL ROUTINES #
#####################

def get_qatari_ministry_of_planning_and_statistics_resource_list():
    pass
"""
Glossarizer tools for domains hosting data using (simple ones for now, meaning, non-AJAX) HTML tables.
"""

import requests
import bs4
import itertools
from urban_physiology_toolkit.glossarizers.utils import (preexisting_cache, load_glossary_todo,
                                                         write_resource_file, write_glossary_file,
                                                         generic_glossarize_resource)
import urllib.parse

from tqdm import tqdm


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
    links = [a['href'] for a in hrefs if 'href' in a.attrs]

    # Make relative link paths absolute.
    def reroot_link(link):
        """Patches relative links over into absolute ones."""
        parsed_link = urllib.parse.urlparse(link)

        if parsed_link.scheme == "":
            return urllib.parse.urljoin(url, link)
        else:
            return link

    links = [reroot_link(link) for link in links]

    # Filter links.
    links = [link for link in links if filter(link)] if filter else links

    # Remove duplicate links and return.
    return list(set(links))


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
    # TODO: Safe failover with https://github.com/rholder/retrying
    if domain == "http://www.mdps.gov.qa/en/statistics1/Pages/default.aspx":
        return get_qatari_ministry_of_planning_and_statistics_resource_list()
    # All other HTML grabbers have not been implemented yet.
    elif domain is None:
        raise ValueError("No domain was provided.")
    else:
        raise NotImplementedError("Glossarization has not yet been implemented for the {0} domain.".format(domain))


def write_resource_list(domain=None, filename=None, use_cache=True):
    """
    TODO: This docstring.
    """
    if preexisting_cache(filename, use_cache):
        return
    else:
        write_resource_file(get_resource_list(domain=domain), filename)


def get_glossary(domain, resource_list=None, glossary=None, timeout=60):
    """
    TODO: This docstring.
    """
    if domain == "http://www.mdps.gov.qa/en/statistics1/Pages/default.aspx":
        return get_qatari_ministry_of_planning_and_statistics_glossary(resource_list, glossary, timeout=timeout)

    # All other HTML grabbers have not been implemented yet.
    elif domain is None:
        raise ValueError("No domain was provided.")
    else:
        raise NotImplementedError("Glossarization has not yet been implemented for the {0} domain.".format(domain))


def write_glossary(domain=None, resource_filename=None, glossary_filename=None, timeout=60, use_cache=True):
    resource_list, glossary = load_glossary_todo(resource_filename, glossary_filename, use_cache)

    try:
        resource_list, glossary = get_glossary(domain, resource_list, glossary, timeout=timeout)

    # Save output.
    finally:
        write_resource_file(resource_list, resource_filename)
        write_glossary_file(glossary, glossary_filename)


#####################
# INTERNAL ROUTINES #
#####################

def get_qatari_ministry_of_planning_and_statistics_resource_list():
    homepage = "http://www.mdps.gov.qa/en/statistics1/Pages/default.aspx"
    cat_links = _extract_links(homepage, "div.population-census")

    def filter_func(l):
        pdf_or_xls = l.split(".")[-1] in ['pdf', 'xls']
        non_social = ('facebook' not in l and 'google' not in l and 'linkedin' not in l and 'twitter' not in l)
        return pdf_or_xls and non_social

    rlinks = list(itertools.chain(*[_extract_links(url, "div.archive-content",
                                                   filter=filter_func) for url in tqdm(cat_links)]))
    return [{'domain': 'http://www.mdps.gov.qa/en/statistics1/Pages/default.aspx',
            'resource': r,
             'flags': []} for r in rlinks]


def get_qatari_ministry_of_planning_and_statistics_glossary(resource_list, glossary, timeout=60):
    for resource in tqdm(resource_list):
        modified_resource, glossarized_resource = generic_glossarize_resource(resource, timeout)
        resource.update(modified_resource)
        glossary += glossarized_resource

    return resource_list, glossary

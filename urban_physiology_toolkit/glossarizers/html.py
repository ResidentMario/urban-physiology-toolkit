"""
HTML glossarizer. Currently limited to non-AJAX HTML tables. A general implementation reference is available online at
https://github.com/ResidentMario/urban-physiology-toolkit/wiki/Glossarization-Notes:-HTML.
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
    Extracts a list of links from an HTML page, optionally applying a filter to the results.

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


#############################
# WRAPPERS AND USER METHODS #
#############################

def get_resource_list(domain=None):
    """
    Generates a resource list for the given domain. Non-IO subroutine of the user-facing `write_resource_list`.

    Parameters
    ----------
    domain: str, required
        A domain. See further the `write_resource_list` docstring.

    Returns
    -------
    Returns the resource list for the given domain.
    """
    # TODO: Harden against network failures using https://github.com/rholder/retrying
    if "mdps.gov.qa/en/statistics1/Pages/default.aspx" in domain:
        return _get_qatari_ministry_of_planning_and_statistics_resource_list()
    # All other HTML grabbers have not been implemented yet.
    elif domain is None:
        raise ValueError("No domain was provided.")
    else:
        raise NotImplementedError("Glossarization has not yet been implemented for the {0} domain.".format(domain))


def write_resource_list(domain=None, filename=None, use_cache=True):
    """
    Creates a resource list for the given domain and writes it to disc.

    Parameters
    ----------
    domain: str, required
        A domain. Must be one of the implemented domains, e.g. "mdps.gov.qa/en/statistics1/Pages/default.aspx".
    filename: str
        The name of the file to write the resource list to.
    use_cache: bool, default True
        If a resource file is already present at `filename` and `use_cache` is `True`, return immediately.
    """
    if preexisting_cache(filename, use_cache):
        return
    else:
        write_resource_file(get_resource_list(domain=domain), filename)


def get_glossary(domain, resource_list=None, glossary=None, timeout=60):
    """
    Fetches and returns a glossary for the given domain.

    Non-IO subroutine of the user-facing `write_glossary`.
    """
    resource_list = [] if resource_list is None else resource_list
    glossary = [] if glossary is None else glossary

    if "mdps.gov.qa/en/statistics1/Pages/default.aspx" in domain:
        return _get_qatari_ministry_of_planning_and_statistics_glossary(resource_list, glossary, timeout=timeout)

    # All other HTML grabbers have not been implemented yet.
    elif domain is None:
        raise ValueError("No domain was provided.")
    else:
        raise NotImplementedError("Glossarization has not yet been implemented for the {0} domain.".format(domain))


def write_glossary(domain=None, resource_filename=None, glossary_filename=None, timeout=60, use_cache=True):
    """
    Use a resource file to write a glossary to disc.

    Parameters
    ----------
    domain: str, required
        The domain landing page URI.
    resource_filename: str
        A path to a resource file to read processing jobs from.
    glossary_filename: str
        A path to a glossary file to write output to.
    timeout: int, default 60
        A timeout on how long the glossarizer can spend downloading a resource before timing it out. This prevents
        occasional very large datasets from overwhelming your CPU. Resources that time out will be populated in the
        glossary with a `filesize` field indicating how long they were downloading for before timing out.
    use_cache: bool, default True
        If a glossary file is already present at `glossary_filename` and `use_cache` is `True`, endpoints already in
        that file will be left untouched and ones that are not will be appended on. If `use_cache` is `False` the
        file will be overwritten instead.
    """
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

def _get_qatari_ministry_of_planning_and_statistics_resource_list():
    """
    Generates a resource list for the Qatar Ministry of Planning and Statistics open datasets.
    """
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


def _get_qatari_ministry_of_planning_and_statistics_glossary(resource_list, glossary, timeout=60):
    """
    Generates a glossary for the Qatar Ministry of Planning and Statistics open datasets.
    """
    for resource in tqdm(resource_list):
        modified_resource, glossarized_resource = generic_glossarize_resource(resource, timeout)
        resource.update(modified_resource)
        glossary += glossarized_resource

    return resource_list, glossary

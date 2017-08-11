"""
Socrata glossarizer. A general implementation reference is available online at
https://github.com/ResidentMario/urban-physiology-toolkit/wiki/Glossarization-Notes:-Socrata.
"""

import json

# import numpy as np
import pandas as pd
import pysocrata
from selenium.common.exceptions import TimeoutException
from tqdm import tqdm

from urban_physiology_toolkit.glossarizers.utils import (preexisting_cache, load_glossary_todo,
                                                         write_resource_file, write_glossary_file, _get_sizings)


def _resourcify(metadata, domain):
    """
    Given raw Socrata API metadata about a certain endpoint, and its domain, return a resource-ified entry for the
    endpoint for inclusion in the resource listing.

    Internal subroutine of the user-facing `write_resource_list`.

    Parameters
    ----------
    metadata: dict, required
        A Socrata portal metadata entry, as would be returned (in a list) by `pysocrata.get_datasets`.

    domain: str, required
        The Socrata portal domain. See the notes in the `write_resource_list` docstring.

    Returns
    -------
    A resource list entry for the given resource.
    """
    # Munge the Socrata API output a bit beforehand: in this case reworking the name references to match our volcab.
    type = metadata['resource']['type']
    volcab_map = {'dataset': 'table', 'href': 'link', 'map': 'geospatial dataset', 'file': 'blob'}
    resource_type = volcab_map[type]

    # Conditional pager import (this inits PhantomJS, don't necessarily want to if we don't have to).
    if resource_type == "blob" or resource_type == "link":
        from .pager import page_socrata_for_resource_link

    endpoint = metadata['resource']['id']

    # The landing_page format is standard.
    landing_page = "https://{0}/d/{1}".format(domain, endpoint)

    # The slug format depends on the API signature, which is in turn dependent on the dataset type.
    if resource_type == "table":
        slug = "https://" + domain + "/api/views/" + endpoint + "/rows.csv?accessType=DOWNLOAD"
    elif resource_type == "geospatial dataset":
        slug = "https://" + domain + "/api/geospatial/" + endpoint + "?method=export&format=GeoJSON"
    else:  # endpoint_type == "blob" or endpoint_type == "link":
        # noinspection PyUnboundLocalVariable
        slug = page_socrata_for_resource_link(domain, landing_page)

    name = metadata['resource']['name']
    description = metadata['resource']['description']
    sources = [metadata['resource']['attribution']]

    created = str(pd.Timestamp(metadata['resource']['createdAt']))
    last_updated = str(pd.Timestamp(metadata['resource']['updatedAt']))
    page_views = metadata['resource']['page_views']['page_views_total']

    column_names = metadata['resource']['columns_name']

    # Endpoints which do not have any domain categories assigned to them do not report this field whatsoever in the
    # API output. To verify this, try inspecting the fields of the data returned from a `pysocrata.get_datasets` run.
    try:
        topics_provided = [metadata['classification']['domain_category']]
    except KeyError:
        topics_provided = []

    keywords_provided = metadata['classification']['domain_tags']

    return {
        'landing_page': landing_page,
        'resource': slug,
        'resource_type': resource_type,
        'protocol': 'https',
        'name': name,
        'description': description,
        'sources': sources,
        'created': created,
        'last_updated': last_updated,
        'page_views': page_views,
        'column_names': column_names,
        'topics_provided': topics_provided,
        'keywords_provided': keywords_provided,
        'flags': []
    }


def _get_portal_metadata(domain, credentials):
    """
    Given a domain, Socrata API credentials for that domain, and a type of endpoint of interest, returns the metadata
    provided by the portal. Internal subroutine of the user-facing `write_resource_list` method. Wraps
    `pysocrata.get_datasets`.
    """
    # Load credentials.
    with open(credentials, "r") as fp:
        auth = json.load(fp)
    auth['domain'] = domain

    # If the metadata doesn't already exist, use pysocrata to fetch portal metadata. Otherwise, use what's provided.
    resources = pysocrata.get_datasets(**auth)

    # We exclude stories---this is a type of resource the Socrata API considers to be a dataset that we are not
    # interested in.
    resources = [d for d in resources if d['resource']['type'] != 'story']

    # We also exclude community-generated datasets.
    resources = [r for r in resources if r['resource']['provenance'] != 'community']

    return resources


def get_resource_list(domain, credentials):
    """
    Given a portal domain and login credentials thereof, generate and return a resource list for this domain.

    Non-IO subroutine of the user-facing `write_resource_list` method.
    """
    resources_metadata = _get_portal_metadata(domain, credentials)

    # Convert the pysocrata output to our data representation using resourcify.
    resources = []
    for metadata in tqdm(resources_metadata):
        resources.append(_resourcify(metadata, domain))

    return resources


def write_resource_list(domain="data.cityofnewyork.us", filename="resource-list.json", use_cache=True,
                        credentials=None):
    """
    Creates a resource list for the given Socrata domain and writes it to disc.

    Parameters
    ----------
    domain: str, default "data.cityofnewyork.us"
        The open data portal root URI. This should be the root URI that is used by endpoints on the portal,
        which may *not* be the same as the URI used for the landing page.
    filename: str
        The name of the file to write the resource list to.
    use_cache: bool, default True
        If a resource file is already present at `filename` and `use_cache` is `True`, endpoints already in that
        file will be left untouched and ones that are not will be appended on. If `use_cache` is `False` the file
        will be overwritten instead.
    credentials: str
        A path to the credentials file. This should be a JSON file containing a Socrata API token, as one is
        necessary in order to make use of Socrata's API. A minimal credentials file looks like this:

            {"token": "randomcharacters"}
    """
    # If the file already exists and we specify `use_cache=True`, simply return.
    if preexisting_cache(filename, use_cache):
        return

    # Otherwise generate to file and exit.
    roi_repr = []
    roi_repr += get_resource_list(domain, credentials)
    write_resource_file(roi_repr, filename)


def _glossarize_table(resource_entry, domain, driver=None, timeout=60):
    """
    Given a tabular resource entry, a domain, and a PhantomJS driver, creates and returns a glossary entry for that
    resource. Internal subroutine to `_write_glossary`.

    Parameters
    ----------
    resource_entry: dict, required
        The resource entry to be glossarized. Must correspond with a tabular resource and must contain a `landing_page`
        key.
    domain: str, required
        The open data portal landing page URI. See the `_write_glossary` docstring for particularities.
    driver: PhantomJS driver, recommended
        A `selenium` PhantomJS driver. If this parameter is left blank a new driver will be created and then exited
        out of during the course of running this method. This is useful in testing but heavily inadvisable in
        production.
    timeout: int, default 60
        A timeout on how long the glossarizer can spend attempting to download a Socrata portal dataset landing page
        before giving up. This UI scrape is necessary to "size up" the table and provide `rows` and `columns` fields
        in the method output.

    Returns
    -------
    If the method executes successfully, returns a glossary entry for the given resource that is fit for inclusion
    in the given glossary. If the method fails because the endpoint in question was removed or made private (in
    which case a 301 Redirect to the portal landing page is issued), prints (!) a warning and returns an empty list.
    If the method fails because the headless request to the resource landing page took too long, prints (!) a
    warning and returns a similarly empty list.
    """
    from .pager import page_socrata_for_endpoint_size, DeletedEndpointException

    # If a PhantomJS driver has not been initialized (via import), initialize it now.
    driver_passed = bool(driver)
    if not driver_passed:
        from .pager import driver

    # TODO: Raise actual warnings here (instead of emitting print statements).
    try:
        rowcol = page_socrata_for_endpoint_size(domain, resource_entry['landing_page'], timeout=timeout)
    except DeletedEndpointException:
        print("WARNING: the '{0}' endpoint was deleted.".format(resource_entry['landing_page']))
        resource_entry['flags'].append('removed')
        return []
    except TimeoutException:
        print("WARNING: the '{0}' endpoint took too long to process.".format(resource_entry['landing_page']))
        resource_entry['flags'].append('removed')
        return []

    # If no errors were caught, write and return. Uncaught (fatal) errors are sent to the `_get_glossary` outer
    # finally block.

    # Remove the "processed" flag from the resource going into the glossaries, if one exists.
    glossarized_resource = resource_entry.copy()
    glossarized_resource['flags'] = [flag for flag in glossarized_resource['flags'] if flag != 'processed']

    # Attach sizing information.
    glossarized_resource['rows'] = rowcol['rows']
    glossarized_resource['columns'] = rowcol['columns']

    # Attach format information.
    glossarized_resource['available_formats'] = ['csv', 'json', 'rdf', 'rss', 'tsv', 'xml']
    glossarized_resource['preferred_format'] = 'csv'
    glossarized_resource['preferred_mimetype'] = 'text/csv'

    # If a fatal error was caught the data gets sent to the outer (`get_glossary`) finally block.
    glossarized_resource['dataset'] = '.'

    if not driver_passed:
        driver.quit()

    return [glossarized_resource]


def _glossarize_nontable(resource_entry, timeout=60):
    """
    Given a nontabular resource entry, returns a glossary entry for that resource. Internal subroutine to
    `_write_glossary`.

    Parameters
    ----------
    resource_entry: dict, required
        The resource entry to be glossarized. Must correspond with a nontabular resource and must contain a
        `landing_page` key.
    timeout: int, default 60
        A timeout on how long the glossarizer can spend attempting to download the given resource before giving up.
        This download is necessary because Socrata does not provide `content-length` information in its data
        transfer headers, meaning the only way to know the size of a resource -- an operational necessity -- is to
        (attempt to) download it yourself.

    Returns
    -------
    If the method executes successfully, returns a glossary entry for the given resource that is fit for inclusion
    in the given glossary. If the method fails because an error was raised, prints (!) a warning and returns an empty
    list.
    """

    # TODO: Refactor this convoluted method into a simpler `utils.generic_glossarize_resource` wrapper.

    import zipfile
    from requests.exceptions import ChunkedEncodingError

    try:
        sizings = _get_sizings(
            resource_entry['resource'], timeout=timeout
        )
    except zipfile.BadZipfile:
        # cf. https://github.com/ResidentMario/datafy/issues/2
        # print("WARNING: the '{0}' endpoint is either misformatted or contains multiple levels of "
        #       "archiving which failed to process.".format(resource['landing_page']))
        resource_entry['flags'].append('error')
        return []
    # This error is raised when the process takes too long.
    except ChunkedEncodingError:
        # print("WARNING: the '{0}' endpoint took longer than the {1} second timeout to process.".format(
        #     resource['landing_page'], timeout))
        # TODO: Is this the right thing to do?
        return []
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        # External links may point anywhere, including HTML pages which don't exist or which raise errors when you
        # try to visit them. During testing this occurred with e.g. https://data.cityofnewyork.us/d/sah3-jw2y. It's
        # impossible to exclude everything; best we can do is raise a warning.
        print("WARNING: an error was raised while processing the '{0}' endpoint.".format(resource_entry['landing_page']))
        resource_entry['flags'].append('error')
        return []

    # If successful, append the result to the glossaries.
    if sizings and sizings[0]['mimetype'] != 'text/html':
        glossarized_resource = []

        for sizing in sizings:
            # ...but with one caveat. When this process is run on a link, there is a strong possibility
            # that it will result in the concatenation of a landing page. There's no automated way to
            # determine whether or not a specific resource is or is not a landing page other than to
            # inspect it outselves. For example, you can probably tell that
            # "http://datamine.mta.info/user/register" is a landing page, but how about
            # "http://ddcftp.nyc.gov/rfpweb/rfp_rss.aspx?q=open"? Or
            # "https://a816-healthpsi.nyc.gov/DispensingSiteLocator/mainView.do"?

            # Nevertheless, there is one fairly strong signal we can rely on: landing pages will be HTML
            # front-end, and python-magic should in *most* cases determine this fact for us and return it
            # in the file typing information. So we can use this to hopefully eliminate many of the
            # problematic endpoints.
            if sizing['extension'] != "htm" and sizing['extension'] != "html":
                # Remove the "processed" flag from the resource going into the glossaries, if one exists.
                glossarized_resource_element = resource_entry.copy()
                glossarized_resource_element['flags'] = [flag for flag in glossarized_resource_element['flags'] if
                                                         flag != 'processed']

                # Attach sizing information.
                glossarized_resource_element['filesize'] = sizing['filesize']

                # Attach format information.
                glossarized_resource_element['preferred_format'] = sizing['extension']
                glossarized_resource_element['preferred_mimetype'] = sizing['mimetype']

                # If no repairable errors were caught, write in the information.
                # (if a non-repairable error was caught the data gets sent to the outer finally block)
                glossarized_resource_element['dataset'] = sizing['dataset']

                glossarized_resource.append(glossarized_resource_element)

        return glossarized_resource

    elif sizings[0]['mimetype'] == 'text/html':
        return []

    # If unsuccessful, append a signal result to the glossaries.
    else:
        glossarized_resource_element = resource_entry.copy()

        glossarized_resource_element['flags'] = [flag for flag in glossarized_resource_element['flags'] if
                                         flag != 'processed']

        glossarized_resource_element["filesize"] = ">{0}s".format(str(timeout))
        glossarized_resource_element['dataset'] = "."

        return [glossarized_resource_element]


def get_glossary(resource_list, glossary, domain='opendata.cityofnewyork.us', timeout=60):
    """
    Given a resource list and an extant glossary, generate and return an updated glossary.

    Non-IO subroutine of the user-facing `write_glossary` method.
    """
    try:
        tables = [r for r in resource_list if r['resource_type'] == "table"]
        nontables = [r for r in resource_list if r['resource_type'] != "table"]

        # tables:
        from .pager import driver

        for resource in tqdm(tables):
            glossarized_resource = _glossarize_table(resource, domain, driver=driver)
            glossary += glossarized_resource

            # Update the resource list to make note of the fact that this job has been processed.
            if "processed" not in resource['flags']:
                resource['flags'].append("processed")

        # geospatial datasets, blobs, links:
        for resource in tqdm(nontables):
            glossarized_resource = _glossarize_nontable(resource, timeout=timeout)
            glossary += glossarized_resource

            # Update the resource list to make note of the fact that this job has been processed.
            if "processed" not in resource['flags']:
                resource['flags'].append("processed")

    # Whether we succeeded or got caught on a fatal error, in either case clean up.
    finally:
        # If a driver was open, close the driver instance.
        # noinspection PyUnboundLocalVariable
        driver.quit()  # pager.driver
    return resource_list, glossary


def write_glossary(domain='opendata.cityofnewyork.us', resource_filename=None, glossary_filename=None,
                   use_cache=True, timeout=60):
    """
    Use a resource file to write a glossary to disc.

    Parameters
    ----------
    domain: str, default "opendata.cityofnewyork.us"
        The open data portal landing page URI. Socrata automatically redirects requests to URIs corresponding with
        expired, deleted, or invisible endpoints to the data portal homepage. This homepage may differ from the one
        used to host the rest of the website: on the New York City Open data portal for example the root URL is
        "opendata.cityofnewyork.us" (the default argument), but the datasets themselves are hosted from
        "data.cityofnewyork.us". The latter not the former should be provided as the `domain`, not the former,
        because otherwise there is no way to detect when a URI "bounce" occurs.
    resource_filename: str
        A path to a resource file to read processing jobs from.
    glossary_filename: str
        A path to a glossary file to write output to.
    use_cache: bool, default True
        If a glossary file is already present at `glossary_filename` and `use_cache` is `True`, endpoints already in
        that file will be left untouched and ones that are not will be appended on. If `use_cache` is `False` the
        file will be overwritten instead.
    timeout: int, default 60
        A timeout on how long the glossarizer can spend downloading a resource before timing it out. This prevents
        occasional very large datasets from overwhelming your CPU. Resources that time out will be populated in the
        glossary with a `filesize` field indicating how long they were downloading for before timing out.
    """

    # Load the glossarization to-do list.
    resource_list, glossary = load_glossary_todo(resource_filename, glossary_filename, use_cache)

    # Generate the glossaries.
    try:
        resource_list, glossary = get_glossary(resource_list, glossary, domain=domain,
                                               timeout=timeout)

    # Save output.
    finally:
        write_resource_file(resource_list, resource_filename)
        write_glossary_file(glossary, glossary_filename)

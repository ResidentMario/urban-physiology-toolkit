"""
CKAN glossarizer. A general implementation reference is available online at
https://github.com/ResidentMario/urban-physiology-toolkit/wiki/Glossarization-Notes:-CKAN.
"""

import warnings
import pandas as pd
import requests
from tqdm import tqdm

from urban_physiology_toolkit.glossarizers.utils import (preexisting_cache, load_glossary_todo, write_resource_file,
                                                         write_glossary_file, get_sizings)


def write_resource_list(domain="data.gov.sg", filename=None, use_cache=True, protocol='https'):
    """
    Creates a resource list for the given CKAN domain and writes it to disc.

    Parameters
    ----------
    domain: str, default "data.gov.sg"
        The open data portal root URI.
    filename: str
        The name of the file to write the resource list to.
    use_cache: bool, default True
        If a resource file is already present at `filename` and `use_cache` is `True`, return immediately.
    protocol: {'http', 'https'}, default 'https'
        The transfer protocol the portal in question uses. This is used to construct all queries to e.g. the portal
        API. Although the Internet as a whole is moving towards HTTPS, because CKAN is a federated run-local asset,
        many of the portals online are still on HTTP.
    """

    # If the file already exists and we specify `use_cache=True`, simply return.
    if preexisting_cache(filename, use_cache):
        return

    package_list_slug = "{0}://{1}/api/3/action/package_list".format(protocol, domain)
    package_list = requests.get(package_list_slug).json()

    if 'success' not in package_list or package_list['success'] != True:
        raise requests.RequestException("The CKAN catalog page did not resolve successfully.")

    resources = package_list['result']

    roi_repr = []

    try:
        for resource in tqdm(resources):
            # package_metadata_show vs. package_show?
            metadata = requests.get("{0}://{1}/api/3/action/package_show?id={2}".format(protocol,
                                                                                        domain,
                                                                                        resource)).json()

            # Individual fields vary between providers.
            if domain == "data.gov.sg":
                license = metadata['result']['license']
                publisher = metadata['result']['publisher']['name']
                keywords = metadata['result']['keywords']
                description = metadata['result']['description']
                topics = metadata['result']['topics']
                name = metadata['result']['title']
                sources = metadata['result']['sources']
                update_frequency = metadata['result']['frequency']

                created = None
                last_updated = str(pd.Timestamp(metadata['result']['last_updated']))

            elif domain == "catalog.data.ug":
                # Note: Organization sometimes left blank.

                license = metadata['result']['license_title']
                publisher = metadata['result']['organization']['title'] if metadata['result']['organization'] else None
                keywords = []
                description = metadata['result']['notes']
                topics = [tag['name'] for tag in metadata['result']['tags']]
                name = metadata['result']['title']
                sources = metadata['result']['organization']['title'] if metadata['result']['organization'] else None
                update_frequency = None

                created = str(pd.Timestamp(metadata['result']['metadata_created']))
                last_updated = str(pd.Timestamp(metadata['result']['metadata_modified']))

            try:
                # It is possible to have a dataset with no data in it.
                # Example: http://catalog.data.ug/dataset/nema
                # This is distinct from what would transpire on e.g. Socrata, where you could have a 0-entity
                # dataset, but it's still a dataset.
                # No-data nodes can safely be skipped.
                canonical = metadata['result']['resources'][0]
            except:
                continue

            preferred_format = canonical['format'].lower()
            slug = canonical['url']

            if domain == "data.gov.sg":
                # Slug: "https://storage.data.gov.sg/3g-public-cellular-mobile-telephone-services/[...]"
                # We need: "3g-public-cellular-mobile-telephone-services"
                # Because landing page is: "https://data.gov.sg/dataset/3g-public-cellular-mobile-telephone-services"
                landing_page = "{0}/dataset/{1}".format(domain, slug.split("/")[3])
            elif domain == "catalog.data.ug":
                # We need the id: "f72b9932-52a1-4014-987e-047a370c3d96".
                # Because landing page is: "http://catalog.data.ug/dataset/f72b9932-52a1-4014-987e-047a370c3d96"
                # The "human-readable" landing page is "http://catalog.data.ug/dataset/2014-census"
                # But there's no way to back that URL out of the metadata, surprisingly, because the "URL" parameter
                # is often left empty.
                # landing_page = "{0}/dataset/{1}".format(domain, metadata['result']['url'].lower().replace(" ", "-"))
                landing_page = "{0}/dataset/{1}".format(domain, metadata['result']['id'])

            # CKAN treats resources as resources. A single endpoint may host a few different datasets, differentiated
            # in the interface by a tab menu, or it may host the same dataset in multiple formats (in which case you
            # get a menu of options in the interface). The metadata export does not make it immediately obvious which
            # of the two is the case. Instead, we use the following heuristic to determine.

            # https://data.gov.sg/api/3/action/package_metadata_show?id=abc-waters-sites
            # A metdata export from the Singapore open data portal of a dataset with two formats available contains a
            # "resources" key, in which there exists a list of two dicts, a key of which is url. The two URLs are:
            # "https://geo.data.gov.sg/abcwaterssites/2016/10/28/kml/abcwaterssites.zip"
            # "https://geo.data.gov.sg/abcwaterssites/2016/10/28/shp/abcwaterssites.zip"

            # A metadata export from the Singapre open data portal of a dataset with two files available:
            # "https://storage.data.gov.sg/3g-public-cellular-mobile-telephone-services/resources/[long name 1].csv"
            # "https://storage.data.gov.sg/3g-public-cellular-mobile-telephone-services/resources/[long name 2].csv"

            # In the second case the names (stripping out the extension) are distinct. In the first case, they are not.
            # This is the heuristic we use to determine whether we have two exports of the same data, or two different
            # datasets proper.
            multiple_datasets = len(set([m['url'].split("/")[-1].split(".")[0]\
                                         for m in metadata['result']['resources']])) > 1

            if multiple_datasets:
                for dataset in metadata['result']['resources']:
                    # Composite names, but the key name depends on the domain.
                    if domain == "data.gov.sg":
                        name = "{0} - {1}".format(name, dataset['title'])
                    elif domain == "catalog.data.ug":
                        name = "{0} - {1}".format(name, dataset['name'])

                    roi_repr.append({
                        'landing_page': landing_page,
                        'resource': dataset['url'],
                        'protocol': protocol,
                        'name': name,
                        'description': description,
                        'publisher': publisher,
                        'sources': sources,
                        'created': created,
                        'last_updated': last_updated,
                        'update_frequency': update_frequency,
                        'tags_provided': keywords,
                        'topics_provided': topics,
                        'available_formats': [dataset['format'].lower()],
                        'preferred_format': dataset['format'].lower(),
                        'license': license,
                        'flags': []
                    })

            else:
                available_formats = [m['format'].lower() for m in metadata['result']['resources']]

                roi_repr.append({
                    'landing_page': landing_page,
                    'resource': slug,
                    'protocol': protocol,
                    'name': name,
                    'description': description,
                    'publisher': publisher,
                    'sources': sources,
                    'created': created,
                    'last_updated': last_updated,
                    'update_frequency': update_frequency,
                    'tags_provided': keywords,
                    'topics_provided': topics,
                    'available_formats': available_formats,
                    'preferred_format': preferred_format,
                    'license': license,
                    'flags': []
                })
    finally:
        # Write to file and exit.
        write_resource_file(roi_repr, filename)


def write_glossary(domain="data.gov.sg", resource_filename=None, glossary_filename=None,
                   use_cache=True, timeout=60):
    """
    Use a resource file to write a glossary to disc.

    Parameters
    ----------
    domain: str, default "data.gov.sg"
        The open data portal landing page URI.
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
    resource_list, glossary = load_glossary_todo(resource_filename, glossary_filename, use_cache=use_cache)

    # Whether we succeed or fail, we'll want to save the data we have at the end with a try-finally block.
    try:
        for i, resource in tqdm(list(enumerate(resource_list))):

            glossarized_resource = resource.copy()

            # Get the sizing information.
            # If the resource is its own dataset, this is provided in the content header. Sometimes it is not.
            headers = requests.head(resource['resource']).headers

            try:
                glossarized_resource['preferred_mimetype'] = headers['content-type']
            except KeyError:
                import pdb; pdb.set_trace()
                # HTTP error that occurs when.
                continue

            try:
                glossarized_resource['filesize'] = headers['content-length']
                glossarized_resource['dataset'] = '.'
                succeeded = True

            # If we error out, this is a packaged/gzipped file. Do sizing the basic way, with a GET request.
            except KeyError:
                dataset_repr = get_sizings(resource['resource'])
                try:
                    glossarized_resource['filesize'] = dataset_repr[0]['filesize']
                    glossarized_resource['dataset'] = dataset_repr[0]['dataset']
                    succeeded = True
                except TypeError:
                    # Transient failure.
                    succeeded = False
                    warnings.warn(
                        "Couldn't parse the URI {0} due to a transient network failure."\
                            .format(resource['resource'])
                    )

            # Update the resource list to make note of the fact that this job has been processed.
            if 'processed' not in resource['flags'] and succeeded:
                resource["flags"].append("processed")

            glossary.append(glossarized_resource)

    # Whether we succeeded or got caught on a fatal error, in either case clean up.
    finally:
        # Save output.
        write_resource_file(resource_list, resource_filename)
        write_glossary_file(glossary, glossary_filename)


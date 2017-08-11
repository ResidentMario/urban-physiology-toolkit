"""
Generic methods common across multiple glossarizers.
"""

import os
import json
import errno
import warnings

############
# FILE I/O #
############


def preexisting_cache(folder_filepath, use_cache):
    """
    If the file already exists and `use_cache` is `True`, return immediately.
    """
    return use_cache and os.path.isfile(folder_filepath)


def write_resource_file(resource_listings, resource_filename):
    """
    Writes a resource list to a file. Handles merging duplicate and preexisting records.
    """
    # If a resource file already exists, only write in resources in the current resource listing that do not already
    # exist in the file.
    if os.path.isfile(resource_filename):
        with open(resource_filename, 'r') as fp:
            existing_resources = json.load(fp)
        existing_resource_uris = {r['resource'] for r in existing_resources}
        resources_to_be_added = [r for r in resource_listings if r['resource'] not in existing_resource_uris]

        resource_list = existing_resources + resources_to_be_added
        with open(resource_filename, 'w') as fp:
            json.dump(resource_list, fp, indent=4)

    # If the resource file does not already exist, simply write what we get to file.
    else:
        with open(resource_filename, 'w') as fp:
            json.dump(resource_listings, fp, indent=4)


def write_glossary_file(glossary_repr, glossary_filename):
    with open(glossary_filename, "w") as fp:
        json.dump(glossary_repr, fp, indent=4)


def load_glossary_todo(resource_filename, glossary_filename, use_cache=True):
    """
    Loads and returns the resource list and glossary corresponding with the given `resource_filename` and
    `glossary_filename`, respectively. Handles merging duplicate and preexisting records.
    """

    # Begin by loading in the data that we have.
    with open(resource_filename, "r") as fp:
        resource_list = json.load(fp)

    # If use_cache is True, remove resources which have already been processed. Otherwise, only exclude "ignore" flags.
    # Note: "removed" flags are not ignored. It's not too expensive to check whether or not this was a fluke or if the
    # dataset is back up or not.
    if use_cache:
        resource_list = [r for r in resource_list if "processed" not in r['flags'] and "ignore" not in r['flags']]
    else:
        resource_list = [r for r in resource_list if "ignore" not in r['flags']]

    # Check whether or not the glossaries file exists.
    preexisting = os.path.isfile(glossary_filename)

    # If it does, load it. Otherwise, load an empty list.
    if preexisting:
        with open(glossary_filename, "r") as fp:
            glossary = json.load(fp)
    else:
        glossary = []

    return resource_list, glossary

####################
# SIZING PROCESSES #
####################


def __timeout_process(seconds=10, error_message=os.strerror(errno.ETIME)):
    """
    Times out a process. Taken from Stack Overflow: 2281850/timeout-function-if-it-takes-too-long-to-finish.

    Some notes:
    * Timing out a process, it turns out, is very hard. I wrote a timeout using multiprocessing.Queue from the
      standard library, wrapped in a package I named "limited-requests". It worked for simple files,
      but broke down on more complex ones, with the process simply hanging forever. I don't know why; I suspect
      that Queue implicity requires the operations that are placed in the queue not touch the filesystem,
      or somesuch. One day I would like to find out the answer why.

    * I don't claim to really understand this code. It uses UNIX signaling to get the OS to force the process to
      explode.

    * This means that even though it says it will throw a TimeoutError, in reality it seems to always throw a
      ChunkEncodedError instead (from requests). Or maybe it's that requests is intercepting the timeout and
      throwing its own error? Heavens knows.

    * It's UNIX-only.

    * It only allows timeouts in integers...amusingly.

    * It's very dangerous. XXX.

    * More details: http://eli.thegreenplace.net/2011/08/22/how-not-to-set-a-timeout-on-a-computation-in-python

    * multiprocessing.Pool may provide a better interface. I do not know how to operate a multiprocessing.Pool
     though.
    """
    import signal
    from functools import wraps
    # TODO: Use https://github.com/pnpnpn/timeout-decorator/blob/master/timeout_decorator/timeout_decorator.py instead.

    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def get_sizings(uri, timeout=60):
    """
    Given a URI, attempts to download it within `timeout` seconds. On success, returns size and format information on
    the downloaded data.

    A timed (using `__timeout_process`) wrapper of the externally-packaged `datafy.get` method.

    Parameters
    ----------
    uri: str, required
        The resource URI.
    timeout: int, required
        The timeout assigned to this resource download.

    Returns
    -------

    If the download succeeds (the entire file is downloaded within `timeout` seconds), returns a structured list of
    dicts of size and type-related metadata on the downloaded file. Each entry in the list will be of the following
    format:

        {'filesize': int, 'filepath': str, 'mimetype': str, 'extension': str}

    The list will consist of only one entry if the resource contains a single file, and multiple entries if the
    resource contains many files. Note that as packaged resources may contain metadata and junk files,
    not just data, the references contained in this list are not datasets *per se*.

    If the download times out, raises a `requests.exceptions.ChunkedEncodingError`, a generic error returned
    whenever `requests` is cut off whilst downloading (see further the `__timeout_process` docstring). All other
    errors are uncaught and get raised upstream.
    """
    import datafy
    import sys

    @__timeout_process(timeout)
    def _size_up(uri):
        resource = datafy.get(uri)
        resource_components = []
        for resource_component in resource:
            resource_components.append({
                'filesize': sys.getsizeof(resource_component['data'].content) / 1024,
                'dataset': resource_component['filepath'],
                'mimetype': resource_component['mimetype'],
                'extension': resource_component['extension']
            })
        return resource_components

    return _size_up(uri)


def generic_glossarize_resource(resource, timeout):
    """
    A generic resource glossarization method that transforms a resource entry into a glossary entry by attempting to
    download it within `timeout` seconds.

    This method should only be used if no `content-length` header is provided by the server and no other method of
    finding out filesize (such as by perusing an API) exists.

    Parameters
    ----------
    resource: dict, required
        A resource entry for processing.
    timeout: int, required
        The timeout assigned to this resource download.

    Returns
    -------

    This method returns a tuple with two elements in it: the modified resource entry, and a list of generated
    glossary entries.

    The second element returned will correspond with the given resource with `filesize`, `preferred_format`,
    `preferred_mimetype`, `resource`, and `dataset` field filled in.

    If the resource cannot be downloaded in `timeout` seconds, the `filesize` field will be filled with an entry
    of the form `">Ns", where N is the timeout.

    If the process fails during the download due to an unrelated error, a warning will be raised, an "error" string is
    appended to the resource flags, and an empty list will be returned.

    If the process fails during processing because of bad data formatting in a compressed file, a warning will the
    raised and an "error" string is again appended to the resource flags. An empty list will be returned.

    If the process succeeds, but we discover that our result is an HTML file (this occurs in the case of external
    links to landing pages), an empty list will be returned.
    """
    import zipfile
    from requests.exceptions import ChunkedEncodingError

    try:
        sizings = get_sizings(
            resource['resource'], timeout=timeout
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except zipfile.BadZipfile:
        # cf. https://github.com/ResidentMario/datafy/issues/2
        warnings.warn("The '{0}' resource is either misformatted or contains multiple levels of "
                      "compression, and failed to process. This resource will be flagged as an error in the "
                      "resource list".format(resource['landing_page']))
        resource['flags'].append('error')
        return resource, []
    # This error is raised when the process takes too long.
    except ChunkedEncodingError:
        glossarized_resource_element = resource.copy()
        glossarized_resource_element['flags'] = [flag for flag in glossarized_resource_element['flags'] if
                                                 flag != 'processed']
        glossarized_resource_element['dataset'] = "."
        return resource, [glossarized_resource_element]
    except:
        # External links may point anywhere, including HTML pages which don't exist or which raise errors when you
        # try to visit them. During testing this occurred with e.g. https://data.cityofnewyork.us/d/sah3-jw2y. It's
        # impossible to exclude everything; best we can do is raise a warning.
        warnings.warn("An error was raised while processing the '{0}' resource. This resource will be flagged as an "
                      "error in the resource list.".format(resource['landing_page']))
        resource['flags'].append('error')
        return resource, []

    # If successful, and the result is an HTML file, pass out an empty list.
    if sizings[0]['mimetype'] == 'text/html':
        return resource, []

    # If successful, and the result is NOT an HTML file, append the result to the glossaries.
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

            # However, realistically there would need to be some kind of secondary list mechanism that's
            # maintained by hand for excluding specific pages. That, however, is a TODO.
            if sizing['extension'] != "htm" and sizing['extension'] != "html":
                # Remove the "processed" flag from the resource going into the glossaries, if one exists.
                glossarized_resource_element = resource.copy()
                glossarized_resource_element['flags'] = [flag for flag in glossarized_resource_element['flags'] if
                                                         flag != 'processed']

                # Attach the info.
                glossarized_resource_element['filesize'] = sizing['filesize']
                glossarized_resource_element['preferred_format'] = sizing['extension']
                glossarized_resource_element['preferred_mimetype'] = sizing['mimetype']
                glossarized_resource_element['dataset'] = sizing['dataset']

                glossarized_resource.append(glossarized_resource_element)

        resource['flags'].append('processed')

        return resource, glossarized_resource

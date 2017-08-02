"""
Generic methods common across all or many glossarizers.
"""

import os
import json
import errno
import warnings

############
# FILE I/O #
############

def preexisting_cache(folder_filepath, use_cache):
    # If the file already exists and we specify `use_cache=True`, simply return.
    preexisting = os.path.isfile(folder_filepath)
    if preexisting and use_cache:
        return


def write_resource_file(roi_repr, resource_filename):
    with open(resource_filename, 'w') as fp:
        json.dump(roi_repr, fp, indent=4)


def write_glossary_file(glossary_repr, glossary_filename):
    with open(glossary_filename, "w") as fp:
        json.dump(glossary_repr, fp, indent=4)


def load_glossary_todo(resource_filename, glossary_filename, use_cache=True):
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


def timeout_process(seconds=10, error_message=os.strerror(errno.ETIME)):
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


def _get_sizings(uri, timeout=60):
    """
    Given a URI, attempts to download it within `timeout` seconds. This method throws a
    `requests.exceptions.ChunkedEncodingError` if it does not succeed before the timeout is reached. It does no
    error handling on its own, so if the download fails due to some other reason it will raise. Otherwise,
    if the download succeeds (the entire file is downloaded within `timeout` seconds), returns a structured list of
    dicts of size and type-related metadata on the downloaded file. This data comes in the form of a list because if
    the file is an archival file (a ZIP file), the file will be unpacked locally and its contents will be
    inspected and recorded. On the other hand, if the file being downloaded is singular, the list will only have one
    element.

    This method uses `timeout_process`, above, and adapts the separately packaged `datafy` module to do processing.

    The developer-facing wrapper for this method is `attach_filesize_to_resource`.
    """
    import datafy
    import sys

    @timeout_process(timeout)
    def _size_up(uri):
        resource = datafy.get(uri)
        thing_log = []
        for thing in resource:
            thing_log.append({
                'filesize': sys.getsizeof(thing['data'].content) / 1024,
                'dataset': thing['filepath'],
                'mimetype': thing['mimetype'],
                'extension': thing['extension']
            })
        return thing_log

    return _size_up(uri)


def generic_glossarize_resource(resource, timeout):
    """
    Given a resource entry and a timeout, writes `filesize`, `preferred_format`, `preferred_mimetype`, and `dataset`
    fields into a glossarization of that resource entry. Also attached a "processed" flag to that resource's `flags`
    field. This method is the brute force way of glossarizing a resource; it should only be used if no faster way
    getting this same information is found.

    This method does its job by calling the `_get_sizings` method above (itself reliant on the separately-packaged
    `datafy` module) and processing the result.

    What happens next depends on what the result is:

    If the resource cannot be downloaded in `timeout` seconds, the `filesize` field will be filled with an entry
    of the form `">Ns", where N is timeout, and a glossary entry will be returned.

    If the process fails during the download due to an unrelated error, an "error" string is appended to the
    resource flags, and an empty list will be returned.

    If the process fails during processing because of bad data formatting in a compressed file, an "error" string is
    again appended to the resource flags, and an empty list will be returned.

    If the process succeeds, but we discover that our result is an HTML file (this occurs in the case of external
    links to landing pages), an empty list will be returned.

    Returns
    -------

    This method returns a tuple with two elements in it: the modified resource entry, and a list of generated
    glossary entries.
    """
    import zipfile
    from requests.exceptions import ChunkedEncodingError

    try:
        sizings = _get_sizings(
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

"""
Generic IO methods which are common across all glossarizers, but not important enough to need their own package,
as well as airscooter integration tools.
"""

import os
import json
import errno


def _preexisting_cache(folder_filepath, use_cache):
    # If the file already exists and we specify `use_cache=True`, simply return.
    preexisting = os.path.isfile(folder_filepath)
    if preexisting and use_cache:
        return


def _write_resource_file(roi_repr, resource_filename):
    with open(resource_filename, 'w') as fp:
        json.dump(roi_repr, fp, indent=4)


def _write_glossary_file(glossary_repr, glossary_filename):
    with open(glossary_filename, "w") as fp:
        json.dump(glossary_repr, fp, indent=4)


def _load_glossary_todo(resource_filename, glossary_filename, use_cache=True):
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


def _timeout_process(seconds=10, error_message=os.strerror(errno.ETIME)):
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


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    # cf. https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
    import unicodedata
    import re

    value = unicodedata.normalize('NFKC', value)
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    value = re.sub('[-\s]+', '-', value)
    return value


def init_catalog(glossary_filepath, root):
    """
    Initializes a catalog's folder structure for the given glossary at the given root folder.
    """
    from pathlib import Path
    import os

    root = str(Path(root).resolve())
    root_folders = os.listdir(root)

    if 'catalog' not in root_folders:
        os.mkdir(root + "/catalog")
    if 'tasks' not in root_folders:
        os.mkdir(root + "/tasks")

    with open(glossary_filepath, "r") as f:
        glossary = json.load(f)

    resource_folder_names = []

    for entry in glossary:
        resource_folder_name = slugify(entry['name'])

        if resource_folder_name not in resource_folder_names:
            os.mkdir(root + "/tasks" + "/{0}".format(resource_folder_name))
            os.mkdir(root + "/catalog" + "/{0}".format(resource_folder_name))
            resource_folder_names.append(resource_folder_name)

        dataset_name = entry['dataset'] if entry['dataset'] != "." else "data.csv"
        dataset_filepath = "{0}/catalog/{1}/{2}".format(root, resource_folder_name, dataset_name)

        with open(root + "/tasks" + "/{0}".format(resource_folder_name) + "/depositor.py", "w") as f:
            if entry['dataset'] == "." and entry['preferred_format'] == "csv":
                f.write("""
import requests
r = requests.get("{0}")
with open("{1}", "wb") as f:
    f.write(r.content)
""".format(entry['resource'], dataset_filepath))
            else:
                f.write("""# TODO: Implement fetch for {0}
pass""".format(entry['resource']))

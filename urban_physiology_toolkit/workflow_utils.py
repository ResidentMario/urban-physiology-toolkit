"""
Methodology for the portal localization workflow. Excludes glossarizer-specific utilities, which are provided instead
in `glossarizer_utils`.
"""

from ast import literal_eval
import json
import os
from pathlib import Path


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


def generate_data_package_from_glossary_entry(entry):
    """
    Given a glossary entry, returns a datapackage.json file for that entry.
    """
    # TODO: Mark geospatial files complete as well. See also the tests.
    solo_csv_resource = entry['dataset'] == '.' and entry['preferred_format'] == 'csv'

    package = {
        # datapackage fields
        'name': slugify(entry['name']),
        'datapackage_version': '1.0-beta',
        'title': entry['name'],
        'version': 'N/A',
        'keywords': entry['keywords_provided'],
        'description': entry['description'],
        'licenses': [],
        'sources': [{'name': source, 'web': 'https://example.com'} for source in entry['sources']],
        'contributors': [],
        'maintainers': [],
        'publishers': [],
        'dependencies': dict(),
        'resources': [],
        'views': [],
        # possibly empty glossary fields
        'rows': None if 'rows' not in entry else entry['rows'],
        'columns': None if 'columns' not in entry else entry['columns'],
        'filesize': None if 'filesize' not in entry else entry['filesize'],
        'available_formats': None,
        # signal field
        'complete': solo_csv_resource
    }

    # other glossary fields
    for field in ['page_views', 'preferred_mimetype', 'topics_provided', 'preferred_format', 'column_names',
                  'landing_page', 'last_updated', 'protocol', 'created']:
        package[field] = entry[field]

    # populate the resources field directly, if appropriate.
    if solo_csv_resource:
        package['resources'] = [
            {'path': 'data.csv', 'url': entry['resource']}
        ]

    return package


def init_catalog(glossary_filepath, root):
    """
    Initializes a catalog's folder structure for the given glossary at the given root folder.
    """
    from pathlib import Path
    import os

    # Initialize the bare root folders.
    root = str(Path(root).resolve())
    root_folders = os.listdir(root)

    if 'catalog' not in root_folders:
        os.mkdir(root + "/catalog")
    if 'tasks' not in root_folders:
        os.mkdir(root + "/tasks")

    # Read in the glossary.
    with open(glossary_filepath, "r") as f:
        glossary = json.load(f)

    # Resource names are not necessarily unique; only resource URLs are. We need to modify our resource names
    # as we go along to ensure that all of our elements end up in the right places, folder-wise.
    resources = [entry['resource'] for entry in glossary]
    raw_resource_folder_names = [slugify(entry['name']) for entry in glossary]
    resource_folder_names = []
    name_map = dict()

    for resource, raw_resource_folder_name in zip(resources, raw_resource_folder_names):
        if resource in name_map:
            resource_folder_names.append(name_map[resource])
        elif resource not in name_map and raw_resource_folder_name in name_map.values():  # collision!
            n = 2
            while True:
                if raw_resource_folder_name + str(n) in name_map:
                    n += 1
                    continue
                else:
                    resource_folder_name = raw_resource_folder_name + "-" + str(n)
                    break

            name_map[resource] = resource_folder_name
            resource_folder_names.append(resource_folder_name)
        else:
            name_map[resource] = raw_resource_folder_name
            resource_folder_names.append(name_map[resource])

    # Write the depositor task folders. We do this by iterating through glossary entries, writing new depositors for
    # any entry we find which is not already in our touched folders.
    folders = set()
    for entry, resource_folder_name in zip(glossary, resource_folder_names):
        if resource_folder_name not in folders:
            os.mkdir(root + "/tasks" + "/{0}".format(resource_folder_name))
            os.mkdir(root + "/catalog" + "/{0}".format(resource_folder_name))
            folders.add(resource_folder_name)

            data_filename = "data.{0}".format(entry['preferred_format']) if entry['dataset'] == "." else "data.zip"
            dataset_filepath = "{0}/catalog/{1}/{2}".format(root, resource_folder_name, data_filename)
            depositor_filepath = root + "/tasks" + "/{0}".format(resource_folder_name) + "/depositor.py"

            with open(depositor_filepath, "w") as f:
                f.write("""import requests
r = requests.get("{0}")
with open("{1}", "wb") as f:
    f.write(r.content)

outputs = ["{1}"]
""".format(entry['resource'], dataset_filepath))

    # Write the transform task folders. We do this by first organizing our entries into three categories:
    # 1. CSV and geospatial resources. No transform necessary, so none is written.
    # 2. Archival resources. We know that a resource is an archival resource when its URI appears multiple times in
    #    the glossary. Right now the limitation in place is that if we have an archival file, we assume that it is
    #    provided in a ZIP format (as opposed to, say, a TAR, or some other archive). Significant re-engineering
    #    still needs to be done in order to enable alternative file formats. See the GitHub issues. Anyway,
    #    if it's an assumed ZIP file, an incomplete transform is written that unzips the file and prepares a data
    #    package.
    # 3. Other resources. These are single-dataset resources which are not CSV or geospatial files. An incomplete
    #    transform is written in these cases too.
    folders = set()
    for entry, resource_folder_name in zip(glossary, resource_folder_names):
        if resource_folder_name not in folders:
            catalog_filepath = root + "/catalog" + "/{0}".format(resource_folder_name)
            transform_filepath = root + "/tasks" + "/{0}".format(resource_folder_name) + "/transform.py"
            data_filename = "data.{0}".format(entry['preferred_format']) if entry['dataset'] == "." else "data.zip"
            dataset_filepath = "{0}/catalog/{1}/{2}".format(root, resource_folder_name, data_filename)

            with open(catalog_filepath + "/datapackage.json", "w") as f:
                json.dumps(generate_data_package_from_glossary_entry(entry), indent=4)

            if entry['dataset'] == "." and entry['preferred_format'] in ["csv", "geojson"]:  # Case 1
                pass
            elif entry['dataset'] != ".":  # Case 2
                with open(transform_filepath, "w") as f:
                    f.write("""# TODO: Finish implementing!
from zipfile import ZipFile
z = ZipFile("{0}", "r")

outputs = []
""".format(dataset_filepath))
            else:  # Case 3
                with open(transform_filepath, "w") as f:
                    f.write("""# TODO: Finish implementing!
# {0}

outputs = []
""".format(dataset_filepath))


def update_dag(root="."):
    """
    Updates the Airscooter DAG so that it reflects the current state of the catalog. This op:
    * Create a .airflow folder and Airflow DAG if one does not yet exist.
    * Adds tasks with fleshed-out depositors and transforms to the DAG.
    * Ignores incomplete transforms (the flag currently used is the presence of a # TOOD shebang line)
    * Removes tasks defined in the DAG which are not also defined in the catalog tasks.
    """
    from airscooter.orchestration import Depositor, Transform

    resource_folders = os.listdir("{0}/tasks/".format(root))
    tasks = []

    for folder in resource_folders:

        todo = os.listdir("{0}/tasks/{1}".format(root, folder))

        # TODO: Allow {py, ipynb, sh} tasks.
        if "depositor.py" in todo:
            name = "{0}-depositor".format(folder)
            filename = "{0}/tasks/{1}/depositor.py".format(root, folder)

            with open(filename, "r") as f:
                outputs = literal_eval(f.readlines()[-1].split("=")[-1].strip())

            def munge_path(path):
                # TODO: Flag relative-versus-absolute better.
                # Make relative filepaths absolute.
                if "/" not in path:
                    return str(Path("{0}/catalog/{1}/{2}".format(root, folder, filename)).resolve())
                else:
                    return path

            outputs = [munge_path(path) for path in outputs]

            py_name = "var_" + name.replace("-", "_")  # clean up the URL slug name so that it can be used as a var
            dep = Depositor(py_name, filename, outputs)
            tasks.append(dep)

        if "transform.py" in todo:
            name = "{0}-transform".format(folder)
            filename = "{0}/tasks/{1}/transform.py".format(root, folder)
            depositor_prior = tasks[-1]
            inputs = tasks[-1].output

            with open(filename, "r") as f:
                outputs = literal_eval(f.readlines()[-1].split("=")[-1].strip())

            py_name = "var_" + name.replace("-", "_")  # clean up the URL slug name so that it can be used as a var
            trans = Transform(py_name, filename, inputs, outputs, requirements=[depositor_prior])
            tasks.append(trans)

    from airscooter.orchestration import (serialize_to_file, write_airflow_string)

    serialize_to_file(tasks, "{0}/.airflow/airscooter.yml".format(root))
    write_airflow_string(tasks, "{0}/.airflow/datablocks_dag.py".format(root))

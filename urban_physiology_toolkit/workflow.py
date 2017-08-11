"""
Methodology for the portal localization workflow. Excludes glossarizer-specific utilities, which are provided instead
in `glossarizer_utils`.
"""

from ast import literal_eval
import json
import os
from pathlib import Path
import shutil
import nbformat


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters, and converts spaces to hyphens.
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
    Transforms a glossary entry into a data package field. Subroutine of `init_catalog`.

    Parameters
    ----------
    entry: dict, required
        The glossary entry being transformed.

    Returns
    -------
    The packaged glossary entry.
    """
    no_transform_needed = entry['dataset'] == '.' and entry['preferred_format'] in ['csv', 'geojson']

    package = {
        # datapackage fields
        'name': slugify(entry['name']) if 'name' in entry else hash(entry['resource']),
        'datapackage_version': '1.0-beta',
        'title': entry['name'],
        'version': None if 'N/A' not in entry else entry['N/A'],
        'keywords': None if 'keywords_provided' not in entry else entry['keywords_provided'],
        'description': None if 'description' not in entry else entry['description'],
        'licenses': [],
        'sources': [{'title': source} for source in entry['sources']] if 'sources' in entry else None,
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
        # TODO: mirror this field in the glossary
        'metadata': [],
        # signal field
        'complete': no_transform_needed
    }

    # other glossary fields
    for field in ['page_views', 'preferred_mimetype', 'topics_provided', 'preferred_format', 'column_names',
                  'landing_page', 'last_updated', 'protocol', 'created']:
        package[field] = entry[field] if field in entry else None

    # populate the resources field directly, if appropriate.
    if no_transform_needed:
        package['resources'] = [
            {'path': 'data.csv', 'url': entry['resource']}
        ]

    return package


def init_catalog(glossary_filepath, root, max_filesize=None, max_columns=None):
    """
    Initializes a catalog's folder structure.

    Parameters
    ----------
    glossary_filepath: str, required
        The filepath location of the glossary file to be processed.
    root: str, required
        The folder path to which the catalog folder assemblage will be written.
    max_filesize: int or float, optional
        If specified, only glossary entries for resources less than this size in KB will be written to the catalog.
        Otherwise, write everything. Note that glossary entries lacking a non-null `filesize` field will not be
        filtered out.
    max_columns: int, optional
        If specified, only glossary entries for resources less than this length in terms of number of columns will be
        written to the catalog.  Otherwise, write everything. Note that glossary entries lacking a non-null `columns`
        field will not be filtered out.
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

    # Filter by size.
    if max_filesize is not None:
        glossary = [resource for resource in glossary if (('filesize' not in resource) or
                                                          (('s' not in str(resource['filesize']) and
                                                          (resource['filesize'] < max_filesize))))]
    if max_columns is not None:
        glossary = [resource for resource in glossary if (('columns' in resource and resource['columns'] < max_columns)
                                                          or 'columns' not in resource)]

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
                f.write(json.dumps(generate_data_package_from_glossary_entry(entry), indent=4))

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
    Updates the Airscooter DAG so that it reflects the current state of the catalog. This operation creates the
    `.airflow` folder, initializes the `airflow` DAG, and adds all discoverable tasks to the DAG.

    Note

    Parameters
    ----------
    root: str, required
        A folder path to the catalog root folder.
    """
    from airscooter.orchestration import Depositor, Transform

    resource_folders = os.listdir("{0}/tasks/".format(root))
    tasks = []

    def munge_path(path):
        """Helper function. Makes relative filepaths absolute."""
        if os.path.isabs(path):
            return path
        else:
            return str(Path(path).resolve())

    def read_output(fp):
        """Helper function. Reads and returns task outputs."""
        format = fp.rsplit(".")[-1]
        with open(filename, "r") as f:
            if format == 'py':
                last_line = f.readlines()[-1]
                outputs = literal_eval(last_line.split("=")[-1].strip())
            elif format == 'sh':
                last_line = f.readlines()[-1]
                array = last_line.split("=")[-1].strip().replace("(", "").replace(")", "")
                # Bash arrays may contain variables both with and without quotation strings.
                # e.g. OUTPUTS=(Foo Bar "Foo Bar") is legal, and needs to be mapped to ("Foo", "Bar", "Foo Bar").
                vars = array.split(" ")
                outputs = []
                for var in vars:
                    if len(var) > 0:  # avoid parsing multi-spaces
                        outputs.append(var.replace('"', '').replace("'", ''))
            else:  # ipynb
                nb = nbformat.read(fp, as_version=4)
                last_line = nb['cells'][-1]['source']
                outputs = literal_eval(last_line.split("=")[-1].strip())  # same as py at this point

        return [munge_path(out) for out in outputs]

    for folder in resource_folders:

        todo = os.listdir("{0}/tasks/{1}".format(root, folder))

        try:
            depositor = next(f for f in todo if "depositor" in f)
        except StopIteration:
            depositor = None
        try:
            transform = next(f for f in todo if "transform" in f)
        except StopIteration:
            transform = None

        if depositor:
            name = "{0}-depositor".format(folder)
            filename = "{0}/tasks/{1}/{2}".format(root, folder, depositor)

            outputs = read_output(filename)

            py_name = "var_" + name.replace("-", "_")  # clean up the URL slug name so that it can be used as a var
            dep = Depositor(py_name, filename, outputs)
            tasks.append(dep)

        if transform:
            name = "{0}-transform".format(folder)
            filename = "{0}/tasks/{1}/{2}".format(root, folder, transform)
            depositor_prior = tasks[-1]
            inputs = tasks[-1].output

            outputs = read_output(filename)

            py_name = "var_" + name.replace("-", "_")  # clean up the URL slug name so that it can be used as a var
            trans = Transform(py_name, filename, inputs, outputs, requirements=[depositor_prior])
            tasks.append(trans)

    from airscooter.orchestration import (serialize_to_file, write_airflow_string)

    serialize_to_file(tasks, "{0}/.airflow/airscooter.yml".format(root))

    if not os.path.isdir("{0}/.airflow/dags/".format(root)):
        os.mkdir("{0}/.airflow/dags/".format(root))
    write_airflow_string(tasks, "{0}/.airflow/dags/airscooter_dag.py".format(root))


def finalize_catalog(root="."):
    """
    Removes any catalog and task folders that are in an incomplete state. This operation will not touch the
    `.airflow` DAG however: to update *that*, see `update_dag`.

    Parameters
    ----------
    root: str, required
        A folder path to the catalog root folder.
    """
    catalog_folders = os.listdir("{0}/catalog".format(root))

    for folder in catalog_folders:
        with open("{0}/catalog/{1}/datapackage.json".format(root, folder), "r") as f:
            dp = json.load(f)

        if not dp['complete']:
            shutil.rmtree("{0}/catalog/{1}".format(root, folder))
            shutil.rmtree("{0}/tasks/{1}".format(root, folder))

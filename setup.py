from setuptools import setup

setup(
  name='urban_physiology_toolkit',
  packages=['urban_physiology_toolkit'], # this must be the same as the name above
  install_requires=['numpy', 'pandas', 'requests', 'pysocrata', 'bs4', 'requests-file', 'selenium', 'tqdm',
                    'python-magic', 'airscooter', 'nbformat'],
  py_modules=['urban_physiology_toolkit'],
  version='0.0.1',  # note to self: also update the one is the source!
  description='Missing data visualization module for Python.',
  author='Aleksey Bilogur',
  author_email='aleksey.bilogur@gmail.com',
  url='https://github.com/ResidentMario/urban_physiology_toolkit',
  download_url='https://github.com/ResidentMario/urban_physiology_toolkit/tarball/0.0.1',
  keywords=['data', 'data analysis', 'open data', 'civic data', 'data science'],
  classifiers=[],
)

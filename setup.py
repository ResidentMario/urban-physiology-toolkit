from setuptools import setup

setup(
  name = 'urban-physiology-toolkit',
  packages = ['urban-physiology-toolkit'], # this must be the same as the name above
  install_requires=['numpy', 'pandas', 'requests', 'pysocrata', 'bs4', 'requests-file', 'selenium', 'tqdm',
                    'python-magic'],
  py_modules=['urban-physiology-toolkit'],
  version = '0.0.1',  # note to self: also update the one is the source!
  description = 'Missing data visualization module for Python.',
  author = 'Aleksey Bilogur',
  author_email = 'aleksey.bilogur@gmail.com',
  url = 'https://github.com/ResidentMario/urban-physiology-toolkit',
  download_url = 'https://github.com/ResidentMario/urban-physiology-toolkit/tarball/0.0.1',
  keywords = ['data', 'data analysis', 'open data', 'civic data', 'data science'],
  classifiers = [],
)

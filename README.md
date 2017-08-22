# urban-physiology-toolkit ![t](https://img.shields.io/badge/status-alpha-red.svg)

The Urban Physiology pilot project was a research project at NYU CUSP in 2017. The underlying goal of the project 
has been to develop an understanding of the challenges and opportunities in mixing urban datasets together. Is it 
possible to understand the patter of life in a city by examining the data that it publishes about itself?

Part of the challenge to addressing this question is in finding what it is, exactly, that we have to work with in 
the first place. Since municipalities and nations do not publish their data in any particular standard, this is not a
trivial problem to solve: it requires writing a framework for handling all of the different kinds of data and 
different ways it can be published in, one that can conform all of that mess to a well-described, unified 
specification.

This repository (and its sisters) constitute that framework. It constitutes one conceptual half of the output of the
pilot.

## Documentation

This repository's [Wiki](https://github.com/ResidentMario/urban-physiology-toolkit/wiki) serves as the project 
documentation.

## Installation

The entire Urban Physiology workflow requires quite a few packages to install. This is moderately tedious at the 
moment, but should be smoothed out eventually. Do the following:

1. Create and activate a new virtual environment. `conda` is recommended, `virtualenv` is also good. I highly recommend 
not mucking with your root environment.
2. `git` clone this repository (`urban-physiology-toolkit`) locally and install it using `pip install .` Or 
alternatively, run `pip install git+git://github.com/ResidentMario/urban-physiology-toolkit.git`. You will need 
collaborator privileges to do either of these things, as this repository is currently private.
3. Install `airflow` HEAD directly from GitHub by running 
`pip install git+git://github.com/apache/incubator-airflow.git` ([why?](https://github.com/ResidentMario/airscooter#installation)).
4. Install `airscooter` directly from GitHub by running `pip install git+git://github.com/ResidentMario/airscooter.git`.
5. Run either `git clone` and `pip install .`  or 
`pip install git+git://github.com/ResidentMario/airscooter-urban-physiology-plugin.git` (as in step 2) to get the 
[`airscooter-urban-physiology-plugin`](https://github.com/ResidentMario/airscooter-urban-physiology-plugin) localized.
6. Install [PhantomJS](http://phantomjs.org/), and make that available on the system `PATH`. On most NIX systems 
this meant running `nano ~/.bashrc`, scrolling to the bottom, appending 
`:$HOME/$HOME/Desktop/phantomjs-2.1.1-linux-x86_64/bin` to the path list (or whatever the version you downloaded is)
, and then closing and reopening the terminal. To verify that you have added PhantomJS to the path correctly, run `echo 
$HOME` or `phantomjs -v` in the terminal. Note: this step is only necessary if you are using the Socrata glossarizer.

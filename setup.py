#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import os

from setuptools import find_packages, setup

import tsa

with open('requirements.txt') as f:
    required = f.read().splitlines()

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

# Package meta-data.
NAME = 'tsa'
DESCRIPTION = 'Indexing linked data and relationships between datasets.'
URL = 'https://github.com/eghuro/nkod-tsa'
EMAIL = 'alex@eghuro.cz'
AUTHOR = 'Alexandr Mansurov'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = tsa.__version__

# What packages are required for this module to be executed?
REQUIRED = required

# What packages are optional?
EXTRAS = {
    # 'fancy feature': ['django'],
}

# The rest you shouldn't have to touch too much :)
# ------------------------------------------------
# Except, perhaps the License and Trove Classifiers!
# If you do change the License, remember to change the Trove Classifier for that!

here = os.path.abspath(os.path.dirname(__file__))

long_description = readme + '\n\n' + history

# Load the package's __version__.py module as a dictionary.
about = {}
about['__version__'] = VERSION


# Where the magic happens:
setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=['tests', '*.tests', '*.tests.*', 'tests.*']),
    # If your package is a single module, use this instead of 'packages':
    # py_modules=['mypackage'],

    # entry_points={
    #     'console_scripts': ['mycli=mymodule:cli'],
    # },
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    license='BSD',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
        'Development Status :: 3 - Alpha',
        'Framework :: Flask',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Natural Language :: English'
    ]
)

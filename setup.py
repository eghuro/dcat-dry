#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

import tsa

with open('requirements.txt', encoding='utf-8') as requirements_file:
    required_set = set(requirements_file.read().splitlines())
    required_set.discard('statsd==3.3.0')
    required_set.discard('reppy==0.4.14')
    required = list(required_set)

with open('README.rst', encoding='utf-8') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst', encoding='utf-8') as history_file:
    history = history_file.read()

# Package meta-data.
NAME = 'dcat-dry'
DESCRIPTION = 'Indexing linked data and relationships between datasets.'
URL = 'https://github.com/eghuro/dcat-dry'
EMAIL = 'alex@eghuro.cz'
AUTHOR = 'Alexandr Mansurov'
REQUIRES_PYTHON = '>=3.8.7'
VERSION = tsa.__version__

# What packages are required for this module to be executed?
REQUIRED = required

# What packages are optional?
EXTRAS = {
    'monitor': ['statsd==3.3.0'],
    'robots': ['reppy==0.4.14']
}

# The rest you shouldn't have to touch too much :)
# ------------------------------------------------
# Except, perhaps the License and Trove Classifiers!
# If you do change the License, remember to change the Trove Classifier for that!


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
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    zip_safe=False,
    license='MIT',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Development Status :: 3 - Alpha',
        'Framework :: Flask',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Natural Language :: English'
    ]
)

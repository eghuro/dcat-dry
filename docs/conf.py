# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath(".."))

import tsa

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "DCAT DRY"
copyright = "2023, Alexandr Mansurov"
author = "Alexandr Mansurov"
release = "0.6"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "celery.contrib.sphinx",
    "sphinx_click",
    "sphinx.ext.autodoc",
]

templates_path = ["_templates"]
exclude_patterns = []
include_patterns = ["**.rst"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
autosummary_generate = True
celery_task_prefix = []

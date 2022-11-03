# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'pgcopydb'
copyright = '2022, Dimitri Fontaine'
author = 'Dimitri Fontaine'

# The full version, including alpha/beta/rc tags
version = '0.10'
release = '0.10'


# -- General configuration ---------------------------------------------------

#
# Avoid problems with older versions of shpinx as found on debian buster.
#
master_doc = 'index'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


#
# https://stackoverflow.com/questions/9899283/how-do-you-change-the-code-example-font-size-in-latex-pdf-output-with-sphinx
#
from sphinx.highlighting import PygmentsBridge
from pygments.formatters.latex import LatexFormatter


class CustomLatexFormatter(LatexFormatter):
    def __init__(self, **options):
        super(CustomLatexFormatter, self).__init__(**options)
        self.verboptions = r"formatcom=\scriptsize"


PygmentsBridge.latex_formatter = CustomLatexFormatter


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ("ref/pgcopydb", "pgcopydb", "pgcopydb", [author], 1),
    ("ref/pgcopydb_config", "pgcopydb", "pgcopydb", [author], 5,),
    (
        "ref/pgcopydb_clone",
        "pgcopydb clone",
        "pgcopydb clone",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_clone",
        "pgcopydb fork",
        "pgcopydb fork",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_follow",
        "pgcopydb follow",
        "pgcopydb follow",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_snapshot",
        "pgcopydb snapshot",
        "pgcopydb snapshot",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_copy",
        "pgcopydb copy",
        "pgcopydb copy",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_dump",
        "pgcopydb dump",
        "pgcopydb dump",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_restore",
        "pgcopydb restore",
        "pgcopydb restore",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_list",
        "pgcopydb list",
        "pgcopydb list",
        [author],
        1,
    ),
    (
        "ref/pgcopydb_stream",
        "pgcopydb stream",
        "pgcopydb stream",
        [author],
        1,
    ),
]

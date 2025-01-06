# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('..'))



# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'cupsm'
copyright = '2024, Nils Weitzel, Muriel Racky, Laura Braschoss, Kira Rehfeld'
author = 'Nils Weitzel, Muriel Racky, Laura Braschoss, Kira Rehfeld'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'autoapi.extension'
   ]
#extensions = [
#    'sphinx.ext.autodoc',
#    'sphinx.ext.viewcode',
#    'sphinx.ext.napoleon'
#   ]
# Document Python Code
autoapi_dirs = ['../cupsm']

#templates_path = ['_templates']
#exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Exclude all private members (prefix '_')
#autodoc_default_options = {
#    'private-members': False,
#}

autoapi_options = [ 'members', 'undoc-members', 'show-inheritance', 'show-module-summary', 'special-members']
# autoapi_options = [ 'members', 'undoc-members', 'show-inheritance', 'show-module-summary', 'special-members', 'imported-members']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
#html_static_path = ['_static']

.. cupsm documentation master file, created by
   sphinx-quickstart on Thu Dec  5 18:46:41 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to cupsm!
=================================
**Proxy system modeling with data cubes**

cupsm is an open-source python package for proxy system modeling that makes use of xarray and dask for efficient data handling and parallelization.

Introduction
---------------------------------------

To get started, clone the package from github with ``git clone https://github.com/paleovar/cupsm.git``. You can create an environmental with all dependencies using the ``condaenv_python-3.11.7.yml`` file. Browse through the jupyter notebooks in the tutorials directory to learn everything you need to know about the package. All modules are documented in the API reference.

.. image:: package_structure1.png
   :width: 600
   :alt: Package structure

Documentation
--------------

* :doc:`concepts`
* :doc:`objects`
* :doc:`tutorial1`


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Contents:

   concepts
   objects
   tutorial1
   modules

License
---------------------------------------
cupsm is published under the MIT License (Copyright (c) 2024, Nils Weitzel, Muriel Racky, Laura Braschoss, Kira Rehfeld)

Acknowledgments
---------------------------------------

This work has been funded by the German Research Foundation (`NFDI4Earth <https://www.nfdi4earth.de/>`_, DFG project no. 460036893).

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

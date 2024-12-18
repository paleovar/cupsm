``cupsm`` objects
=================================

Two base objects are needed for model-proxy comparison, one for ESM output (``sim_data``) and one for proxy data (``obs_data``). The obs_data object is also used to store forward-model proxy time series, which have the same structure as proxy data. PSM operators are mapping between ``sim_data`` and ``obs_data`` objects (or intermediate products in the case of sequential application of operators). The structure of the two objects is depicted in the figure. ``cupsm`` builds on the python packages `xarray <https://docs.xarray.dev/en/stable/>`_ and `dask <https://www.dask.org/>`_. 

.. image:: site_object.png
   :width: 600
   :alt: Structure of the base objects in cupsm

*Figure description: Measurement operators translate the sim_data (top, given by a chunked xarray DataArray for the variable ‘tos’, which is the CMOR variable name of SST, with space and time dimensions) into an obs_data object (bottom), which is given by N site_objects. Each site_object contains an xarray Dataset with dimensions depth and ens.*

``sim_data``
---------------------------------------

We load simulation data as an ``xarray.Dataset`` which has coordinates ``time``, ``lon``, ``lat``, ``level`` (vertical dimension), and optionally ``ens`` (ensemble members). Each ESM variable is loaded as an ``xarray.DataArray`` within the ``xarray.Dataset``. Using a standard xarray object structure has the advantage that existing ESM processing functionalities can be applied in addition to PSM operators. The ``xarray.Dataset`` structure facilitates lazy loading and parallelization of operations by chunking datasets, which is of importance for the large data size of long paleoclimate simulations.

``obs_data`` and ``site_object``
---------------------------------------

In collections of proxy records, the number of samples and the measured and reconstructed variables tend to differ between measurement sites (here site refers to a specific location where a proxy archive is collected; archives can for example be sediment or ice cores). We use an ``xarray.Dataset`` for data from a single site (``site_object``), which has the dimensions ``depth`` or ``age`` (samples are identified by depth in a sediment core or by their inferred ages) and ``ens`` (ensemble members to quantify uncertainties), and can store four types of variables: chronological data, measured proxy data, inferred variables such as temperature reconstructions, and forward-modeled proxy time series derived from applying a PSM to ESM output The ``site_objects`` contain relevant metadata as attributes. Our ``obs_data`` object is a dictionary or list of site_objects. The dictionary/list structure allows loading of the ``site_object`` data based on metadata filtering, by first creating an overview table containing only the site metadata before loading the site data into memory in a second step. We demonstrate parallelization over ``site_objects`` with the python library dask. Within one ``site_object``, operations can also be parallelized using existing xarray functionalities.

PSM operators
---------------------------------------

So far, three types of operators are implemented in ``cupsm``: space operators (``field2site``) that map the spatial fields of the ``sim_data`` onto the spatial structure of the ``site_objects``, chronology operators (``time2chron``) that map data from the regular ``sim_data`` time axis onto the irregular ``site_object`` time axis, and exemplifying variable operators that perturb the data in ``sim_data`` or ``site_objects`` with either white or autocorrelated noise to imitate uncertainties from the proxy-climate relationship and archival processes.

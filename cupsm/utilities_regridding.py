"""
For regridding data with the xesmf package
- load xesmf package (until now)
"""
import xarray as xr
import numpy as np

# for using xesmf
from pathlib import Path
import os
if 'ESMFMKFILE' not in os.environ:
    # RTD doesn't activate the env, and esmpy depends on a env var set there
    # We assume the `os` package is in {ENV}/lib/pythonX.X/os.py
    # See conda-forge/esmf-feedstock#91 and readthedocs/readthedocs.org#4067
    os.environ['ESMFMKFILE'] = str(Path(os.__file__).parent.parent / 'esmf.mk')
import xesmf as xe

# in and out file
examplefile = '/data/projects/nfdi4earth/test_data/tos_Omon_MPI-ESM1-2-CR_transient-deglaciation-prescribed-glac1d_r1i1p3f2_gn_1000101-1010012.nc'
ds_example = xr.open_dataset(examplefile)

ds_out = xr.Dataset(
    {
        "lat": (["lat"], np.linspace(-90, 90, 101)),
        "lon": (["lon"], np.linspace(-180, 180, 123)[:-1]),
    }
)

def get_regridder(do_in=ds_example, do_out=ds_out, method="bilinear", periodic=True,
                  ignore_degenerate=True):   
    # create the regridder:
    regridder = xe.Regridder(do_in, do_out, method=method, periodic=periodic, 
                             ignore_degenerate=ignore_degenerate)#, extrap_method='nearest_s2d')
    return regridder

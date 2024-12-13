"""
- Code for regridding the example data
- function "transfer_timeaxis_deglac"
"""

# ~~~~~~~~~~~~~~~~~~~
#      Regridding
# ~~~~~~~~~~~~~~~~~~~

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
examplefile = ""
if examplefile == "":
    raise ValueError("")
    
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
                             ignore_degenerate=ignore_degenerate)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
#  Time axis transformation
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

def transfer_timeaxis_deglac(data, return_np_years=False):
    """
    Returns the data array with a transformed time axis based on the following assumption:
    ------------------------------------
    - SPECIFIC for the example data!! -
    ------------------------------------
    This function assumes that the original time axis is in simulation years, since there are 
    300 000 monthly timesteps --> 25 000 simulation years in total.
    The simulation starts in 25 kiloyears before present (simulation year 0) and goes to
    0 kiloyears before present (simulation year 24 999).

    Parameters:
    ----------
    return_np_years:  returns additionally the years as numpy array
    """
    # no in place changes
    data = data.copy()
    
    # extract time axis
    time_values = data.time.values
    # empty list for shifted date time objects
    new_time_values = []
    years_list = []
    
    # as far as I know no vectorization available :/
    for val in time_values:
        dt = val.replace(year = 25001 - val.year)
        new_time_values.append(dt)
        years_list.append(dt.year)
    
    # replace time axis in data array
    data["time"] = np.array(new_time_values)
    #data["time"].attrs = {"units": 'days since 0000-01-01'}
    
    if not return_np_years:
        return data
    else:
        return data, np.array(years_list)
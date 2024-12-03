path_lipd = "/data/obs/proxy_databases/PalMod130k/PALMOD130k_v1_0_1_250320/LiPD/"

import numpy as np


# funciton to transfer time axis
def transfer_timeaxis_deglac(data, assumption="mpi_deglac", return_np_years=False):
    """
    returns the data array with a changed time axis based on the assumption

    return_np_years: returns additionally the years as numpy array
    
    available assumptions:
    ----------------
    - mpi_deglac: -
    ----------------
    I assume that the original time axis is in simulation years, since there are 
    300 000 monthly timesteps --> 25 000 simulation years in total.
    I assume the simulation covers 25 ka (simulation year 0) to 0 ka (simulation year 24 999)

    """
    # no in place changes
    data = data.copy()
    
    if assumption != "mpi_deglac": 
        raise KeyError("No other transfer assumption has been implemented")
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

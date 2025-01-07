"""
- Code for regridding the example data
- function "transfer_timeaxis_deglac"
"""

# ~~~~~~~~~~~~~~~~~~~
#      Regridding
# ~~~~~~~~~~~~~~~~~~~

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt

# Code that only runs when the script is executed directly
if __name__ == "__main__":
    
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
        raise ValueError("No example file provided! To set up the regridder, one must provide an example input grid. Please use one of the the downloaded files.")
        
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


# ~~~~~~~~~~~~~~~~~~~~~
#  Plotting function
# ~~~~~~~~~~~~~~~~~~~~~

def plot_results(site_list, obs_data, res_sim_data, for_obs_data, mapping_vars):
    
    fig, axes = plt.subplots(2, 3, figsize=(12,6), sharex=True, sharey=True)
    axes = axes.flatten()
    
    # Container to hold the lines plotted
    lines = []
    
    # Iterate through axes
    for i,site_name in enumerate(site_list):
        if site_name == "legend":
            axes[i].axis('off')
            continue
        for site_object in obs_data.values():
            if site_name == site_object.site_name:
                id = site_object.site_name
                ax = axes[i]
                letter = ["a) ", "b) ", None, "c) ", "d) ", "e) "][i]
                lon, lat, _ = site_object.coords
                title = f"{letter}{id} ({int(np.round(lon))}°E, {int(np.round(lat))}°N)"
                ax.set_title(title, loc="left")
                ax.set_xlim(18,10)
                # Set ticks
                #ax.set_xticks(np.arange(0,30e3,5000), minor=True)
                #ax.set_yticks(np.arange(-7.5,5,2.5), minor=True)
                #ax.grid(which='both')
                # prepare data
                model_data = res_sim_data[0][id] # add [0] because dask function returns a tuple
                psm_data = for_obs_data[0][id]
                record_data = site_object.load(quiet=True)
                var = mapping_vars[id]
                # merge coords such that data is of same shape
                data = xr.combine_by_coords([psm_data, record_data.age, record_data.ageEnsemble.T, record_data[var]], join="right").copy()
                #---------------
                ## plot 
                # model data
                line1, = ax.plot(model_data.year/1000, model_data,
                                color="silver", zorder=0)
                # paleo data
                mask = ~np.isnan(data[var])
                
                for j,age in enumerate(data.ageEnsemble):
                    # plot original paleo data
                    ax.plot(age[mask], data[var][mask],
                            alpha=0.01, color="teal", lw=0.5)
                    # plot forward model paleo data
                    ax.plot(age, data.tos[:,j],
                            alpha=0.01, color="blue", lw=0.5)
                
                # plot age model median as thin black line
                ax.plot(data.age[mask], data[var][mask], color="black", lw=0.5)
                
                # dirty hack for legend:
                if i == 1:
                    ax.plot([], [] ,color="silver", label="Simulation")
                    ax.plot([], [] ,color="teal", label="Temperature\nreconstruction")
                    ax.plot([], [] ,color="blue", label="Forward-modeled\nproxy time series")
                    ax.legend(loc="center right", bbox_to_anchor=(2,0.5), frameon=False)
                            
                    
    
    axes[0].set_ylabel("SST anomaly (K)")
    axes[3].set_ylabel("SST anomaly (K)")
    axes[3].set_xlabel("Time (ka BP)")
    axes[4].set_xlabel("Time (ka BP)")
    axes[5].set_xlabel("Time (ka BP)")
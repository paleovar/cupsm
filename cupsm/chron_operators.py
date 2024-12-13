"""
The code of this module deals with the time axes of the simulation data and the proxy data. The forward-modeling operator "time2chron" resamples the simulation data according to the target requirements and the chronology data (age ensemble) of the site object.
It contains:
- chron operator "time2chron" 
- chron operator helpers
    - function "resample_sim_data"
    - function "provide_chron_data"
"""
# Further helper functions (excluded from ReadTheDocs documentation)
#    - function "sampfunc_slice2point"
#    - function "sampfunc_point2point"
#    - function "create_bounds_adjacent"
#    - function "create_bounds_distant"

# Imports
from .utilities import *
import numpy as np
import xarray as xr
import pandas as pd

# ~~~~~~~~~~~~~~~~~~~~~
# Chron operator
# ~~~~~~~~~~~~~~~~~~~~~
def time2chron(sim_data, site_obj, target,
               method="point2point", sampling=None, sampling_size=None,
               quiet=False, return_resampled=False):
    """
    Resamples the simulation data in time according to the target requirements and the chronology data (age ensemble) of the site object, 
    using the provided pointing method. 
    The obtained forward-modelled proxy time series object is returned as a xarray DataArray.

    Note:
    ----
    --> In the case that the simulation data itself contains multiple ensemble members (e.g. created by adding additional noise due to application of
        cupsm.white_noise() or cupsm.ar1_noise()), each age ensemble member is paired with a randomly selected simulation data ensemble member during
        resampling. Hereby the first simulation data ensemble member is excluded from the random selection, as it is assumed to contain the original data
        that is free from additionally created noise (see also the documentation of cupsm.white_noise() or cupsm.ar1_noise()). 
    ---> Warning: The dimension of simulation data ensemble members must be named "ensemble_member" to ensure the functionality of the operator.

    Parameters:
    ----------
    sim_data        : xarray DataArray of simulation data interpolated to the site location of interest (e.g. precomputed with cupsm.field2site()).
    
    site_obj        : Site object of interest (python class object created from lipd file of interest by applying cupsm.get_records_df(), see
                        cupsm.get_records_df() documentation for more details).
                        
    target          : Target object containing meta information for the forward proxy object.
                        
    method          : string; pointing method between simulation and proxy time axis. Available keywords are:
                            - point2point: For resampling the simulation data, the time axes of the simulation data and the chronology data
                                             are compared point to point. The target variable from the simulation data is then selected at those
                                             time steps. Faster.
                            - slice2point: This method assumes that a measurement point in the proxy data is actually an integration over a 
                                             measured slice (e.g., a slice from a sediment core). Thus, this method compares a time slice 
                                             in time axis of the simulation data to the chronology data of the proxy record. The variable from 
                                             the simulation data is then selected within these time slices and averaged over the slice.
                                             The method makes different assumptions depending on the keyword "sampling" which determines the slice 
                                             sizes (see below). Takes longer.
                        Default is "point2point".
                        
    sampling        : string; sampling method, determines slice bounds and sizes. Only used if pointing method is "slice2point". Available keywords are: 
                            - "adjacent": This method assumes that the entire material of a proxy record (e.g., a sediment core) is used for the measurement
                                          and that resulting slices are adjacent. The depth axis of the proxy data corresponds to the midpoints of these slices. 
                                          The method determines the upper and lower bounds by halving the distance between two consecutive depth values.
                            - "distant":  This method assumes that the measurements in the proxy record material were taken at the depth values given in the
                                          depth axis of the proxy data. These are the midpoints of a measurement sampling slice with a fixed sampling size, which
                                          is set by the "sampling_size" keyword (see below).
                        Default is None, must be changed if method is changed to "slice2point".

    sampling_size   : integer; length of the sample in the depth axis in millimeter, only used if sampling method is "distant". 
                        Default is 10mm.

    quiet           : boolean; print (False) or suppress (True) diagnostic output. Default is False.

    return_resampled: boolean; if True, the simulation data is returned after resampling in time according to 
                        the target object attributes as xarray Dataarray. Default is False.
    """
    ## Prior checks:
    # Checks:
    if method not in ['point2point','slice2point']:
        raise ValueError("method must be either 'point2point' or 'slice2point'.")
    if method == "slice2point":
        if sampling == None or sampling not in  ['adjacent', 'distant']:
            raise ValueError("If method is 'slice2point', sampling must be either 'adjacent' or 'distant'")
        if sampling == "distant" and sampling_size is None:
            print("A default sampling size of 10 millimeter is used.")
            sampling_size = 10
    
    ## Simulation data
    # name of the sim_data variable
    varname = sim_data.name
    
    # resample simulation data given the target's requirements
    if 'ensemble_member' in sim_data.coords and sim_data.ensemble_member.ndim!=0: 
        a=[]
        for i in sim_data.ensemble_member.values:
            a.append(resample_sim_data(sim_data.sel(ensemble_member=i), target, site_obj.coords[1]))
        sim_data = xr.concat(a,dim="ensemble_member")
        sim_noise=True
    else:
        sim_data = resample_sim_data(sim_data, target, site_obj.coords[1])
        sim_noise=False

    ## Chronology data
    chron_data = provide_chron_data(site_obj=site_obj, sim_data=sim_data, quiet=quiet)
    
    ## Time mapping
    # prepare iteration through ensemble axis
    n_depth, n_ens = chron_data.shape
    forward_proxy = np.full(chron_data.shape, np.nan) 

    #iterate through ensemble axis
    for i,ens_chron in enumerate(chron_data.T):
        ens_chron_d = ens_chron
        ens_chron = ens_chron.rename({"depth":"year"}).assign_coords({"year": ens_chron.values})
        # Avoid duplicates due to nans
        ens_chron_red= ens_chron[np.isnan(ens_chron.values)==False] 
        if sim_noise==True:
            # choose sim noise ensemble member randomly
            sim_data_rand=sim_data.sel(ensemble_member=np.random.choice(list(sim_data.ensemble_member.values)[1:]))
        else: 
            # if no multiple ensemble members, choose the same sim_data for each chron ens member 
            sim_data_rand=sim_data

        if method == "point2point":
            forward_proxy = sampfunc_point2point(i=i, forward_proxy=forward_proxy, ens_chron=ens_chron,
                                                 ens_chron_red=ens_chron_red, sim_data_rand=sim_data_rand,quiet=quiet)

        elif method == "slice2point":
            forward_proxy = sampfunc_slice2point(i=i, forward_proxy=forward_proxy, ens_chron=ens_chron,
                                                 ens_chron_d = ens_chron_d,
                                                 ens_chron_red=ens_chron_red, sim_data_rand=sim_data_rand,
                                                 sampling=sampling, sampling_size=sampling_size,quiet=quiet)

    # create xr.DataArray for forward proxy object
    forward_proxy = xr.DataArray(data=forward_proxy, dims=chron_data.dims, 
                                 coords=chron_data.coords, name=varname)
    forward_proxy.attrs = {"site": site_obj.sitename,
                           "lon": site_obj.coords[0],
                           "lat": site_obj.coords[1],
                              }
    if hasattr(sim_data, "units"):
        forward_proxy.attrs["units"] = sim_data.attrs["units"]
    
    if return_resampled:
    	return forward_proxy, sim_data
    else:
    	return forward_proxy

# ~~~~~~~~~~~~~~~~~~~~~~
# Helper functions
# ~~~~~~~~~~~~~~~~~~~~~~
def resample_sim_data(sim_data, target, lat_coord): 
    """
    Resamples the given simulation data based on the attributes of the target object. Returns result as a xarray Dataarray. Helper function
    for cupsm.time2chron().

    Parameters:
    ----------
    sim_data        : xarray Dataarray of simulation data interpolated to the site location of interest (e.g. precomputed with cupsm.field2site()).                  
    target          : Target object containing meta information for the forward proxy object.
    lat_coord       : float; latitude of site location in degrees North.
    """
    # sort time axis of the simulation data (that resampling works)
    sim_data = sim_data.sortby("time")

    # resampling
    if hasattr(target, "habitatSeason"):
        if target.habitatSeason not in ["annual", "unknown"]:
            # latitude decides over month, season is local
            if lat_coord < 0:
                season_mapping = {"winter" : "JJA", "summer" : "DJF"}   
            else:
                season_mapping = {"summer" : "JJA", "winter" : "DJF"}
            season = season_mapping[target.habitatSeason]
            # chose target month
            if target.month_i is None:
                month_i = {"JJA" : [6, 7, 8], "DJF" : [12, 1, 2] }[season]
            else:
                month_i = target.month_i 
            
            # resampling, not weighted by length of month and no calendar adjustment
            seasonal_data = sim_data.where(sim_data['time.month'].isin(month_i), drop=True)
            resampled = seasonal_data.resample(time="1Y").mean("time")
            # convert time axis to years as integers
            resampled = resampled.groupby("time.year").mean("time")
            
        else:
            resampled = sim_data.resample(time="1Y").mean("time")
            resampled = resampled.groupby("time.year").mean("time")
    else:
        pass
    return resampled

def provide_chron_data (site_obj, sim_data, quiet):
    """
    Converts site object chronology data from kiloyears to years, rounds it to annual scale and cuts it accordingly to the age limits of the provided simulation data. 
    The result is returned as a xarray Datarray. Helper function for cupsm.time2chron().
    
    Parameters:
    ----------
    site_obj        : Site object of interest (python class object created from lipd file of interest by applying cupsm.get_records_df(), see
                        cupsm.get_records_df() documentation for more details).
    sim_data        : xarray Dataarray of simulation data interpolated to the site location of interest (e.g. precomputed with cupsm.field2site()) and resampled
                        in time according to the target object attributes (as done by the helper function cupsm.resample_sim_data(), see documentation of 
                        cupsm.resample_sim_data for more details).
    quiet           : boolean; if True plot chronology data after reduction to relevant ages.
    """
    # load chron data
    chron_data = site_obj.load_chron_data()
    # convert age data to ka (comparison beyond annual scale not reasonable)
    chron_data = (chron_data * 1000).round().astype(int) 
    # cut the chron data to simulation years min and max
    simy_min = sim_data.year.min().values
    simy_max = sim_data.year.max().values
    chron_data = chron_data.where((chron_data <= simy_max) & (chron_data >= simy_min), drop=True)

    if not quiet:
        print("Chron data after reduction to relevant ages:")
        chron_data.plot()
        plt.show()
    return chron_data

def _sampfunc_slice2point(i, forward_proxy, ens_chron, ens_chron_d,
                         ens_chron_red, sim_data_rand,
                         sampling, sampling_size, quiet):
    """
    Performs year to slice sampling between a member of the age ensemble and the simulation data. Returns results as entries
    of a numpy.ndarray of the shape of the chronology data. Helper function for cupsm.time2chron().

    Parameters:
    ----------
    i              : integer; index of age ensemble member of interest
    forward_proxy  : numpy.ndarray in the shape of the chronology data that was loaded via cupsm.provide_chron_data(); see documentation of 
                       cupsm.provide_chron_data() for more details.
    ens_chron      : xarray Dataarray; age ensemble member of interest after transforming depth to year axis
                       #ens_chron = ens_chron.rename({"depth":"year"}).assign_coords({"year":  ens_chron.values})            
    ens_chron_d    : xarray Dataarray; age ensemble member of interest 
    ens_chron_red  : xarray Dataaray; obtained by removing NaN values from "ens_chron"
    sim_data_rand  : xarray Dataarray; ensemble member of interest of simulation data interpolated to the site location of interest
                      (e.g. precomputed with cupsm.field2site()) and resampled in time according to the target object attributes 
                      (as done by the helper function cupsm.resample_sim_data(), see documentation of cupsm.resample_sim_data for more details). 
                        
    sampling       : string; sampling method. Available keywords: "adjacent" (whole core was sampled)
                       and "distant" (samples of a certain sampling size with a certain sampling distance).

    sampling_size  : integer; length of the sample in the depth axis in millimeter, only used if sampling method is "distant". 

    quiet           : boolean; print (False) or suppress (True) diagnostic output. Default is False.
    """
    
    ## Create bounds
    if sampling == "adjacent":
        lower_bounds, upper_bounds = create_bounds_adjacent(ens_chron=ens_chron,
                                                            ens_chron_red=ens_chron_red, 
                                                            sim_data_rand=sim_data_rand, 
                                                            quiet=quiet)
    elif sampling == "distant":
        lower_bounds, upper_bounds = create_bounds_distant(ens_chron=ens_chron,
                                                           ens_chron_d = ens_chron_d,
                                                           ens_chron_red=ens_chron_red, 
                                                           sim_data_rand=sim_data_rand,
                                                           sampling_size = sampling_size,
                                                           quiet=quiet)
    # unfortunately elementwise (iterate through elements of ens member (actually their bounds))
    # iteration through .notnull() to jump over np.nans, but keep index j correctly running
    for j, notnull in enumerate(ens_chron.notnull()):
        if notnull == False: 
            continue
        # bounds
        lower = lower_bounds[j]
        upper = upper_bounds[j]
        # mask and mean
        mean = sim_data_rand.sel(year=slice(lower, upper)).mean()
        # write data in
        forward_proxy[j,i] = mean
        
    return forward_proxy 

def _sampfunc_point2point(i, forward_proxy, ens_chron,
                         ens_chron_red, sim_data_rand, quiet):
    """
    Performs year to year sampling between a member of the age ensemble and the simulation data. Returns results as entries
    of a xarray Dataarray of the shape of the chronology data. Helper function for cupsm.time2chron().

    
    Parameters:
    ----------
    i              : integer; index of age ensemble member of interest
    forward_proxy  : numpy.ndarray in the shape of the chronology data that was loaded via cupsm.provide_chron_data(); see documentation of 
                       cupsm.provide_chron_data() for more details.
    ens_chron      : xarray Dataarray; age ensemble member of interest after transforming depth to year axis            
    ens_chron_red  : xarray Dataaray; obtained by removing NaN values from "ens_chron"
    sim_data_rand  : xarray Dataarray; ensemble member of interest of simulation data interpolated to the site location of interest
                      (e.g. precomputed with cupsm.field2site()) and resampled in time according to the target object attributes 
                      (as done by the helper function cupsm.resample_sim_data(), see documentation of cupsm.resample_sim_data for more details).
    quiet          : boolean; if True prints out information about potential year duplicates in the age model. Default is False.
    """
    varname = sim_data_rand.name
    try:
        forward_proxy[:,i][ens_chron.notnull()]=xr.merge([ens_chron_red,sim_data_rand],join="left")[varname].values # merge excluding nans
    except ValueError:
        # this happens due to year duplicates in the age model
        if not quiet:
            print(f"Site {site_obj.sitename}: For chron ensemble member {i+1}, the age column contains duplicates.")
        ens_chron_unique=ens_chron_red.drop_duplicates(dim="year",keep=False) # drop all duplicates
        duplicates=set(list(ens_chron_red.year.values)).difference(list(ens_chron_unique.year.values)) # years for which we have duplicates
        if not quiet:
            print("Years with duplicates:"+str(len(duplicates)))
        ens_chron_copy=np.copy(ens_chron) # simple copy of values 
        for d in duplicates:
            sim_ind=np.where((sim_data_rand.year==d)==True)
            forward_proxy[np.where(ens_chron==d),i]=sim_data_rand[sim_ind].values 
            ens_chron_copy[np.where(ens_chron==d)]=np.nan # set duplicates to nans in ens_chron_copy
        forward_proxy[:,i][np.isnan(ens_chron_copy)==False]=xr.merge([ens_chron_unique,sim_data_rand],join="left")[varname].values # merge exclud. duplicates
    return forward_proxy

def _create_bounds_adjacent(ens_chron, ens_chron_red, sim_data_rand, quiet):
    """
    Determines the upper and lower bounds of the time slices for the simulation data over which will be averaged. Assumes adjacent slices.
    Helper function for cupsm.sampfunc_slice2point().
    """
    # create 2 empty arrays
    lower_bounds = np.full(ens_chron_red.shape, 0) 
    upper_bounds = lower_bounds.copy()
    
    #write values in
    lower_bounds[1:] = ens_chron_red.values[1:] - ens_chron_red.diff(dim="year").values/2
    lower_bounds[0] = ens_chron_red.values[0] - ens_chron_red.diff(dim="year").values[0]/2
    
    upper_bounds[:-1] = lower_bounds[1:]
    upper_bounds[-1] = ens_chron_red.values[-1] + ens_chron_red.diff(dim="year").values[-1]/2
    
    
    # check bounds with sim data
    if upper_bounds.max() > sim_data_rand.year.max():
        #if not quiet:
            #print("""Simulation data does not cover planned slices 
            #--> upper bounds are cut to simulation data availability""")
        upper_bounds[-1] = sim_data_rand[-1].year.values
    
    if lower_bounds.min() < sim_data_rand.year.min():
        #if not quiet: 
            #print("""Simulation data does not cover planned slices 
            #--> lower bounds are cut to simulation data availability""")
        lower_bounds[0] = sim_data_rand[0].year.values

    # bring back into original (nan containing shape)
    lower_bounds_with_nan = np.full(ens_chron.shape, np.nan) 
    upper_bounds_with_nan = lower_bounds_with_nan.copy()

    # put in data
    lower_bounds_with_nan[ens_chron.notnull()] = lower_bounds
    upper_bounds_with_nan[ens_chron.notnull()] = upper_bounds
    
    return lower_bounds_with_nan, upper_bounds_with_nan

def _create_bounds_distant(ens_chron, ens_chron_d, ens_chron_red, sim_data_rand, sampling_size, quiet):
    """
    Determines the upper and lower bounds of the time slices for the simulation data over which will be averaged. Assumes distant slices.
    Helper function for cupsm.sampfunc_slice2point().
    """
    # mask nans out, but still in depth space:
    depth_red = ens_chron_d.depth[ens_chron.notnull().values]

    # interpolate to finer depth axis
    step=0.001 #resolution in m
    depth_interp = np.arange(depth_red[0], depth_red[-1]+step,step=step) #target depth axis
    ages_interp = np.round(np.interp(depth_interp, depth_red, ens_chron_red)).astype(int) #interpolated ages

    # create DataArrays:
    depth_interp = xr.DataArray(data=depth_interp, dims=depth_red.dims, coords={"depth":depth_interp}, name="depth")
    ages_interp = xr.DataArray(data=ages_interp, dims="year", coords={"year":ages_interp}, name="ageEnsemble")

    # bounds in depth space (unit: meter)
    lower_bounds = depth_red - sampling_size * 0.001 * 0.5 #half the sampling size, from mm to m
    upper_bounds = depth_red + sampling_size * 0.001 * 0.5 #half the sampling size, from mm to m
    
    # find appropriate ages
    # depth_interp.isin(depth_interp.sel(depth=upper_bounds, method="nearest")).values :
    # creates a mask of True and False where elements are equal, then ages are indexed
    lower_bounds = ages_interp[depth_interp.isin(depth_interp.sel(depth=lower_bounds, method="nearest")).values]
    upper_bounds = ages_interp[depth_interp.isin(depth_interp.sel(depth=upper_bounds, method="nearest")).values]
    
    # correct first lower value and last upper value (is due to interpolation the covered age range)
    # by symmetric mirroring of distance
    lower_bounds[0] = lower_bounds[0] - (upper_bounds[0] - lower_bounds[0])
    upper_bounds[-1] = upper_bounds[-1] + (upper_bounds[-1] - lower_bounds[-1])

    # check bounds with sim data
    if upper_bounds.max() > sim_data_rand.year.max():
        #if not quiet:
            #print("""Simulation data does not cover planned slices 
            #--> upper bounds are cut to simulation data availability""")
        upper_bounds[-1] = sim_data_rand[-1].year.values
    
    if lower_bounds.min() < sim_data_rand.year.min():
        #if not quiet: 
            #print("""Simulation data does not cover planned slices 
            #--> lower bounds are cut to simulation data availability""")
        lower_bounds[0] = sim_data_rand[0].year.values

    # bring back into original (nan containing shape)
    lower_bounds_with_nan = np.full(ens_chron.shape, np.nan) 
    upper_bounds_with_nan = lower_bounds_with_nan.copy()

    # put in data
    lower_bounds_with_nan[ens_chron.notnull()] = lower_bounds
    upper_bounds_with_nan[ens_chron.notnull()] = upper_bounds
    
    return lower_bounds_with_nan, upper_bounds_with_nan
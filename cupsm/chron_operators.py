"""
- chron operator "time2chron" (only point2point implemented)
- chron operator helpers
    - methods (functions) of the chron operator (more to be implemented, not separated yet)
    - function "resample_sim_data"
- misc:
    - "transfer_timeaxis_deglac" transfer time axis of mpi-esm deglac sim
"""
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
    Resamples the simulation data in time axis according to the time information of the
    observation data object using the provided method. The forward-modelled proxy time 
    series object is added to the data of the site_obj.

    sim_data:       Simulation data
    site_obj:       Site object
    target:         Target variable object

    method:         string, valid arguments are:
                    - "point2point"
                    - "slice2point"
    sampling:       string, only used if method is "slice2point", valid arguments are:
                    - "adjacent" (whole core was sampled) 
                    - "distant" (samples of a certain sampling size with a certain sampling distance)

    sampling_size:  integer, length of the sample in the depth axis in mm, 
                    only used if sampling is "distant"

    quiet:          boolean, True: no diagnostic output is printed
    """
    ## Prior checks:
    # Checks:
    if method not in ['point2point','slice2point']:
        raise ValueError("method must be either 'point2point' or 'slice2point'.")
    if method == "slice2point":
        if sampling == None or sampling not in  ['adjacent', 'distant']:
            raise ValueError("If method is 'slice2point', sampling must be either 'adjacent' or 'distant'")
        if sampling == "distant" and sampling_size is None:
            print("A default sampling size of 10mm is used.")
            sampling_size = 10
    
    ## Simulation data
    # name of the sim_data variable
    varname = sim_data.name
    
    # resample simulation data given the targets requirements
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

# ~~~~~~~~~~~~~~~~~~~~~
# Old/Other versions
# ~~~~~~~~~~~~~~~~~~~~~
# version 27_08_24
def time2chron_wn(sim_data, site_obj, target, method="point2point", quiet=False, return_resampled=False):
    """
    Resamples the simulation data in time axis according to the time information of the
    observation data object using the provided method. The forward-modelled proxy time 
    series object is added to the data of the site_obj.

    sim_data:     Simulation data
    site_obj:     Site object
    target:       Target variable object

    method:


    quiet:        Print diagnostic output
    """
    # load chron data
    chron_data = site_obj.load_chron_data()
    # convert age data to ka (comparison beyond annual scale not reasonable)
    chron_data = (chron_data * 1000).round().astype(int) 

    # name of the sim_data variable
    varname = sim_data.name
    # resample given the targets requirements
    if 'ensemble_member' in sim_data.coords and sim_data.ensemble_member.ndim!=0:
        a=[]
        for i in sim_data.ensemble_member.values:
            a.append(resample_sim_data(sim_data.sel(ensemble_member=i), target, site_obj.coords[1]))
        sim_data = xr.concat(a,dim="ensemble_member")
    else:
        sim_data = resample_sim_data(sim_data, target, site_obj.coords[1])    
        
    # cut the chron data to simulation years min and max
    simy_min = sim_data.year.min().values
    simy_max = sim_data.year.max().values
    chron_data = chron_data.where((chron_data <= simy_max) & (chron_data >= simy_min), drop=True)
    
    # prepare iteration through ensemble axis
    n_depth, n_ens = chron_data.shape
    forward_proxy = np.full(chron_data.shape, np.nan) 

    # check for noise in simulation data
    sim_noise=False
    if "ensemble_member" in sim_data.coords and sim_data.ensemble_member.ndim!=0:
        sim_noise=True

    #iterate through ensemble axis
    for i,ens_chron in enumerate(chron_data.T):
        ens_chron = ens_chron.rename({"depth":"year"}).assign_coords({"year": ens_chron.values})
        ens_chron_red= ens_chron[np.isnan(ens_chron.values)==False] # to avoid duplicates due to nans
        if sim_noise==True:
            sim_data_rand=sim_data.sel(ensemble_member=np.random.choice(list(sim_data.ensemble_member.values)[1:])) # choose sim noise ensemble members randomly
        else: 
            sim_data_rand=sim_data # if no multiple ensemble members, choose the same sim_data for each chron ens member 
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

# (version 24_04_24)
def time2chron_old(sim_data, site_obj, target, method="point2point", quiet=False, return_resampled=False):
    """
    Resamples the simulation data in time axis according to the time information of the
    observation data object using the provided method. The forward-modelled proxy time 
    series object is added to the data of the site_obj.

    sim_data:     Simulation data
    site_obj:     Site object
    target:       Target variable object

    method:


    quiet:        Print diagnostic output
    """
    # load chron data
    chron_data = site_obj.load_chron_data()
    # convert age data to ka (comparison beyond annual scale not reasonable)
    chron_data = (chron_data * 1000).round().astype(int) 

    # name of the sim_data variable
    varname = sim_data.name
    # resample given the targets requirements
    sim_data = resample_sim_data(sim_data, target, site_obj.coords[1])

    # cut the chron data to simulation years min and max
    simy_min = sim_data.year.min().values
    simy_max = sim_data.year.max().values
    #print(simy_min,simy_max)
    chron_data = chron_data.where((chron_data <= simy_max) & (chron_data >= simy_min), drop=True)
    
    # prepare iteration thorugh ensemble axis
    n_depth, n_ens = chron_data.shape
    forward_proxy = np.full(chron_data.shape, np.nan) 

    #iterate through ensemble axis
    for i,ens_chron in enumerate(chron_data.T):
        ens_chron = ens_chron.rename({"depth":"year"}).assign_coords({"year": ens_chron.values})
        ens_chron_red= ens_chron[np.isnan(ens_chron.values)==False] # to avoid duplicates due to nans
        try:
            forward_proxy[:,i][ens_chron.notnull()]=xr.merge([ens_chron_red,sim_data],join="left")[varname].values # merge excluding nans
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
                sim_ind=np.where((sim_data.year==d)==True)
                forward_proxy[np.where(ens_chron==d),i]=sim_data[sim_ind].values 
                ens_chron_copy[np.where(ens_chron==d)]=np.nan # set duplicates to nans in ens_chron_copy
            forward_proxy[:,i][np.isnan(ens_chron_copy)==False]=xr.merge([ens_chron_unique,sim_data],join="left")[varname].values # merge excluding duplicates


    
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
    resamples the sim_data based on the attributes of the target object
    """
    # sort time axis of the simulation data (that resampling works)
    sim_data = sim_data.sortby("time")
    
    # 
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
                month_i = target.month_i #problem here?
            # resampling, not weighted (length of month?)
            #resampled = sim_data.resample(time='QE', closed='left', label='left').mean(dim='time')
            # chose by start month index of season
            #resampled = resampled.sel(time=resampled['time.month'] == month_i)
            # convert time axis to years as integers
           # resampled = resampled.groupby("time.year").mean("time")
            
            # resampling, not weighted (length of month?) / calendar adjustment (anderer operator) #this should be added
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
    Helper function to round and cut chron data.
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

def sampfunc_slice2point(i, forward_proxy, ens_chron, ens_chron_d,
                         ens_chron_red, sim_data_rand,
                         sampling, sampling_size, quiet):
    """
    Helper function which performs year to slice sampling between a chronology and a simulated variable.
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

def sampfunc_point2point(i, forward_proxy, ens_chron,
                         ens_chron_red, sim_data_rand, quiet):
    """
    Helper function which performs year to year sampling between a chronology and a simulated variable.
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

def create_bounds_adjacent(ens_chron, ens_chron_red, sim_data_rand, quiet):
    
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

def create_bounds_distant(ens_chron, ens_chron_d, ens_chron_red, sim_data_rand, sampling_size, quiet):
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
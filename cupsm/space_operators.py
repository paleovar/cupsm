"""
- space operator "field2site"
- space operator helpers
    - function "dx_dy_in_meter"
"""
from .utilities import *
import numpy as np
import xarray as xr
from geopy.distance import great_circle

# ~~~~~~~~~~~~~~~~~~~~~~
# Space operator 
# ~~~~~~~~~~~~~~~~~~~~~~
def field2site(field, site_location, method="dist", radius_km=500, plot_mask=False):
    """
    Interpolates the data in field to the given site location. Returns an xarray DataArray.

    Note:
    ----
    --> Usually lipd files report the coordinate longitude between -180°E and +180°E, 
    so the longitude coordinate of the field may be transformed accordingly.
    --> To avoid artefacts, grid cells that are nan (empty/undefined) at any point 
    on the time axis are ignored for the entire calculation. 
    
    Parameters:
    ----------
    "field"         : xarray DataArray of simulation data of interest
    "site_location" : (x,y) tuple with longitude (x) and latitude (y) of the site location
    "method"        : Method for interpolation; available keywords "dist" (distance weighted
                      mean over grid cells which are within radius) and "nn" (nearest grid cell
                      which is not nan). Default is "dist".
    "radius_km"     : Radius in km within which grid cells centers should be considered. 
                      The default is radius_km=500.
    "plot_mask"     : Bool, optional diagnostic plot of the weighting mask. Default is False.
    """
    if method not in ["dist", "nn"]:
        raise ValueError(f"Method {method} is not available.")

    if not set(["lon","lat"]).issubset(set(field.dims)):
        raise ValueError(f"The dimensions longitude and latitude must be named 'lon' and 'lat', the coordinates of the field are {field.coords}.")
    
    x,y = site_location
    field = do_to_180(field) # set longitude axis to -180, 180 as it standard in lipd
    lon = field.coords["lon"]
    lat = field.coords["lat"]
    radius_m = radius_km*1e3 # radius in meters

    # create a copy of field with nans in it (for weighting)
    mask = xr.full_like(field, fill_value=np.nan)
    if "time" in mask.dims:
        mask = mask.mean("time")
    
    # identify closest grid cell indices
    ind_lon1 = np.fabs((lon - x)).argsort(axis=-1)[:1].values
    ind_lat1 = np.fabs((lat - y)).argsort(axis=-1)[:1].values
    
    # find the grid resolution
    dx_arr, dy = dx_dy_in_meter(lon, lat)
    dx = dx_arr[ind_lat1].values[0]
    
    # stepsize for identifying relevant gridcells
    step_lon = int(np.ceil(radius_m / dx))
    step_lat = int(np.ceil(radius_m / dy))

    # relevant gridcell boundaries
    lon_min, lon_max = lon[ind_lon1-step_lon].values[0], lon[ind_lon1+step_lon].values[0]
    lat_min, lat_max = lat[ind_lat1-step_lat].values[0], lat[ind_lat1+step_lat].values[0]

    # floor/ceil to two digits
    lon_min, lon_max = np.floor(lon_min * 100)/100, np.ceil(lon_max * 100)/100
    lat_min, lat_max = np.floor(lat_min * 100)/100, np.ceil(lat_max * 100)/100
    
    # compute relevant slice for nan check 
    selected_field = field.sel(lon=slice(lon_min, lon_max), lat=slice(lat_min, lat_max))
    computed = selected_field.compute()

    # create nan mask
    nan_mask = np.isnan(computed).sum("time") #True >= 1, False == 0
    nan_mask = xr.where(nan_mask != 0, 0, 1) #set values to 0 where there was nan, else 1

    # check whether mask is only zeros (only nan):
    if np.all(nan_mask == np.zeros_like(nan_mask)):
        raise ValueError(f"For the chosen proxy location at {x}°E and {y}°N and a radius of {radius} km, the given field does not provide values.")
    
    w_distance_sum = 0 # sum up all distances to normalize in the end to 1
    
    all_nan_check=True # boolean to check for NANs after iterations through lats and lons 
    
    # iterate through relevant lats and lons and write values into mask
    for r_lon in nan_mask.lon.values:
        for r_lat in nan_mask.lat.values:
            # NAN check
            if nan_mask.sel(lon=r_lon, lat=r_lat, method="nearest").values == 0:
                continue
            else:
                # max_dist - distance for weighting
                dist = great_circle((r_lat, r_lon), (y,x)).m
                w_dist = radius_m - dist
                if w_dist < 0:
                    continue
                # add to sum
                w_distance_sum += w_dist
                # write distance into map 
                mask.loc[dict(lon=r_lon, lat=r_lat)] = w_dist
                all_nan_check=False 
                
    if all_nan_check==True:
        raise ValueError(f"For the chosen proxy location at {x}°E and {y}°N and a radius of {radius_km} km, the given field does not provide values.") 


    mask = (mask/w_distance_sum) # normalize to 1

    if plot_mask:
        # plot a diagnostic plot of the weighting mask
        mask.rename("weighting [0-1]").plot()

    if method == "dist":
        selected_mask = mask.sel(lon=slice(lon_min, lon_max), lat=slice(lat_min, lat_max))
        w = np.cos(np.deg2rad(lat)).sel(lat=slice(lat_min, lat_max)) # cosinus weighted
        field_at_loc = xr.DataArray(data=(selected_field * selected_mask).weighted(w).sum(('lon', 'lat')),
                                  attrs=field.attrs)
    elif method == "nn":
        # chose from weighted mask
        mask_argmax = mask.argmax(["lon", "lat"])
        nn_lon_ind = int(mask_argmax["lon"])
        nn_lat_ind = int(mask_argmax["lat"])
        field_at_loc = xr.DataArray(data=(field.isel(lon=nn_lon_ind, lat=nn_lat_ind)),
                                  attrs=field.attrs)
        
    try:
        field_at_loc.attrs = {"lon": x,
                              "lat" : y,
                              "units" : field.attrs["units"]
                              }
    except KeyError:
        field_at_loc.attrs = {"lon": x,
                              "lat" : y,}
        
    return field_at_loc
    
# ~~~~~~~~~~~~~~~~~~~~~~
# Helper functions
# ~~~~~~~~~~~~~~~~~~~~~~
def dx_dy_in_meter(arr_x, arr_y):
    """
    Returns the grid length elements dx and dy in meters for the given 
    longitudes (x) and latitudes (y).
    
    Parameters
    ----------
    arr_x : array of longitudes
    arr_y : array of latitudes
    """
    # determine lon and lat resolution in the data
    values, counts = np.unique(np.diff(arr_x), return_counts=True)
    lon_res = values[np.argmax(counts)]
    #check if grid is regular
    if not np.allclose(values, np.full(shape=len(values), fill_value=lon_res)):
        raise ValueError("The grid is not regular, please provide dx and dy manually.")
    
    values, counts = np.unique(np.diff(arr_y), return_counts=True)
    lat_res = values[np.argmax(counts)]
    #check if grid is regular
    if not np.allclose(values, np.full(shape=len(values), fill_value=lat_res)):
        raise ValueError("The grid is not regular, please provide dx and dy manually.")
        
    #how many meters are 1° lon (latitude dependent)?
    d_1deg_lon = np.cos(np.deg2rad(arr_y))*2*np.pi*6371e3/360
    dx = d_1deg_lon * lon_res

    # how many meters are 1° lat?
    d_1deg_lat = np.pi*6371e3/180
    dy = d_1deg_lat * lat_res

    return dx, dy

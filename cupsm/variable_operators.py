"""
This module contains code for variable transformations. The noise operators "white_noise" and "ar1_noise" perturb variables in sim_data or obs_data with either white or auto-correlated noise to imitate uncertainties from the proxy-climate relationship and archival processes.
It contains:
- white noise operator "white_noise"
- AR1 noise operator "ar1_noise"
"""
from .utilities import *
import numpy as np
import xarray as xr
import pandas as pd

#~~~~~~~~~~~~~~~~~~~~~~~~
# White noise operator
#~~~~~~~~~~~~~~~~~~~~~~~~

def white_noise(sim_data,num_ensemble,mu=0,sigma=1):    
    """
    Creates white noise by randomly drawing values from a normal (Gaussian)
    distribution for each location and point in time of the input data. 
    Adds this white noise to the input data and saves the result as a new
    (white noise) ensemble member. 
    
    Multiple new ensemble members can be created. The original input
    data is kept as the first ensemble member. 
    
    The result is returned as a xarray Dataarray.
    
    Parameters:
    ----------
    sim_data       : xarray Datarray; (simulation) input data
    num_ensemble   : integer; number of additional white noise ensemble members to be created.
    mu             : float; mean of the normal distribution. Default is mu=0.
    sigma          : float; standard deviation of the normal distribution. Default is sigma=1.

    """
    # Check if dimension "ensemble_member" already exists in sim data
    if "ensemble_member" in sim_data.coords:
        raise Exception("Trying to create new dimension named 'ensemble member', but dimension 'ensemble member' already exists.")
    # Create new ensemble members
    else:
        sim_data_wn=xr.concat((num_ensemble+1)*[sim_data],dim=pd.RangeIndex(0,num_ensemble+1,1,name="ensemble_member"))
        for i in range(0,num_ensemble):
            sample= np.random.normal(mu, sigma, size=sim_data.shape) # create white noise
            sim_data_wn[i+1]=sim_data_wn[i+1]+sample # add white noise to sim data
    return sim_data_wn  # return sim data (original as first ensemble member) + new white noise ensemble members
#~~~~~~~~~~~~~~~~~~~~~~~~
# AR1 noise operator
#~~~~~~~~~~~~~~~~~~~~~~~~
def ar1_noise (sim_data,num_ensemble,rho,sigma,quiet=False):
    """
    Creates first order auto-regressive (AR1) noise for each
    location of the input data following Y(t)=rho*Y(t-1)+e(t)
    with time-step t, magnitude rho and error term e(t).
    e(t) is drawn randomly from a normal distribution with
    mean=0 and  standard deviation=sigma*sqrt(1-rho^2). 
    Y(0) is drawn randomly from an uniform distribution over
    [0,1). The number of time-steps is given by the length of
    the input data time axis. 
    
    Adds this AR1 noise to the input data and saves the result
    as a new (AR1 noise) ensemble member. Multiple new ensemble 
    members can be created. The original input data is kept as 
    the first ensemble member. 
    
    The result is returned as a xarray Dataarray.

    Parameters:
    ----------
    sim_data      : xarray Datarray; (simulation) input data
    num_ensemble  : integer; number of additional AR1 noise ensemble members to be created.
    rho           : float; noise magnitude
    sigma         : float; standard deviation of Y(t)
    quiet         : boolean; if True surpresses warning for non-stationary process. Default is False.

    """
    
    # create new ensemble member dimension
    if "ensemble_member" in sim_data.coords:
        raise Exception("Trying to create new dimension named 'ensemble member', but dimension 'ensemble member' already exists.")
    else:
        sim_data_ar1=xr.concat((num_ensemble+1)*[sim_data],dim=pd.RangeIndex(0,num_ensemble+1,1,name="ensemble_member"))
    
    # parameters of AR1 process    
    if abs(rho) >= 1:
        if not quiet:
            print('Warning: The magnitude of rho is greater/equal to 1. The process is non-stationary.')      
    if sigma <= 0:
         raise ValueError('The standard deviation must be positive.')  
    n=int(len(sim_data.time))
    if n<1:
        raise ValueError('The number of (time) steps must be at least one.')
    # Generate AR1 noise
    s = sigma * np.sqrt(1 - rho**2)
    for i in range(0,num_ensemble):
        v = np.random.normal(0, s, size=sim_data.shape)
        v[0] = np.random.rand()
        for t in range(1, n):
            v[t] += rho * v[t - 1]
        # add generated AR1 noise to sim_data
        sim_data_ar1[i+1]=sim_data_ar1[i+1]+v

    return sim_data_ar1
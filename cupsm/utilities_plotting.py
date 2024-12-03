"""
Muriels plotting utilities
"""
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.colors import Normalize
from matplotlib.colors import LinearSegmentedColormap
import cartopy
import cartopy.crs as ccrs
from cartopy.util import add_cyclic_point


# ~~~~~~~~~~~~~~~~~~~~~~
# Functions
# ~~~~~~~~~~~~~~~~~~~~~~

def plot_feature(lsm, ax, key = "land", alpha=0.3):
    if key == "land":
        cmap = "Oranges_r"
        lsm = xr.where(lsm < 0.5, np.nan, 1)
    
    elif key == "land_lsg":
        cmap = "Oranges_r"
        lsm = xr.where(lsm > 0.5, np.nan, 1)
    else:
        cmap = "Blues_r"
        lsm = xr.where(lsm > 0.5, np.nan, 1)
    cyc_data, cyc_lon = add_cyclic_point(lsm, coord=(lsm.lon))
    c = ax.pcolormesh(cyc_lon,lsm.lat,cyc_data, transform=ccrs.PlateCarree(), 
                   alpha=alpha, cmap=cmap)
    

def plot_lsm(lsm, ax):
    cyc_data, cyc_lon = add_cyclic_point(lsm, coord=(lsm.lon))
    c = ax.contour(cyc_lon,lsm.lat,cyc_data, levels=[0.5], transform=ccrs.PlateCarree(), 
                   colors = "black", linewidths=0.8)
    
def plot_grid(ax, des_lons = np.arange(-180,210,30), des_lats = np.arange(-90,90,30), labs=False):
    gl = ax.gridlines(draw_labels=labs, linewidth=0.5, color='gray', linestyle='--')#, alpha=0.5)
    gl.xlocator = plt.FixedLocator(des_lons)
    gl.ylocator = plt.FixedLocator(des_lats)
    return gl

# ~~~~~~~~~~~~~~~~~~~~~~
# Colormaps
# ~~~~~~~~~~~~~~~~~~~~~~

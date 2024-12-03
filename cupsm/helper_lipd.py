"""
- site overview table
- get records
- lipd package contact
"""
from .utilities import *
from .site_object import lipd_site
import os
import numpy as np
import pandas as pd
import lipd

# for suppressing lipd package output
# https://stackoverflow.com/questions/2828953/silence-the-stdout-of-a-function-in-python-without-trashing-sys-stdout-and-resto/40054132#40054132
import sys, traceback
class Suppressor():
    def __enter__(self):
        self.stdout = sys.stdout
        sys.stdout = self
    def __exit__(self, exception_type, value, traceback):
        sys.stdout = self.stdout
        if exception_type is not None:
            # Do normal exception handling
            raise Exception(f"Got exception: {exception_type} {value} {traceback}")
    def write(self, x): pass
    def flush(self): pass
# ~~~~~~~~~~~~~~~~~~~~~~
# Proxy data directory handling
# ~~~~~~~~~~~~~~~~~~~~~~

def empty_lipd_dict(path, cols=None, returnN=False):
    """
    creates an empty array
    
    path: path to lipd files
    cols: user defined wanted columns of the resulting table
    """
    # number of lipd files in path
    N = len([file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file)) and ".lpd" in file])
    # array full of nan
    a = [np.nan for i in range(N)]
    # empty dictionary
    empty = {}
    if cols is None:
        cols = ["filename", "archive", "lon", "lat", "elevation", "age_min", "age_max"]
    for col in cols:
        empty[col] = a
    if returnN:
        return empty, N
    else:
        return empty

def create_proxy_info(file_path, save_path=None, update=False, 
                      class_creator=lipd_site, filename=".proxy_meta_data.pkl"):
    """
    creates / loads the proxy information table
    """
    if save_path is None:
        save_path = file_path
    # check if file is already present:
    if os.path.isfile(save_path+filename) and update == False:
        print(f"The {filename} file is already present in {save_path} and is returned.")
        print("For updating the file, run with the update=True")
        return pd.read_pickle(save_path+filename) 
    #-----
    else:
        # create the colums for the dummy array with an example file
        example_file = [f for f in os.listdir(file_path) if ".lpd" in f][0]
        with Suppressor():
            example_object = class_creator(lipd.readLipd(file_path+example_file), path=file_path,
                                           filename=example_file)
        cols = list(example_object.info(meta=True).keys())

        # create the empty dataframe and list for indexing
        empty, N = empty_lipd_dict(file_path, cols=cols, returnN=True)
        index = [np.nan for i in range(N)]
        # transform into dataframe
        empty_df = pd.DataFrame(empty)
        # iterate trhough lipd files
        for ind,file in enumerate(os.listdir(file_path)): 
            if not ".lpd" in file: continue
            # create class object
            with Suppressor():
                object = class_creator(lipd.readLipd(file_path+file), path=file_path,filename=file)
            index[ind] = object.sitename
            proxy_dict = object.info(meta=True)
            empty_df.update(pd.DataFrame(proxy_dict, index=[ind]))
        
        empty_df.index = index
        # write to file
        empty_df.to_pickle(save_path+filename)
        return empty_df


def get_records_df(df, filename=None, sitename=None, location=None, loc_radius=None,
                desired_data=None, class_creator=lipd_site):
    """
    Searches filepath for lipd files and returns class objects for records 
    depending on the given arguments:

    df:             Dataframe with an overview over all proxies, created by create_proxy_info function
    
    filename:       Name of the lipd file, path not required (e.g. "XXXX.lipd")
    sitename:       Name of the proxy record site (e.g. "MD88_770")
                    -> detects only first hit
    location:       A list or tuple for the proxy record location, given in degrees East
                    and North and meters ignoring decimals, (e.g. "[96, -46, -3290]")
                    put True if all values should be taken
                    If only two values are provided, elevation is ommitted
                    -> searches all files at filepath
    loc_radius:     will search for records in the interval location +/- loc_radius
                    for the provided list in the format [lon, lat, m]
    min_age:        Minumum age that should be covered (min_age < max_age; e.g. 12 ka < 19 ka)
    max_age:        Maximum age that should be covered (min_age < max_age; e.g. 12 ka < 19 ka)
    desired_data:   desired variable as str, must match definition in lipd file, 
                    e.g. 'surface.temp'
                    -> searches all files at filepath
    class_creator:  class creation class to be used

    If no arguments are given or several files fulfill the given requirements, 
    a list of class objects is returned.
    """
    def check_read_return(fpath,fname):
        if os.path.isfile(fpath+fname):
            with Suppressor():
                proxy_object = class_creator(lipd.readLipd(fpath+fname), path=fpath, filename=fname)
            return proxy_object
        else:
            raise FileNotFoundError(f"The provided file {fname} is not found at {fpath}. Is your proxy info table up-to-date?")

    def collect_check_return_list(files, paths):
        # collect and load files
        record_object_list = []
        for i,file in enumerate(files):
            if os.path.isfile(paths[i]+file):
                with Suppressor():
                    temp_lipd=lipd.readLipd(paths[i]+file)
                record_object_list.append(class_creator(temp_lipd, path=paths[i], filename=file))
            else:
                raise FileNotFoundError(f"The provided file {fname} is not found at {fpath}. Is your proxy info table up-to-date?")
        print(f"I return a list of {len(record_object_list)} record objects at {location} +/- {loc_radius}.")
        return record_object_list

    # list functions for location / datasets
    def mask_location_return(loc, loc_r):
        if len(location) == 3:
            lon, lat, depth = loc
        elif len(location) == 2:
            lon, lat = loc
            depth = True
        else:
            raise ValueError("Please provide lon, lat, (depth) as tuple, list or array.")
        if loc_r is not None:
            if len(loc_radius) == 3:
                dx, dy, dz = loc_r
            elif len(location) == 2:
                dx, dy = loc_r
                dz = np.infty
            else:
                raise ValueError("Please provide the loc radius as tuple, list or array.")
        else:
            dx, dy, dz = 0,0,0
        # create conditions for mask if not set to True
        # lon
        if lon == True:
            con_lon = (df["lon"] != True )
        else:
            con_lon = (np.abs(df["lon"]-lon) <= dx)
        # lat
        if lat == True:
            con_lat = (df["lat"] != True )
        else:
            con_lat = (np.abs(df["lat"]-lat) <= dy)
        # elevation
        if depth == True:
            con_depth = (df["elevation"] != True )
        else:
            con_depth = (np.abs(df["elevation"]-depth) <= dz)
        # combine
        mask = con_lon & con_lat & con_depth
        files = df.loc[mask]["file"].values
        paths = df.loc[mask]["path"].values
        return files, paths

    
    # -----------------------------------------
    # if filename provided (assumed as unique)
    if filename is not None:
        if ".lpd" not in filename:
            filename=filename+".lpd"
        if filename in df["file"].values:
            filepath = df.loc[df["file"] == filename]["path"].values[0]
            return check_read_return(filepath, filename)
        else:
            raise FileNotFoundError(f"The provided file is not found")
    
    # if sitename is provided (assumed as unique)
    elif sitename is not None:
        if sitename in df.index:
            filename = df.loc[sitename]["file"]
            filepath = df.loc[sitename]["path"]
            return check_read_return(filepath, filename)
        else:
            raise KeyError(f"The sitename {sitename} is not available. Please check the indeces of your dataframe.")
        
    # if location is provided
    elif location is not None:
        # select files which have the correct location
        loc_files, loc_paths = mask_location_return(loc=location,loc_r=loc_radius)
        if desired_data is None:
            print(desired_data)
            return collect_check_return_list(loc_files, loc_paths)
        else:
            # select files which have the required data:
            if desired_data not in list(df.keys()):
                raise KeyError("The desired data set is not available. Please check the keys of your dataframe.") 
            dd_files = df.loc[df[desired_data]==True]["file"].values
            dd_paths = df.loc[df[desired_data]==True]["path"].values
            files, loc_ind, dd_ind = np.intersect1d(loc_files, dd_files, return_indices=True)
            return collect_check_return_list(files, loc_paths[loc_ind])

    # if desired_data is provided 
    elif desired_data is not None:
        if desired_data not in list(df.keys()):
            raise KeyError("The desired data set is not available. Please check the keys of your dataframe.") 
        dd_files = df.loc[df[desired_data]==True]["file"].values
        dd_paths = df.loc[df[desired_data]==True]["path"].values
        return collect_check_return_list(dd_files, dd_paths)
    else:
        raise TypeError(f"ABORTED: No record information provided.")

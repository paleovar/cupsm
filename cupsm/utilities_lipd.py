"""
This module contains helper routines to handle a database of LiPD files. 
It contains:
- obs_data object creator function "get_records_df" 
- Overview table creator function "create_proxy_info"
"""
# Further helper functions and classes (excluded from ReadTheDocs documentation)
# - Table creator function helper
#    - function "empty_lipd_dict"
#    - class "Suppressor" 

# Imports
from .utilities import *
from .site_object import lipd2object
import os
import numpy as np
import pandas as pd
import lipd
import sys, traceback

# ~~~~~~~~~~~~~~~~~~~~~~
# LiPD file handling
# ~~~~~~~~~~~~~~~~~~~~~~

def get_records_df(df, file_name=None, site_name=None, location=None, loc_radius=None,
                desired_data=None, return_as="list"):
    """
    Based on the overview table (created by the function create_proxy_info) lipd proxy record files are selected, parsed,
    and returned as class objects "site_object". If multiple files match the selection criteria, a list or a dictionary can be returned "obs_data".

    Parameters:
    ----------
    df:             pandas DataFrame; the proxy overview table created by the function create_proxy_info
    file_name:      string or list of strings; name(s) of the lipd file without directory path (e.g. "XXXX.lipd")
    site_name       string or list of strings; name(s) of the proxy record site (e.g. "MD88_770")
                    -> detects only first hit
    location:       A list or tuple for the desired proxy record location, given in degrees East and North and meters ignoring 
                    decimals, (e.g. "[96, -46, -3290]"). Set to True if all values should be taken for that dimension.
                    If only two values are provided, elevation is ommitted
                    -> searches all files listed in the overview table
    loc_radius:     A list or tuple which determines a location interval --> location +/- loc_radius, given in the format [lon, lat, m]
                    in degrees East and North and meters
    desired_data:   Desired variable as str, must match definition in lipd file, e.g. 'surface.temp'
                    -> searches all files listed in the overview table
    return_as:      string, either "list" or "dictionary". Determines whether an unsorted list of proxy record objects are returned or
                    as dictionary with record names as keys for the respective objects.
    """

    # --------------------
    # Internal Helpers
    # --------------------
    
    def check_read_return_single(fpath,fname):
        if os.path.isfile(fpath+fname):
            with Suppressor():
                proxy_object = class_creator(lipd.readLipd(fpath+fname), path=fpath, file_name=fname)
            return proxy_object
        else:
            raise FileNotFoundError(f"The provided file {fname} is not found at {fpath}. Is your proxy info table up-to-date?")

    def collect_check_return(files, paths):
        # collect and load files
        record_object_list = []
        for i,file in enumerate(files):
            if os.path.isfile(paths[i]+file):
                with Suppressor():
                    temp_lipd=lipd.readLipd(paths[i]+file)
                record_object_list.append(class_creator(temp_lipd, path=paths[i], filename=file))
            else:
                raise FileNotFoundError(f"The provided file {fname} is not found at {fpath}. Is your proxy info table up-to-date?")
        print(f"I return a {return_as} with {len(record_object_list)} record objects at {location} +/- {loc_radius}.")
        
        # return as list or dictionary:
        if return_as == "list":
            return record_object_list
        
        else:
            #return dictionary with record name as key
            record_object_dict = {}
            for object in record_object_list:
                record_object_dict[object.site_name] = object
            return record_object_dict
            

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
    
    # -----
    # Main
    # -----

    if return_as not in ["list", "dictionary"]:
        raise ValueError("The keyword parameter return_as must be string, either 'list' or 'dictionary'.")
    
    class_creator=lipd2object
    
    #if file name provided (assumed as unique)
    if file_name is not None:
        if isinstance(file_name, str):
            # file_name is string, only one file
            if not file_name.endswith(".lpd"):
                file_name=file_name+".lpd"
            if file_name in df["file"].values:
                filepath = df.loc[df["file"] == file_name]["path"].values[0]
                return check_read_return_single(filepath, file_name)
            else:
                raise FileNotFoundError(f"The provided file is not found. Please provide without directory path.")
        
        elif isinstance(file_name, list) and all(isinstance(item, str) for item in file_name):
            # file_name is list of strings, several files
            # make sure all files end with .lpd
            file_name = [name if name.endswith(".lpd") else f"{name}.lpd" for name in file_name]
            # find files and path
            matches = df[df['file'].isin(file_name)]
            fname_files = matches["file"].tolist()
            fname_paths = matches["path"].tolist()
            # return
            return collect_check_return(fname_files, fname_paths)
        else:
            # handle other cases
            raise ValueError("The parameter 'file_name' must either be a string or a list of strings.")
    
    # if site name is provided (assumed as unique)
    if site_name is not None:
        if isinstance(site_name, str):
            # site_name is string, only one file
            if site_name in df.index:
                file_name = df.loc[site_name]["file"]
                filepath = df.loc[site_name]["path"]
                return check_read_return_single(filepath, file_name)
            else:
                raise KeyError(f"The site name {site_name} is not available. Please check the indeces of your dataframe.")
        
        elif isinstance(site_name, list) and all(isinstance(item, str) for item in site_name):
            # site_name is list of strings, several files
            # find files and path
            matches = df[df.index.isin(site_name)]
            sname_files = matches["file"].tolist()
            sname_paths = matches["path"].tolist()
            # return
            return collect_check_return(sname_files, sname_paths)
        else:
            # handle other cases
            raise ValueError("The parameter 'site_name' must either be a string or a list of strings.")
    
    elif site_name is not None:
        if site_name in df.index:
            file_name = df.loc[site_name]["file"]
            filepath = df.loc[site_name]["path"]
            return check_read_return_single(filepath, file_name)
        else:
            raise KeyError(f"The site name {site_name} is not available. Please check the indeces of your dataframe.")
        
    # if location is provided
    elif location is not None:
        # select files which have the correct location
        loc_files, loc_paths = mask_location_return(loc=location,loc_r=loc_radius)
        if desired_data is None:
            return collect_check_return(loc_files, loc_paths)
        else:
            # select files which have the required data:
            if desired_data not in list(df.keys()):
                raise KeyError("The desired data set is not available. Please check the keys of your dataframe.") 
            dd_files = df.loc[df[desired_data]==True]["file"].values
            dd_paths = df.loc[df[desired_data]==True]["path"].values
            files, loc_ind, dd_ind = np.intersect1d(loc_files, dd_files, return_indices=True)
            return collect_check_return(files, loc_paths[loc_ind])

    # if desired_data is provided 
    elif desired_data is not None:
        if desired_data not in list(df.keys()):
            raise KeyError("The desired data set is not available. Please check the keys of your dataframe.") 
        dd_files = df.loc[df[desired_data]==True]["file"].values
        dd_paths = df.loc[df[desired_data]==True]["path"].values
        return collect_check_return(dd_files, dd_paths)
    else:
        raise TypeError(f"ABORTED: No record information provided.")

def create_proxy_info(database_path, save_path=None, file_name=".proxy_meta_data.pkl", update=False):
    """
    Creates or loads the proxy information table for a given database path. If the overview table is already present, 
    it is only loaded.

    Parameters:
    ----------
    database_path:   string; path to directory where LiPD files are.
    save_path:       string; path where the overview table should be stored. Default is database_path.
    file_name:       string; file name of the overview table
    update:          boolean; default is False (load table if it already exists), if True the overview table is 
                     recreated for given paths
    """
    # set class creator
    class_creator=lipd2object

    # check file name
    if not file_name.endswith(".pkl"):
        raise ValueError("The overview table is saved in as pickle file. The file name must end with '.pkl'.")
        
    # set default save_path
    if save_path is None:
        save_path = database_path
    
    # check if file is already present:
    if os.path.isfile(save_path+file_name) and update == False:
        print(f"The {file_name} file is already present in {save_path} and is returned.")
        print("For updating the file, run with the update=True")
        return pd.read_pickle(save_path+file_name) 
    #-----
    else:
        # create the colums for the dummy array with an example file
        example_file = [f for f in os.listdir(database_path) if ".lpd" in f][0]
        with Suppressor():
            example_object = class_creator(lipd.readLipd(database_path+example_file), path=database_path,
                                           file_name=example_file)
        cols = list(example_object.info(meta=True).keys())

        # create the empty dataframe and list for indexing
        empty, N = empty_lipd_dict(database_path, cols=cols, returnN=True)
        index = [np.nan for i in range(N)]
        # transform into dataframe
        empty_df = pd.DataFrame(empty)
        # iterate trhough lipd files
        for ind,file in enumerate(os.listdir(database_path)): 
            if not ".lpd" in file: continue
            # create class object
            with Suppressor():
                object = class_creator(lipd.readLipd(database_path+file), path=database_path,file_name=file)
            index[ind] = object.site_name
            proxy_dict = object.info(meta=True)
            empty_df.update(pd.DataFrame(proxy_dict, index=[ind]))
        
        empty_df.index = index
        # write to file
        empty_df.to_pickle(save_path+file_name)
        return empty_df

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Helper functions and classes
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def _empty_lipd_dict(database_path, cols=None, returnN=False):
    """
    Creates an empty dictionary as base for the the proxy overview table.

    Parameters:
    ----------
    database_path:   string; path to directory where LiPD files are.
    cols:            list of strings; Column names of the resulting table, 
                     default are ["file_name", "archive", "lon", "lat", "elevation", "age_min", "age_max"]
    returnN:         boolean; default is False, if True, the number of LiPD files found is returned as well
    """
    # number of lipd files in path
    N = len([file for file in os.listdir(path) if os.path.isfile(os.path.join(path, file)) and ".lpd" in file])
    # array full of nan
    a = [np.nan for i in range(N)]
    # empty dictionary
    empty = {}
    if cols is None:
        cols = ["file_name", "archive", "lon", "lat", "elevation", "age_min", "age_max"]
    for col in cols:
        empty[col] = a
    if returnN:
        return empty, N
    else:
        return empty

class _Suppressor():
    """
    Suppresses standard output from a python function. Used to suppress the output from the lipd package.
    Based on:
    https://stackoverflow.com/questions/2828953/silence-the-stdout-of-a-function-in-python-without-trashing-sys-stdout-and-resto/40054132#40054132
    """
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
"""
- lipd file to site object
"""
import numpy as np
import xarray as xr

# ~~~~~~~~~~~~~~~~~~~~~~
# Proxy class objects
# ~~~~~~~~~~~~~~~~~~~~~~

class lipd_site:
    """
    Creates a python class object from a loaded lipd file.
    """
    ### Initialization
    #-----------------
    
    def __init__(self, loaded_file, path=None, filename=None):
        # basic
        self.lipd=loaded_file
        if path != None:
            self.path=path
        if filename != None:
            self.fname=filename
        # from lipd file
        self.sitename=loaded_file['geo']['siteName']
        self.coords=loaded_file['geo']['geometry']['coordinates'] #lon, lat, depth
        self.archivetype=loaded_file['archiveType']
        self.av_ds=list(loaded_file['paleoData']['paleo0']['measurementTable']['paleo0measurement0']['columns'].keys())
        try:
            self.age=loaded_file['paleoData']['paleo0']['measurementTable']['paleo0measurement0']['columns']['age']['values']
        except KeyError:
            self.age=["unknown", "unknown"]
            
    ### Functions
    #-----------------
    
    def info(self, meta=False):
        """
        Prints basic information on the record 

        meta: returns a dictionary of meta info on the proxy
        """
        age=[x for x in self.age if "nan" not in str(x)]
        
        if not meta:
            print(f"""
{self.archivetype} record {self.sitename} ({age[0]:.2f} - {age[-1]:.2f} ka)
Position: lon={self.coords[0]}°E, lat={self.coords[1]}°N, elevation={self.coords[2]}m
available datasets:
{self.av_ds}
            """)
        
        else:
            # write values into new dict
            proxy_dict = {"path" : self.path,
                          "file" : self.fname,
                          "archive" : self.archivetype,
                          "lon" : self.coords[0],
                          "lat" : self.coords[1],
                          "elevation" : self.coords[2],
                          "age_min" : age[0],
                          "age_max" : age[-1],
            }

            if 'chronData' in self.lipd.keys():
                proxy_dict["agemodel"] = True

            ds_list = ['depth_merged', 'age', 'planktonic.d18O', 'planktonic.d18O-1', 'benthic.d18O', 
                       'benthic.d18O-1', 'benthic.d13C', 'surface.temp', 'age-1', 'benthic.MgCa', 'deep.temp',
                       'planktonic.MgCa', 'label', 'label-1', 'CaCO3', 'age-2', 'IRD', 'age-3', 'TOC',
                       'planktonic.d13C', 'DBD', 'BSi', 'planktonic.d13C-1', 'benthic.d13C-1', 'surface.temp-1',
                       'planktonic.MgCa-1', 'depth.top', 'depth.bottom', 'UK37', 'planktonic.foram.abundance',
                       'planktonic.foram.abundance-1', 'planktonic.foram.abundance-2',
                       'planktonic.foram.abundance-3', 'planktonic.foram.abundance-4', 
                       'planktonic.foram.abundance-5', 'planktonic.foram.abundance-6', 
                       'planktonic.foram.abundance-7','planktonic.foram.abundance-8', 
                       'planktonic.foram.abundance-9', 'planktonic.foram.abundance-10',
                       'planktonic.foram.abundance-99', 'age-4', 'surface.temp-2', 'planktonic.MgCa-2',
                       'surface.temp-3', 'surface.temp.error', 'surface.temp.error-1', 'note', 
                       'benthic.d18O-2', 'benthic.d13C-2','planktonic.d18O-2', 'note-1', 'depth', 
                       'depth-1', 'label-2', 'depth-2', 'label-3', 'depth-3', 'C37.concentration',
                       'benthic.d13C-3', 'benthic.d18O-3', 'benthic.d13C-4', 'benthic.d18O-4',
                       'benthic.d18O-5', 'benthic.d18O-6', 'benthic.d13C.error', 'benthic.d13C.error-1',
                       'benthic.d18O.error', 'benthic.d18O.error-1', 'planktonic.d18O.error',
                       'planktonic.d13C.error', 'surface.temp-4', 'TOC-1', 'benthic.MgCa-1', 'depth-4', 
                       'depth-5', 'depth-6', 'depth-7', 'depth-8', 'depth-9', 'depth.top-1', 'planktonic.d13C-2',
                       'benthic.d13C-5', 'age-5', 'age-6', 'planktonic.d18O-3', 'planktonic.d13C-3',
                       'planktonic.d18O-4', 'planktonic.d13C-4', 'planktonic.d18O-5', 'planktonic.d13C-5', 'age-7',
                       'planktonic.d18O-6', 'planktonic.d13C-6', 'TOC.error', 'C37.concentration-1', 
                       'surface.temp-5', 'surface.temp-6', 'IRD-1', 'planktonic.MgCa-3', 'UK37-1',
                       'depth_uncorrected']

            # add all as entries as False
            proxy_dict.update({item: False for item in ds_list})

            # add existing entries as True
            for ds in self.av_ds:
                proxy_dict[ds] = True
            
            return proxy_dict
                    


    def load_chron_data(self,):
        """
        #----
        Returns an xarray data array for the age model data
        #----
        """
        if 'chronData' in self.lipd.keys():
            
            # get to the data level in the lipd file
            data_dic = self.lipd['chronData']['chron0']['model']['chron0model0']['ensembleTable']['chron0model0ensemble0']['columns']
            
            #iterate through cols and collect data
            age_data_list = []
            ens_count=0
            for col in data_dic.keys():
                # depth data
                if col == 'depth':
                    depth_data=np.array(data_dic[col]['values'])
                    depth_unit=data_dic[col]['units']
                    depth_name=data_dic[col]['variableName']
                
                # age model data                
                elif "ens" in col:
                    data=np.array(data_dic[col]['values'], dtype=float)
                    age_data_list.append(data)
                    # check data attributes
                    if ens_count == 0:
                        data_unit=data_dic[col]['units']
                        data_name=data_dic[col]['variableName'].split("-")[0]
                        N = len(data)
                    else:
                        # check whether properties are unchanged
                        temp_data_unit=data_dic[col]['units']
                        temp_data_name=data_dic[col]['variableName'].split("-")[0]
                        temp_N = len(data)
                        if (temp_data_unit, temp_data_name, temp_N) != (data_unit, data_name, N):
                            raise AttributeError(f"""
                            Proxy record {self.sitename}: The attributes in the age model ensemble data are not consistent.
                            Please load the data manually""")
                    ens_count+=1 # increase counter
            
            # create xr data array
            age_model_data = np.stack(age_data_list).T
            xr_ds = xr.DataArray(
                data=age_model_data,
                dims=[depth_name, "ens"],
                coords={depth_name: depth_data,
                        "ens":np.arange(1,ens_count+1)},
                attrs= {"units": data_unit},
                name=data_name                      
            )
            xr_ds['depth'].attrs = {"units": depth_unit}
            
            return xr_ds.drop_duplicates(dim=depth_name)   
        else:
            raise KeyError(f"No age model data found in for proxy record from {self.sitename}.")
            
    def load_paleo_data(self, data_set, coord="depth", quiet=False, save_in_object=False):
        """
        #----
        Returns an xarray data set for the given data_set keyword.
        
        Either the coordinate depth or age are loaded.
        
        data_set: str, must be listed in self.av_ds.
                  For several datasets, put a list of strings
                  If you want all, put "all"

        coord:    str, either "depth" ("depth_merged") or "age" ("updated age model (median)")
        quiet:    do not print log messages
        #----
        """
        # Preparation
        print_naming_warning = False
        #---
        # Put desired keys in a list & check whether data is available
        if data_set == "all" or data_set == ["all"]:
            data_set=self.av_ds
        elif data_set in self.av_ds:
            data_set=[data_set]
        elif not set(data_set) <= set(self.av_ds):
            raise KeyError(f"The data set {data_set} is not available for proxy record from {self.sitename}.")
        
        # remove coordinate variable
        var_list=[]
        for i,entry in enumerate(data_set):
            if coord in entry:
                continue
            else:
                var_list.append(entry)

        # Check and prepare coordinates
        if coord not in ["depth", "age"]:
            raise KeyError(f"As coordinates, only 'depth' and 'age' are available.")
        if coord == "depth": coord_name = "depth_merged"
        else: coord_name = coord
            
        
        # get to the data level in the lipd file
        lipd_data_dic = self.lipd['paleoData']['paleo0']['measurementTable']['paleo0measurement0']['columns']
    
        # empty dics for data set creation
        data_dic = {}
        attrs_dic = {}
        
        # write variable data in:
        for i,var in enumerate(var_list):
            # get the variable name
            if "-" in var or "." in var:
                print_naming_warning=True      
            name = var.replace(".", "_").replace("-", "_") #underscore are replaced here
            # get the data
            try:
                data = np.array(lipd_data_dic[var]['values']).astype(float)
            except ValueError:
                data = np.array(lipd_data_dic[var]['values'])
            # local attributes
            local_attr = {}
            for attribute in lipd_data_dic[var].keys():
                if attribute in ["values", "number"]: continue
                elif attribute == "habitatSeason":
                    habitat_season = lipd_data_dic[var][attribute]
                    mapping = {"annual" : "annual",
                               "annual mean" : "annual",
                               "boreal winter" : "winter",
                               "winter" : "winter",
                               "summer" : "summer" }
                    if habitat_season not in mapping.keys():
                        print(f"The habitat season {habitat_season} is not known. Please update the code.")
                    else:
                        local_attr[attribute] = mapping[habitat_season]
                else:
                    local_attr[attribute] = lipd_data_dic[var][attribute]
#            if "units" in lipd_data_dic[var].keys():
#                local_attr["units"] = lipd_data_dic[var]['units']
#            if "hasDataLink" in lipd_data_dic[var].keys():
#                local_attr["data_link"] = lipd_data_dic[var]['hasDataLink']
#            if "description" in lipd_data_dic[var].keys():
#                local_attr["description"] = lipd_data_dic[var]['description']
#           if "hasPubDOI" in lipd_data_dic[var].keys() and 'doi' not in locals():
#                doi = lipd_data_dic[var]['hasPubDOI']
            # make entry in data dic
            data_dic[name]=([coord], data, local_attr)
        
        # print warning
        if not quiet and print_naming_warning:
            print(f"Variables were renamed to make them accessible via as xarray.dataset attributes, e.g. 'planktonic.d18O-1' --> 'planktonic_d18O_1' ")
        
        # global data set attributes
        attrs_dic["description"]=f"Measured paleo data from {self.sitename}."
        attrs_dic["note"]="Variables were renamed, e.g. 'planktonic.d18O-1' --> 'planktonic_d18O_1'"
        
        # create data set
        xr_ds = xr.Dataset(
            data_vars=data_dic,
            coords={coord:np.array(lipd_data_dic[coord_name]['values']).astype(float)},
            attrs=attrs_dic
        )

        # drop duplicates
        xr_ds=xr_ds.drop_duplicates(dim=coord)
        
        if save_in_object:
            try:
                self.data
            except AttributeError:
                self.data = xr_ds
            else:
                if isinstance(self.data, xr.Dataset):
                    merged = xr.merge([self.data, xr_ds], join="outer", compat='override')
                    self.data = merged
                    return
                else:
                    raise TypeError("Instance data is of unknown type.")
                
        return xr_ds

    def load(self, method="left", quiet=False):
        """"
        Loads the paleo/proxy data and the age model data of the proxy and combines them in one single
        xarray dataset. A common depth axis is chosen.

        method: how to merge the depth axes (default: left):
            - "left": age model depth axis is used (destructive)
            - "right": proxy data depth axis is used (destructive)
            - "inner": intersection of depth axes is used (destructive)
            - "outer": union of depth axes is used (non-destructive)
        """
        chron_data = self.load_chron_data()
        paleo_data = self.load_paleo_data("all", quiet=quiet)
        data = xr.merge([chron_data, paleo_data], join=method)
        return data
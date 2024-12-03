"""
utilities
"""
# define __all__ to allow clean import via wildcard *
__all__ = ['do_to_180']


# ~~~~~~~~~~~~~~~~~~~~~~
# MISC
# ~~~~~~~~~~~~~~~~~~~~~~
def do_to_180(dataobject, lon_name = 'lon', quiet=True):
    """
    Transforms the longitude coordinate from 0->360 to -180->+180 and 
    changes it in place. The dataobject is returned.
    
    dataobject: xarray dataobject, can be dataset or dataarray
    lon_name:   name of the longitude dimension, default='lon'
    """

    lon_values = dataobject[lon_name].values

    # Check if the longitudes are already in the [-180, 180] range
    if lon_values.min() >= -180 and lon_values.max() <= 180:
        #if not quiet: 
            #print('The longitudes were already in between -180°E and +180°E.')
        pass
    else:
        dataobject[lon_name] = xr.where(dataobject[lon_name] > 180, dataobject[lon_name] - 360, dataobject[lon_name])
        if not quiet: 
            print('The longitudes were transformed to -180°E --> +180°E.')

    # Sort the longitudes in ascending order
    dataobject = dataobject.sortby(lon_name)

    return dataobject

"""
The code of this module comprises helper routines.
"""
# define __all__ to allow clean import via wildcard *
__all__ = ['do_to_180']


# ~~~~~~~~~~~~~~~~~~~~~~
# MISC
# ~~~~~~~~~~~~~~~~~~~~~~
def do_to_180(dataobject, lon_name = 'lon', quiet=True):
    """
    Transforms the longitude coordinate from 0->360 to -180->+180 in place.
    
    dataobject:   Xarray data object, can be DataSet or DataArray
    lon_name:     string; name of the longitude dimension, default='lon'
    """

    lon_values = dataobject[lon_name].values

    # Check if the longitudes are already in the [-180, 180] range
    if lon_values.min() >= -180 and lon_values.max() <= 180:
        pass
    else:
        dataobject[lon_name] = xr.where(dataobject[lon_name] > 180, dataobject[lon_name] - 360, dataobject[lon_name])
        if not quiet: 
            print('The longitudes were transformed to -180°E --> +180°E.')

    # Sort the longitudes in ascending order
    dataobject = dataobject.sortby(lon_name)
    
    return dataobject

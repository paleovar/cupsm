# cupsm

cupsm is a python package for proxy system modeling that makes use of data cube structures.

## Introduction

To use the package, follow these steps

1. Clone the repository to a local directory of your choice:

   Using the terminal, navigate to the directory and type:

   ```git clone git@github.com:paleovar/cupsm.git```

    Alternatively, you can download the `cupsm-main.zip` file and extract it to your local directory.

2. Install the necessary packages

   cupsm depends on other Python packages. To run the package, you need numpy, xarray, pandas, geopy, lipd, dask, and numba. For plotting and regridding, we recommend matplotlib, cartopy, xesmf, and cdo.

   A ready-to-use environment [file](https://github.com/paleovar/cupsm/tree/main/tutorials/condaenv_python-3.11.7.yml) for recreating a working conda environment (using python 3.11.7) is available in the [tutorials](https://github.com/paleovar/cupsm/tree/main/tutorials) directory. To recreate an environment using conda, please make sure that conda is installed. Then type:

   ```conda env create -f tutorials/condaenv_python-3.11.7.yml```

   The default name of the environment is `cupsm_env_python-3.11.7`. If you want a different name, please change it in the environment file before creating the environment.

   Working now in python, make sure to activate the environment and install a jupyter kernel for the environment, if needed.

4. Add the package path to your PYTHONPATH variable

   Working now in an python environment, e.g. a jupyter notebook, you must add the package path to your PYTHONPATH variable. Change the path accordingly and run the following lines in python:

   ```
   import sys
   sys.path.append('path/to/cupsm')
   ```

   Now you are ready to import the package:

   ```import cupsm```

   If you want to add the package path permanently, please change your PYTHONPATH variable.

Fore more, check out the [documentation](https://cupsm.readthedocs.io/en/latest/) and [tutorials](https://github.com/paleovar/cupsm/tree/main/tutorials) with examples.


## Responsible for this repository:

Developers: *[Nils Weitzel](https://github.com/nilsweitzel), [Muriel Racky](https://github.com/mmrac) and [Laura Braschoss](https://github.com/LauraIB23)*

## Acknowledgements

This work has been funded by the German Research Foundation (NFDI4Earth, DFG project no. 460036893, [https://www.nfdi4earth.de/](https://www.nfdi4earth.de/)).

*The Authors, December 2024*

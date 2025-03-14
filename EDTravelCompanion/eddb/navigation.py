# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/eddb/81_navroute.ipynb.

# %% auto 0
__all__ = ['navigationroute']

# %% ../../nbs/eddb/81_navroute.ipynb 3
import os
import json

from logging import getLogger


# %% ../../nbs/eddb/81_navroute.ipynb 4
syslog = getLogger(f"root.{__name__}")
default_journaling_path = os.path.join(os.getenv("userprofile"), "Saved Games", "Elite Dangerous")

# %% ../../nbs/eddb/81_navroute.ipynb 5
def navigationroute(journalingpath:str=None):
    '''
        Iter over Navigation Route files

        Args:
            journalingpath (str): Path to directory with Elite Dangerous logfiles
    
    '''

    try:
        navfile = os.path.join(
            default_journaling_path if journalingpath is None else journalingpath, 
            "NavRoute.json"
        )

        syslog.debug("Reading Navigation Route from %s" % navfile)

        with open(navfile, "rt") as jsonfile:
            for item in json.load(jsonfile).get('Route'):
                yield item

    except KeyboardInterrupt as kbi:
        syslog.exception("Keyboard Interrupt", exc_info=True)
        


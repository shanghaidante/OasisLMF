# -*- coding: utf-8 -*-

__all__ = [
  'KeysLookup'
]

# Python 2 standard library imports
import csv
import io
import logging
import os

# Python 2 non-standard library imports

# Oasis utils and other Oasis imports

from oasislmf.keys.lookup import OasisBaseKeysLookup
from oasislmf.utils.log import oasis_log


class KeysLookup(OasisBaseKeysLookup):
    """
    Model-specific keys lookup logic.
    """

    @oasis_log()
    def __init__(self, keys_data_directory=None, supplier='{{cookiecutter.organization_slug}}', model_name='{{cookiecutter.model_identifier}}', model_version='{{cookiecutter.model_version}}'):
        """
        Initialise the static data required for the lookup.
        """
        super(self.__class__, self).__init__(
            keys_data_directory,
            supplier,
            model_name,
            model_version
        )
        pass
    
    
    @oasis_log()
    def process_locations(self, loc_df):
        """
        Process location rows - passed in as a pandas dataframe.
        """
        pass

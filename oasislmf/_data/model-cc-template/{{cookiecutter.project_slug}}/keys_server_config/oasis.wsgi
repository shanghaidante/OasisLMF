#!/usr/bin/python
import sys
import logging
sys.path.insert(0,"/var/www/oasis")

from oasislmf.keys.server import APP as application
application.secret_key = 'Add your secret key'

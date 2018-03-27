# -*- coding: utf-8 -*-

"""
Flask application for Oasis keys service.

Currently handles compressed/uncompressed POSTed data.
Processes the data sequentially - should be made multi-threaded.

"""
import csv
import gzip
import io
import json
import logging
import os

import pandas as pd

try:
    from flask import (
        Flask,
        request,
        Response,
    )
except ImportError:
    raise ImportError('Cannot import flask, try installing oasislmf[server].')

from ..utils.compress import compress_data
from ..utils.conf import load_ini_file, OasisLmfConf
from ..utils.exceptions import OasisException
from ..utils.http import (
    HTTP_REQUEST_CONTENT_TYPE_CSV,
    HTTP_REQUEST_CONTENT_TYPE_JSON,
    HTTP_RESPONSE_INTERNAL_SERVER_ERROR,
    HTTP_RESPONSE_OK,
    MIME_TYPE_JSON,
)
from ..utils.log import (
    oasis_log,
    read_log_config,
)

from .lookup import OasisKeysLookupFactory

# Module-level variables (globals)
APP = None
KEYS_SERVER_INI_FILE = None
MODEL_CONFIG = None
CONFIG_PARSER = None
logger = None
KEYS_DATA_DIRECTORY = None
MODEL_VERSION_FILE = None
SUPPLIER = None
MODEL_NAME = None
MODEL_VERSION = None
SERVICE_BASE_URL = None
keys_lookup = None
COMPRESS_RESPONSE = False


# App initialisation
@oasis_log()
def init():
    """
    App initialisation.
    """
    global APP
    global KEYS_SERVER_INI_FILE
    global CONFIG_PARSER
    global logger
    global COMPRESS_RESPONSE
    global KEYS_DATA_DIRECTORY
    global MODEL_VERSION_FILE
    global MODEL_NAME
    global MODEL_VERSION
    global SERVICE_BASE_URL
    global keys_lookup
    global MODEL_CONFIG

    # Get the Flask app
    APP = Flask(__name__)

    MODEL_CONFIG = OasisLmfConf()
    KEYS_SERVER_INI_FILE = MODEL_CONFIG.get('keys_server_ini_file', is_path=True)

    # Load INI file into config params dict
    CONFIG_PARSER = load_ini_file(KEYS_SERVER_INI_FILE)
    CONFIG_PARSER['LOG_FILE'] = CONFIG_PARSER['LOG_FILE'].replace('%LOG_DIRECTORY%', CONFIG_PARSER['LOG_DIRECTORY'])

    # Logging configuration
    read_log_config(CONFIG_PARSER)

    logger = logging.getLogger('Starting rotating log.')
    logger.info("Starting keys service.")

    # Get Gzip response and port settings
    COMPRESS_RESPONSE = bool(CONFIG_PARSER['COMPRESS_RESPONSE'])

    # Check that the keys data directory exists
    KEYS_DATA_DIRECTORY = MODEL_CONFIG.get('keys_data_path', is_path=True)
    if not os.path.isdir(KEYS_DATA_DIRECTORY):
        raise OasisException("Keys data directory not found: {}.".format(KEYS_DATA_DIRECTORY))
    logger.info('Keys data directory: {}'.format(KEYS_DATA_DIRECTORY))

    # Check the model version file exists
    MODEL_VERSION_FILE = os.path.join(KEYS_DATA_DIRECTORY, 'ModelVersion.csv')
    if not os.path.exists(MODEL_VERSION_FILE):
        raise OasisException("No model version file: {}.".format(MODEL_VERSION_FILE))

    with io.open(MODEL_VERSION_FILE, 'r', encoding='utf-8') as f:
        SUPPLIER, MODEL_NAME, MODEL_VERSION = map(lambda s: s.strip(), map(tuple, csv.reader(f))[0])

    logger.info("Supplier: {}.".format(SUPPLIER))
    logger.info("Model name: {}.".format(MODEL_NAME))
    logger.info("Model version: {}.".format(MODEL_VERSION))

    # Set the web service base URL
    SERVICE_BASE_URL = '/{}/{}/{}'.format(SUPPLIER, MODEL_NAME, MODEL_VERSION)

    # Creating the keys lookup instance
    try:
        keys_lookup = OasisKeysLookupFactory.get_lookup_class_instance(
            MODEL_CONFIG.get('lookup_package_path', is_path=True),
            KEYS_DATA_DIRECTORY,
            {
                'supplier_id': SUPPLIER,
                'model_id': MODEL_NAME,
                'model_version': MODEL_VERSION
            },
            lookup_class_name=MODEL_CONFIG.get('lookup_class_name')

        )
        logger.info("Loaded keys lookup service {}".format(keys_lookup))
    except OasisException as e:
        raise OasisException("Error in loading keys lookup service: {}.".format(str(e)))


try:
    init()
except Exception as e:
    all_vars_dict = dict(globals())
    all_vars_dict.update(locals())
    if all_vars_dict['logger']:
        logger.exception(str(e))


@oasis_log()
@APP.route('{}/healthcheck'.format(SERVICE_BASE_URL) if SERVICE_BASE_URL else '/healthcheck', methods=['GET'])
def healthcheck():
    """
    Healthcheck response.
    """
    return "OK"


@oasis_log()
@APP.route('{}/get_keys'.format(SERVICE_BASE_URL) if SERVICE_BASE_URL else '/get_keys', methods=['POST'])
def get_keys():
    """
    Do a lookup on posted location data.
    """
    response = res_data = None

    try:
        try:
            content_type = request.headers['Content-Type']
        except KeyError:
            raise OasisException('Error: keys request is missing the "Content-Type" header')
        else:
            if content_type not in [
                HTTP_REQUEST_CONTENT_TYPE_CSV,
                HTTP_REQUEST_CONTENT_TYPE_JSON
            ]:
                raise OasisException('Error: unsupported content type: "{}"'.format(content_type))

        try:
            is_gzipped = request.headers['Content-Encoding'] == 'gzip'
        except KeyError:
            is_gzipped = False

        logger.info("Processing locations.")

        loc_data = (
            gzip.zlib.decompress(request.data).decode('utf-8') if is_gzipped
            else request.data.decode('utf-8')
        )

        loc_df = (
            pd.read_csv(io.StringIO(loc_data), float_precision='high') if content_type == HTTP_REQUEST_CONTENT_TYPE_CSV
            else pd.read_json(io.StringIO(loc_data))
        )
        loc_df = loc_df.where(loc_df.notnull(), None)
        loc_df.columns = loc_df.columns.str.lower()

        lookup_results = []
        for record in keys_lookup.process_locations(loc_df):
            lookup_results.append(record)

        logger.info('### {} exposure records generated'.format(len(lookup_results)))

        data_dict = {
            "status": 'success',
            "items": lookup_results
        }

        res_data = json.dumps(data_dict).encode('utf8')

        if COMPRESS_RESPONSE:
            res_data = compress_data(res_data)

        response = Response(
            res_data, status=HTTP_RESPONSE_OK, mimetype=MIME_TYPE_JSON
        )

        if COMPRESS_RESPONSE:
            response.headers['Content-Encoding'] = 'deflate'
            response.headers['Content-Length'] = str(len(res_data))
    except Exception as e:
        logger.error("Error: {}.".format(str(e)))
        response = Response(
            status=HTTP_RESPONSE_INTERNAL_SERVER_ERROR
        )
    finally:
        return response
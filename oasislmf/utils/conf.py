# -*- coding: utf-8 -*-

"""
    Utilities for running system or file-related commands, and other OS-related utilities.
"""
import io
import json
import socket

import os

from .exceptions import OasisException

__all__ = [
    'load_ini_file',
    'replace_in_file',
]


def load_ini_file(ini_file_path):
    """
    Reads an INI file and returns it as a dictionary.
    """
    try:
        with io.open(ini_file_path, 'r', encoding='utf-8') as f:
            lines = map(lambda l: l.strip(), filter(lambda l: l and not l.startswith('['), f.read().split('\n')))
    except IOError as e:
        raise OasisException(str(e))

    di = dict(map(lambda kv: (kv[0].strip(), kv[1].strip()), (line.split('=') for line in lines)))

    for k in di:
        if di[k].lower() == 'true':
            di[k] = True
        elif di[k].lower() == 'false':
            di[k] = False
        else:
            for conv in (int, float, socket.inet_aton):
                try:
                    di[k] = conv(di[k])
                    break
                except:  # noqa: 722
                    continue
    return di


def replace_in_file(source_file_path, target_file_path, var_names, var_values):
    """
    Replaces a list of placeholders / variable names in a source file with a
    matching set of values, and writes it out to a new target file.
    """
    if len(var_names) != len(var_values):
        raise OasisException('Number of variable names does not equal the number of variable values to replace - please check and try again.')

    try:
        with io.open(source_file_path, 'r') as f:
            lines = f.readlines()

        with io.open(target_file_path, 'w') as f:
            for i in range(len(lines)):
                outline = inline = lines[i]
                present_var_names = filter(lambda var_name: var_name in inline, var_names)
                if present_var_names:
                    for var_name in present_var_names:
                        var_value = var_values[var_names.index(var_name)]
                        outline = outline.replace(var_name, var_value)
                f.write(outline)
    except (OSError, IOError) as e:
        raise OasisException(str(e))


class OasisLmfConf(object):
    def __init__(self, overrides=None, conf_path=None):
        self.conf_path = conf_path or os.environ.get('OASIS_LMF_CONFIG_FILE', 'oasislmf.json')

        self.overrides = overrides or {}
        self.config = {}
        self.config_dir = os.path.dirname(conf_path)
        if os.path.exists(self.conf_path):
            with io.open(self.conf_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)

    def get(self, name, default=None, required=False, is_path=False):
        """
        Gets the names parameter from the command line arguments.

        If it is not set on the command line the configuration file
        is checked.

        If it is also not present in the configuration file then
        ``default`` is returned unless ``required`` is false in which
        case an ``OasisException`` is raised.

        :param name: The name of the parameter to lookup
        :type name: str

        :param default: The default value to return if the name is not
            found on the command line or in the configuration file.

        :param required: Flag whether the value is required, if so and
            the parameter is not found on the command line or in the
            configuration file an error is raised.
        :type required: bool

        :param is_path: Flag whether the value should be treated as a path,
            is so the value is processed as relative to the config file.
        :type is_path: bool

        :raise OasisException: If the value is not found and ``required``
            is True

        :return: The found value or the default
        """
        value = None
        cmd_value = self.overrides.get(name, None)
        if cmd_value is not None:
            value = cmd_value
        elif name in self.config:
            value = self.config[name]

        if required and value is None:
            raise OasisException(
                '{} could not be found in the command args or config file ({}) but is required'.format(name, self.conf_path)
            )

        if value is None:
            value = default

        if is_path and value is not None:
            p = os.path.join(self.config_dir, value)
            value = os.path.abspath(p) if not os.path.isabs(value) else p

        return value
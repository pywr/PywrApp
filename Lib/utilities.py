import os
import subprocess
import time
from HydraLib.PluginLib import HydraPluginError
from dateutil import parser as prs


def get_dict(obj):
    '''
    It returns a dict which represent an object
    '''
    if type(obj) is list:
        list_results=[]
        for item in obj:
            list_results.append(get_dict(item))
        return list_results

    if not hasattr(obj, "__dict__"):
        return obj

    result = {}
    for key, val in obj.__dict__.items():
        if key.startswith("_"):
            continue
        if isinstance(val, list):
            element = []
            for item in val:
                element.append(get_dict(item))
        else:
            element = get_dict(obj.__dict__[key])
        result[key] = element
    return result


def is_in_dict(key_, dict_):
    '''
    Check if a string key in a dict, it is not case sensitive
    :param key_: key to be searched
    :param dict_: dict
    :return: the key in the dict or NOne if itis not found
    '''
    for k in dict_.keys():
        if k.strip().lower()== key_.strip().lower():
            return dict_[k]
    return None

def check_output_file(results_file, start_time):
    if os.path.isfile(results_file) == False:
        raise HydraPluginError('No Output file is found ('+results_file+')')
    dt = prs.parse(time.ctime(os.path.getmtime(results_file)))

    delta = (dt - start_time).total_seconds()
    if delta >= 0:
        pass
    else:
        raise HydraPluginError('No updated Output file is found')
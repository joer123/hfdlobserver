# settings.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import collections
import collections.abc
import json
import os
import os.path
import pathlib
import shutil
import yaml

from typing import Union
from copy import copy

import hfdl_observer.env


base_path: pathlib.Path = None  # type: ignore
registry: dict[str, dict] = {
    'observer': {
        'conductor': {
            # ignored_frequencies is a list of frequencies to ignore in assigning receivers.
            # Each entry in the list can be a single frequency (kHz) or a pair of frequencies specifying a closed
            # interval (inclusive)
            # eg. [6661, [2000, 3999]] would ignore 6661kHz as well as all 2 and 3 MHz frequencies.
            # 'ignored_frequencies': [],
        },
        'tracker': {
            # `station_files` files to load station configurations from. should not normally need to be changed.
            'station_files': ['systable.conf'],
            # `station_updates` tracks the remote sources for active frequencies
            # 'station_updates': [
            #     {
            #         'url': "https://hfdl.observer/active.json",  # the URL of the station data.
            #         'period': 61,  # how often (seconds) to query.
            #     },
            # ],
            # `station_url` location of a JSON file with up to date active frequency lists. The default should be fine.
            'station_url': [],
            # `state` local file to keep up dated active station data. should not normally need to be changed.
            'state': 'stations.state',
            # `save_delay` a period after a change is detected before `state` is written, to allow multiple changes
            # to be bundled
            # and save a bit of wear and tear on sdcards if that's important. May be adjusted (seconds) but will also
            # reduce the responsiveness of the observer to changes as reselection of frequencies happens at save time.
            'save_delay': 8,
        },
        'hfdl_listener': {
            # only udp is supported. 0.0.0.0 address is not properly tolerated. Will need to be addressed when
            # remotes are allowed.
            'address': '127.0.0.1',
            'port': 5540,
        },
        'local_receivers': [f'observer-{x:02}' for x in range(1, 14)],
        'all_receivers': {f'observer-{x:02}': {'config': 'web888'} for x in range(1, 14)}
    },
    'cui': {
        'ticker': {
            'bin_size': 60,
            'display_mode': 'frequency',
            'show_all_active': True,
            'show_active_line': True,
            'show_confidence': True,
            'show_targetting': True,
        }
    },
    "configs": {
        'receiver': {
            'web888': {
                'type': 'Web888ExecReceiver',
                'client': {
                    'type': 'KiwiClientProcess',
                    'config': 'default'
                },
                'decoder': {
                    'type': 'IQDecoderProcess',
                    'config': 'default'
                },
            },
            'pipe': {
                'type': 'Web888PipeReceiver',
                'client': {
                    'type': 'KiwiClient',
                    'config': 'default'
                },
                'decoder': {
                    'type': 'IQDecoder',
                    'config': 'default'
                },
            },
            'dummy': {
                'type': 'DummyReceiver',
                'client': {
                    'type': 'KiwiClient',
                    'config': 'default',
                },
                'decoder': {
                    'type': 'IQDecoder',
                    'config': 'default',
                },
            },
            'soapysdr': {
                'type': 'DirectReceiver',
                'decoder': {
                    'type': 'SoapySDRDecoder',
                    'config': 'soapy'
                }
            }
        },
        'decoder': {
            'default': {
                # defaults for all receivers of this type
                'quiet': True,
                'decoder_path': 'dumphfdl',
                'system_table': 'systable.conf',
                'system_table_save': 'systable_updated.conf',
            },
            'soapy': {
                'quiet': True,
                'decoder_path': 'dumphfdl',
                'system_table': 'systable.conf',
                'system_table_save': 'systable_updated.conf',
                'shoulder': 0.8,
            },
            'rsp1+miri': {  # conjectural, miri does not support RSPdx
                'quiet': True,
                'decoder_path': 'dumphfdl',
                'system_table': 'systable.conf',
                'system_table_save': 'systable_updated.conf',
                'soapysdr': {
                    'driver': 'soapyMiri',
                    'bufflen': '65536',
                    'buffers': '64',
                    'asyncBuffs': '32',
                },
                'device-settings': {
                    'flavour': 'SDRplay',
                    'biastee': 'false',
                },
                'sample-rates': [1300000, 1536000, 2048000, 4000000],
                'shoulder': 0.8,
            },
            'rspdx+sdrplay': {
                'quiet': True,
                'settle_time': 10,
                'decoder_path': 'dumphfdl',
                'system_table': 'systable.conf',
                'system_table_save': 'systable_updated.conf',
                'soapysdr': {
                    'driver': 'sdrplay',
                    # 'serial': '1234567890'
                },
                'device-settings': {
                    'rfnotch_ctrl': 'false',
                    'dabnotch_ctrl': 'false',
                    'agc_setpoint': -14,
                    'biasT_ctrl': 'false',
                    'rfgain_sel': 0,
                },
                'sample-rates': [1300000, 1536000, 2048000, [2000000, 5000000]],
                'shoulder': 0.8,
                # 'gain': ...,
                # 'gain-elements': {
                #     'elem': 'value',
                #     'elem2': 'value2',
                # },
                # "freq-correction": 0.0,
                # "freq-offset": 0.0,
                "antenna": 'Antenna B',
            },
            'airspyhf': {
                'quiet': True,
                'settle_time': 1,
                'decoder_path': 'dumphfdl',
                'system_table': 'systable.conf',
                'system_table_save': 'systable_updated.conf',
                'shoulder': 0.8,
                'soapysdr': {
                    'driver': 'airspyhf'
                },
                'sample-rates': [912000, 768000, 650000, 456000, 384000, 228000, 192000],
                'gain': 52,
            }
        },
        'client': {
            'default': {
                'recorder_path': 'kiwirecorder.py',
                'settle_time': 1,
                'quiet': False,
                'username': 'kiwi_nc:hfdlobserver',
                'channel_width': 12000,
                'agc_files': {
                    '*': 'agc.yaml',
                    2: 'agc-02M.yaml',
                    3: 'agc-03M.yaml',
                    4: 'agc-04M.yaml',
                    5: 'agc-05M.yaml',
                    6: 'agc-06M.yaml',
                    8: 'agc-08M.yaml',
                    10: 'agc-10M.yaml',
                    11: 'agc-11M.yaml',
                    12: 'agc-12M.yaml',
                    13: 'agc-13M.yaml',
                    15: 'agc-15M.yaml',
                    17: 'agc-17M.yaml',
                    21: 'agc-21M.yaml',
                }
            },
        },
    },
}


def load(filepath: Union[str, pathlib.Path]) -> None:
    path = pathlib.Path(filepath)
    if not path.is_absolute():
        path = pathlib.Path(os.getcwd()) / path

    global base_path
    base_path = path.parent
    hfdl_observer.env.base_path = base_path

    loaded = yaml.safe_load(path.read_text())
    merge(registry, loaded)
    # print(json.dumps(registry, indent=4))


def merge(a: dict, b: dict) -> dict:
    for key in b:
        if key in a and isinstance(a[key], dict) and isinstance(b[key], dict):
            merge(a[key], b[key])
        else:
            a[key] = b[key]
    return a


def flatten(settings: collections.abc.MutableMapping, kind: str) -> collections.abc.MutableMapping:
    result = copy(settings)
    configs = registry['configs']
    try:
        default_config = settings['config']
    except KeyError:
        return settings

    default_settings = configs[kind][default_config]

    for key, value in settings.items():
        if isinstance(value, collections.abc.MutableMapping):
            for flatkey, flatval in flatten(value, key).items():
                result[key].setdefault(flatkey, flatval)
        if key in default_settings:
            for flatkey, flatval in flatten(default_settings[key], key).items():
                result[key].setdefault(flatkey, flatval)
    for key, value in default_settings.items():
        if isinstance(value, collections.abc.MutableMapping):
            value = flatten(value, key)
        result.setdefault(key, value)

    # print(json.dumps(result, indent=4))
    return result


def as_path(path_str: str, make_absolute: bool = True) -> pathlib.Path:
    path = pathlib.Path(os.path.expanduser(os.path.expandvars(path_str)))
    if path.is_absolute() or not make_absolute:
        return path
    else:
        return base_path / path


def as_executable_path(path_str: str) -> pathlib.Path:
    # find the executable.
    for absolute in [False, True]:
        executable_path = as_path(path_str, make_absolute=absolute)
        if shutil.which(executable_path):
            return executable_path
    raise FileNotFoundError(path_str)


if __name__ == '__main__':
    load('settings.yaml')
    for rname in registry['observer']['local_receivers']:
        r = registry['observer']['all_receivers'][rname]
        print(json.dumps(flatten(r, 'receiver'), indent=4))

# hfdl_observer/util.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import collections
import collections.abc
import datetime
import json
import math
import re

from typing import Any, Union


def tobool(val: Union[bool, str, int]) -> bool:
    val = val.lower() if isinstance(val, str) else val
    if val in ('y', 'yes', 't', 'true', 'on', '1', True, 1):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0', False, 0):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def timestamp_to_datetime(timestamp: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def datetime_to_timestamp(when: datetime.datetime) -> float:
    return when.timestamp()


def deserialise_station_table(station_table: str) -> dict:
    # station table is a custom(?) "conf" format. Almost, but not quite, JSON.
    # sed -e 's/(/[/g' -e s'/)/]/g' -e 's/=/:/g' -e 's/;/,/g' -e 's/^\s*\([a-z]\+\) /"\1"/' >> ~/gs.json
    # does most of the conversion, but not quite.
    # first the simple substitutions
    for f, t in [('(', '['), (')', ']'), ('=', ':'), (';', ',')]:
        station_table = station_table.replace(f, t)
    # quote the keys...
    lines = station_table.split('\n')
    for ix, line in enumerate(lines):
        lines[ix] = re.sub(r'^\s*([a-z]+) ', r'"\1"', line).strip()
    # remove trailing commas
    station_table = '{' + ''.join(lines).replace(',}', '}').replace(',]', ']').strip(',') + '}'
    # in theory it is now JSON decodable.
    return dict(json.loads(station_table))


def hsv_rgb(hue: float, saturation: float, value: float) -> tuple[float, float, float]:
    i = math.floor(hue * 6)
    f = hue * 6 - i
    p = value * (1 - saturation)
    q = value * (1 - f * saturation)
    t = value * (1 - (1 - f) * saturation)
    r, g, b = [
        (value, t, p),
        (q, value, p),
        (p, value, t),
        (p, q, value),
        (t, p, value),
        (value, p, q),
    ][int(i % 6)]
    return r, g, b


def spectrum_colour(value: int, max_value: int) -> tuple[int, int, int]:
    effective = max_value - min(max(0, value), max_value)
    start_hue = 280
    hue_range = 300
    hue = (start_hue + hue_range * effective / max_value) % 360
    hsv = hsv_rgb(hue / 360, 1, 1)
    return (int(hsv[0] * 255), int(hsv[1] * 255), int(hsv[2] * 255))


def normalize_ranges(ranges: list[int | list[int]]) -> list[tuple[int, int]]:
    result: list[tuple[int, int]] = []
    for arange in ranges:
        if arange:
            if isinstance(arange, list):
                result.append(tuple((arange + arange[-1:])[:2]))  # type: ignore # I'm too clever for mypy
            else:
                result.append((arange, arange))
    return result


Pipe = collections.namedtuple('Pipe', 'read write')


def is_bad_file_descriptor(error: OSError) -> bool:
    # for now, naive.
    return 'Errno 9' in str(error)


class DeepChainMap(collections.ChainMap):
    def __getitem__(self, key: Any) -> Any:
        values = (mapping[key] for mapping in self.maps if key in mapping)
        try:
            first = next(values)
        except StopIteration:
            return self.__missing__(key)
        if isinstance(first, collections.abc.MutableMapping):
            return self.__class__(first, *values)
        return first

    def __repr__(self) -> str:
        return repr(dict(self))  # decompose to dict-ish

    def dict(self) -> dict:
        d = dict(self)
        for k, v in list(d.items()):
            if hasattr(v, 'dict'):
                d[k] = v.dict()
            elif isinstance(v, list):
                d[k] = list(e.dict() if hasattr(e, 'dict') else e for e in v)
        return d

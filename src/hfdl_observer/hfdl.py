# hfdl_observer/hfdl.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import datetime
import logging

from typing import Any, Optional, Union


HFDL_CHANNEL_WIDTH: float = 2.4


logger = logging.getLogger()


class HFDLPacketInfo:
    packet: dict[str, Any]
    timestamp: int
    frequency: int
    ground_station: dict[str, Any]
    station: Optional[str]
    bitrate: Optional[int]
    skew: Optional[float]
    frame_slot: Optional[int]
    snr: float
    src: dict[str, Any]
    dst: dict[str, Any]

    def __init__(self, packet: dict[str, Any]):
        # Not at all a full extraction of a packet.
        packet = packet.get('hfdl', packet)  # in case it's not unwrapped.
        self.packet = packet
        self.timestamp = packet['t']['sec']
        self.frequency = packet['freq'] // 1000
        self.bitrate = packet.get('bitrate')
        self.skew = packet.get('freq_skew')
        self.frame_slot = packet.get('slot')
        self.snr = packet['sig_level'] - packet['noise_level']
        app_data = packet.get('spdu', packet.get('lpdu', {}))
        self.src = app_data.get('src', {})
        self.dst = app_data.get('dst', {})
        self.station = packet.get('station')
        if self.is_downlink:
            self.ground_station = self.dst
        elif self.is_uplink:
            self.ground_station = self.src
        else:
            self.ground_station = {}

    @property
    def is_uplink(self) -> bool:
        return self.src.get('type') == 'Ground station'

    @property
    def is_downlink(self) -> bool:
        return self.dst.get('type') == 'Ground station'

    @property
    def is_squitter(self) -> bool:
        return True if self.packet.get('spdu') else False

    @property
    def when(self) -> datetime.datetime:
        return datetime.datetime.utcfromtimestamp(self.timestamp)

    cpdlc_pos = "acars.arinc622.cpdlc.atc_uplink_msg.atc_uplink_msg_element_id.data.pos.data.lat_lon".split('.')
    cpdlc_alt = "acars.arinc622.cpdlc.atc_uplink_msg.atc_uplink_msg_element_id.data.alt_pos.pos.data.lat_lon".split('.')

    @property
    def position(self) -> Optional[tuple[str, str]]:
        # position could be in several places...
        # all in "hfdl.lpdu.hfnpdu"
        # "pos"
        # "acars.arinc622.adsc.tags.<list>.basic_report"
        # "acars.arinc622.cpdlc.atc_uplink_msg.atc_uplink_msg_element_id.data.pos.data.lat_lon"
        # "acars.arinc622.cpdlc.atc_uplink_msg.atc_uplink_msg_element_id.data.alt_pos.pos.data.lat_lon"
        def get_path(d: dict, path: list[str]) -> Optional[Any]:
            car, *cdr = path
            try:
                node = d[car]
            except (KeyError, AttributeError, IndexError):
                return None
            if cdr:
                return get_path(node, cdr)
            return node

        try:
            hfnpdu = self.packet.get('lpdu', {}).get('hfnpdu')
            if hfnpdu:
                pos = hfnpdu.get('pos')
                if pos:
                    return (pos['lat'], pos['lon'])
                for tag in get_path(hfnpdu, ['acars', 'arinc622', 'adsc', 'tags']) or []:
                    pos = tag.get('basic_report')
                    if pos:
                        return (pos['lat'], pos['lon'])
                for p in [self.cpdlc_pos, self.cpdlc_alt]:
                    pos = get_path(hfnpdu, p)
                    if pos:
                        return (pos['lat'], pos['lon'])
        except KeyError:
            pass

        return None

    def __str__(self) -> str:
        direction = "FROM" if self.is_uplink else "TO"
        if self.packet:
            subtype = "spdu" if self.packet.get('spdu') else ('lpdu' if self.packet.get('lpdu') else 'other')
        else:
            subtype = 'unknown'
        station = self.station or ""
        if station:
            gs = self.ground_station.get('name', None)
            if gs is None:
                gs_id = self.ground_station.get('id', -1)
                gs = STATIONS.get(gs_id).get('name', 'n/a')
            gs = gs.split(',', 1)[0]
        else:
            gs = 'unknown'
        return f'<HFDL/{subtype} {station}@{self.timestamp} {self.frequency}kHz ({self.snr:.1f}dB) {direction} {gs}>'


class StationLookup:
    by_id: dict[int, dict]
    by_freq: dict[int, dict]

    def update(self, systable: dict[int, Any]) -> None:
        self.by_id = systable
        self.by_freq = {}
        for sid, station in systable.items():
            for freq in station['frequencies']:
                self.by_freq[freq] = station

    def __getitem__(self, key: Union[str, int, float]) -> Any:
        return self.get(key)

    def get(self, key: Union[str, int, float], default: Optional[dict[int, Any]] = None) -> Any:
        try:
            k = int(key)
            if k < 2000:
                return self.by_id[k]
            return self.by_freq[k]
        except KeyError:
            if default:
                return default
            raise
        except AttributeError as err:
            if default:
                return default
            raise KeyError(f'Mapping error for {key}') from err


STATIONS = StationLookup()

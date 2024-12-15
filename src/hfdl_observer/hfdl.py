# hfdl_observer/hfdl.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import datetime
import logging

from typing import Any, Optional


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
    def when(self) -> datetime.datetime:
        return datetime.datetime.utcfromtimestamp(self.timestamp)

    def __str__(self) -> str:
        direction = "FROM" if self.is_uplink else "TO"
        if self.packet:
            subtype = "spdu" if self.packet.get('spdu') else ('lpdu' if self.packet.get('lpdu') else 'other')
        else:
            subtype = 'unknown'
        gs = self.ground_station.get('name', 'n/a').split(',', 1)[0] if self.station else 'n/a'
        station = self.station or ""
        return f'<HFDL/{subtype} {station}@{self.timestamp} {self.frequency}kHz ({self.snr:.1f}dB) {direction} {gs}>'

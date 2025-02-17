# hfdl_observer/data.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#
import collections
import dataclasses
import datetime

from typing import Any, Mapping, Optional, Sequence

import hfdl_observer.hfdl as hfdl


class ListenerConfig:
    proto: str = 'udp'
    address: str = '127.0.0.1'
    port: int = 5542


class ObservingChannel:
    _frequencies: set[int]
    allowed_width: int

    def __init__(self, allowed_width: int, frequencies: Optional[list[int]] = None):
        self.allowed_width = allowed_width
        self._frequencies = set(frequencies) if frequencies else set()

    @property
    def min(self) -> float:
        return min(self._frequencies)

    @property
    def max(self) -> float:
        return max(self._frequencies)

    @property
    def center(self) -> float:
        return (self.min + self.max) / 2.0

    @property
    def width(self) -> float:
        if self.frequencies:
            channel_width: float = hfdl.HFDL_CHANNEL_WIDTH
            return self.max - self.min + channel_width
        else:
            return 0.0

    @property
    def frequencies(self) -> list[int]:
        return sorted(self._frequencies)

    def maybe_add(self, frequency: int) -> bool:
        peephole_width = self.allowed_width - hfdl.HFDL_CHANNEL_WIDTH
        if not self._frequencies or (
            abs(frequency - self.min) <= peephole_width and abs(frequency - self.max) <= peephole_width
        ):
            self._frequencies.add(frequency)
            return True
        return False

    def clone(self) -> 'ObservingChannel':
        return ObservingChannel(self.allowed_width, self.frequencies)

    def __str__(self) -> str:
        return f'{self.frequencies}'

    def __repr__(self) -> str:
        return f'ObservingChannel({self.allowed_width}, {self.frequencies})'


class ChannelObserver:
    def observable_widths(self) -> list[int]:
        raise NotImplementedError(str(self.__class__))

    def width_for(self, frequencies: list[int]) -> int:
        needed = max(hfdl.HFDL_CHANNEL_WIDTH, max(frequencies) - min(frequencies))
        widths = sorted(self.observable_widths(), reverse=True)
        for available_width in widths:
            if needed <= available_width:
                return available_width
        raise ValueError(f'cannot accomodate {frequencies} in channel widths {widths}')

    def observing_channel_for(self, frequencies: list[int]) -> ObservingChannel:
        return ObservingChannel(self.width_for(frequencies), frequencies)


class DeprecatedObserverParameters:
    max_sample_rate: int
    num_clients: int

    def channel(self, initial_frequencies: list[int]) -> ObservingChannel:
        return ObservingChannel(self.max_sample_rate, initial_frequencies)


@dataclasses.dataclass
class ReceivedPacket:
    when: datetime.datetime
    agent: str
    ground_station: int
    frequency: int
    uplink: bool
    latitude: Optional[float]
    longitude: Optional[float]
    kind: Optional[str] = None


@dataclasses.dataclass
class FrequencyWatch:
    sequence: int
    started: datetime.datetime
    station_id: int
    frequency: int
    is_targetted: bool
    ended: Optional[datetime.datetime] = None
    observer_id: Optional[str] = None


BinnedPacketDataType = Mapping[int | str, Sequence[int]]


class AbstractPacketWatcher:
    def on_hfdl(self, packet_info: hfdl.HFDLPacketInfo) -> None:
        raise NotImplementedError(str(self.__class__))

    def packets_by_frequency(cls, bin_size: int, num_bins: int) -> BinnedPacketDataType:
        raise NotImplementedError(str(self.__class__))

    def packets_by_agent(cls, bin_size: int, num_bins: int) -> BinnedPacketDataType:
        raise NotImplementedError(str(self.__class__))

    def packets_by_station(cls, bin_size: int, num_bins: int) -> BinnedPacketDataType:
        raise NotImplementedError(str(self.__class__))

    def packets_by_band(cls, bin_size: int, num_bins: int) -> BinnedPacketDataType:
        raise NotImplementedError(str(self.__class__))


PACKET_WATCHER = AbstractPacketWatcher()  # Must be overridden during app initialization


class AgentStats:
    events: dict[str, dict[str, Any]]

    def __init__(self, horizon: int = 3600):
        self.events = collections.defaultdict(dict)
        self.horizon = horizon

    def add_event(self, event: str, agent: str, timestamp: int) -> None:
        self.events[event].setdefault(agent, []).append(timestamp)

    def counts(self, event: str) -> dict[str, int]:
        cutoff = datetime.datetime.now(datetime.timezone.utc).timestamp() - self.horizon
        results = {}
        agent: str
        hits: list[int]
        for agent, hits in self.events[event].items():
            results[agent] = sum(1 for hit in hits if hit >= cutoff)
        return results

    def prune(self, epoch: Optional[int] = None) -> None:
        if epoch is None:
            epoch = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        cutoff = epoch - self.horizon
        for event in self.events.values():
            for agent in event:
                current = event[agent]
                pruned = [hit for hit in current if hit >= cutoff]
                event[agent] = pruned

# hfdl_observer/groundstation.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import collections
import datetime
import itertools
import json
import logging
import re

from collections.abc import Iterable
from enum import Enum
from typing import Any, Iterator, Optional, Sequence, Union

import hfdl_observer.bus
import hfdl_observer.hfdl


logger = logging.getLogger(__name__)

GS_EXPIRY = 3600
SQUITTER_FRAME_TIME = 32 * 6


class Strata(Enum):
    SYSTABLE = 0
    CACHE = 1
    PERFORMANCE = 2
    SQUITTER = 3
    SELF = 4


class GroundStationFrequency:
    def __init__(self, khz: int, valid_at: int, lifetime: int = GS_EXPIRY):
        self.khz = khz
        self._valid_at = valid_at or 0
        self.lifetime = lifetime
        self.active = self._valid_at > 0

    @property
    def is_valid(self) -> bool:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        return self.valid_at + self.lifetime >= now

    @property
    def is_active(self) -> bool:
        return self.active and self.is_valid

    @classmethod
    def build(
        cls, frequency: Union['GroundStationFrequency', int], valid_at: int, lifetime: int = GS_EXPIRY
    ) -> 'GroundStationFrequency':
        if isinstance(frequency, GroundStationFrequency):
            return frequency
        else:
            return cls(frequency, valid_at, lifetime)

    @property
    def valid_at(self) -> int:
        return self._valid_at

    @valid_at.setter
    def valid_at(self, valid_at: int) -> None:
        pass

    def __str__(self) -> str:
        state = 'active' if self.is_active else ('valid' if self.is_valid else 'invalid')
        return f"<{self.khz} {state}>"


class GroundStation:
    last_updated: int = 0
    update_source: Optional[str] = None
    stratum: Strata = Strata.CACHE  # slightly smelly
    id: int
    name: Optional[str]
    _frequencies: dict[int, GroundStationFrequency]
    default_lifetime: int = 0
    longitude: Optional[float] = None
    latitude: Optional[float] = None

    def __init__(self, station_id: int, name: Optional[str] = None, default_lifetime: int = GS_EXPIRY):
        self.id = station_id
        self.name = None
        self._frequencies = {}
        self.default_lifetime = default_lifetime

    def frequencies(self) -> Iterator[GroundStationFrequency]:
        yield from self._frequencies.values()

    def khz(self) -> Iterator[int]:
        yield from self._frequencies.keys()

    def add_frequencies(
        self, frequencies: Sequence[Union[int, GroundStationFrequency]], stamp: Optional[int] = None
    ) -> None:
        if stamp is None:
            stamp = self.last_updated
        for f in frequencies or []:
            frequency = GroundStationFrequency.build(f, stamp, self.default_lifetime)
            stored = self._frequencies.setdefault(frequency.khz, frequency)
            stored.valid_at = max(stored.valid_at, frequency.valid_at)

    def set_frequencies(
        self, frequencies: Sequence[Union[int, GroundStationFrequency]], stamp: Optional[int] = None
    ) -> None:
        if stamp is None:
            stamp = self.last_updated
        if frequencies:
            fs = []
            for f in frequencies or []:
                fs.append(GroundStationFrequency.build(f, stamp, self.default_lifetime))
            self._frequencies = {freq.khz: freq for freq in fs}

    def prune_expired_frequencies(self) -> None:
        for key, value in list(self._frequencies.items()):
            if not value.is_valid:
                del self._frequencies[key]

    def __str__(self) -> str:
        return f"<GroundStation #{self.id} {self.name}>"

    @property
    def any_active_frequencies(self) -> bool:
        return any(frequency.is_active for frequency in self._frequencies.values())

    @property
    def active_frequencies(self) -> list[GroundStationFrequency]:
        return [frequency for frequency in self._frequencies.values() if frequency.is_active]

    @property
    def valid_frequencies(self) -> list[GroundStationFrequency]:
        return [frequency for frequency in self._frequencies.values() if frequency.is_valid]

    @property
    def last_pseudoframe(self) -> int:
        return (self.last_updated // SQUITTER_FRAME_TIME)


class GroundStationTable(hfdl_observer.bus.Publisher):
    updates = 0
    LIFETIME = 3 * GS_EXPIRY
    stations_by_id: dict[int, GroundStation]
    stations_by_name: dict[str, GroundStation]

    def __init__(self) -> None:
        super().__init__()
        self.stations_by_id = {}
        self.stations_by_name = {}

    def update_lookups(self) -> None:
        self.stations_by_name = {gs.name: gs for gs in self.stations_by_id.values() if gs.name}

    def prune_expired(self) -> None:
        for station in list(self.stations_by_id.values()):
            station.prune_expired_frequencies()
            if not station.any_active_frequencies and station.id in self.stations_by_id:
                logger.debug(f'{self} pruning {station}')
                del self.stations_by_id[station.id]

    def update(self, extra: Any) -> None:
        self.updates += 1
        self.prune_expired()
        self.update_lookups()
        self.publish('update', self)

    def get(self, key: Union[int, str], autocreate: bool = False) -> GroundStation:
        try:
            ik = int(key)
        except ValueError:
            return self.stations_by_name[str(key)]
        try:
            return self.stations_by_id[ik]
        except KeyError:
            if autocreate:
                station = GroundStation(ik, default_lifetime=self.LIFETIME)
                try:
                    loc = hfdl_observer.hfdl.STATIONS[ik]
                except KeyError:
                    pass
                else:
                    station.longitude = loc['longitude']
                    station.latitude = loc['latitude']
                self.stations_by_id[ik] = station
                return station
            raise

    def __getitem__(self, key: Union[int, str]) -> GroundStation:
        return self.get(key)

    def __contains__(self, key: Union[int, str]) -> bool:
        try:
            return int(key) in self.stations_by_id
        except ValueError:
            pass
        return key in self.stations_by_name

    def valid_frequencies(self, key: Union[int, str]) -> list[GroundStationFrequency]:
        try:
            return self[key].valid_frequencies
        except KeyError:
            return []

    def active_frequencies(self, key: Union[int, str]) -> list[GroundStationFrequency]:
        try:
            return self[key].active_frequencies
        except KeyError:
            return []

    # @property
    # def stations(self) -> Iterable[GroundStation]:
    #     return self.stations_by_id.values()

    def stations(self, stratum: Optional[Strata]) -> Iterable[GroundStation]:
        for station in self.stations_by_id.values():
            if stratum is None or station.stratum == stratum:
                yield station


class GroundStationStatus(hfdl_observer.bus.Publisher):
    _tables: list[GroundStationTable]
    cached_station_strata: dict[int, dict[Strata, list[GroundStation]]]

    def __init__(self) -> None:
        super().__init__()
        self._tables = []
        self.cached_station_strata = {}

    @property
    def tables(self) -> Iterator[GroundStationTable]:
        yield from self._tables

    def add_table(self, table: GroundStationTable) -> None:
        self._tables.append(table)
        table.subscribe('update', self.on_table_updated)

    def on_table_updated(self, table: GroundStationTable) -> None:
        self.publish('update', self)
        self.populate_strata()

    def valid_frequencies(self, for_station: GroundStation) -> list[int]:
        # valid frequencies gives you the list of valids from all available tables.
        found = set()
        for gsf in itertools.chain(*(lookup.valid_frequencies(for_station.id) for lookup in self.tables)):
            if gsf.is_valid:
                found.add(gsf.khz)
        return sorted(found)

    def active_frequencies(self, for_station: Union[int, str, GroundStation]) -> list[int]:
        # active frequencies only gives you the list of actives from the first available table.
        found: list[int] = []
        station_id: Union[int, str] = for_station.id if hasattr(for_station, 'id') else for_station
        for table in self.tables:
            freqs = table.active_frequencies(station_id)
            if freqs:
                found.extend(gsf.khz for gsf in freqs)
                break
        return sorted(found)

    @property
    def station_names(self) -> list[str]:
        found = set()
        for lookup in self.tables:
            for station in lookup.stations_by_name.keys():
                found.add(station)
        return sorted(found)

    @property
    def station_ids(self) -> list[int]:
        found = set()
        for lookup in self.tables:
            for station in lookup.stations_by_id.keys():
                found.add(station)
        return sorted(found)

    def get(self, station_key: Union[int, str]) -> GroundStation:
        sid = int(station_key)
        strata = reversed(list(s for s in Strata))
        station_data = self.cached_station_strata[sid]
        for stratum in strata:
            try:
                return station_data[stratum][0]
            except (KeyError, IndexError):
                pass
        raise KeyError(f'{station_key} did not have any valid strata data')

    def __getitem__(self, key: Union[int, str]) -> GroundStation:
        return self.get(key)

    def populate_strata(self) -> None:
        for sid in self.station_ids:
            ss = self.station_strata(sid)
            self.cached_station_strata[sid] = ss

    def station_strata(self, station_key: Union[int, str]) -> dict[Strata, list[GroundStation]]:
        strata = collections.defaultdict(list)
        for lookup in self.tables:
            try:
                station = lookup[station_key]
            except KeyError:
                continue
            current_strata = station.stratum
            strata[current_strata].append(station)
        return strata


class SquitterTable(GroundStationTable):

    def update(self, hfdl_packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        hfdl = hfdl_packet.packet
        last_updated = hfdl.get('t', {}).get('sec', 0)
        any_updated = False
        squitter = hfdl.get('spdu', {})
        if squitter:
            self.publish("event", ("squitter", hfdl_packet.station, last_updated))
        for gs in squitter.get('gs_status', []):
            src_id = hfdl_packet.ground_station['id']
            sid = gs['gs']['id']
            stn_id = int(sid) if sid else None
            if not self.should_process(src_id, stn_id):
                continue
            station = self.get(sid, autocreate=True)
            new_freqs = sorted((int(sf['freq']) for sf in gs['freqs'] if 'freq' in sf))
            old_freqs = sorted(station.khz())
            if station.last_pseudoframe < (last_updated // SQUITTER_FRAME_TIME) or new_freqs != old_freqs:
                station.last_updated = last_updated
                station.update_source = src_id
                station.name = gs['gs']['name']
                station.set_frequencies(new_freqs)
                logger.debug(f'squitter update for {station}')
                station.stratum = Strata.SELF if stn_id == src_id else Strata.SQUITTER
                any_updated = True
        if any_updated:
            super().update(None)

    def should_process(self, src_id: int, station_id: Optional[int]) -> bool:
        return True


class SelfSquitterTable(SquitterTable):
    def should_process(self, src_id: int, station_id: Optional[int]) -> bool:
        return src_id == station_id


class UpdateTable(GroundStationTable):
    LIFETIME = GS_EXPIRY

    def update(self, hfdl_packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        hfdl = hfdl_packet.packet
        last_updated = hfdl.get('t', {}).get('sec', 0)
        any_updated = False
        for gs in hfdl.get('lpdu', {}).get('hfnpdu', {}).get('freq_data', []):
            freqs = [int(sf['freq']) for sf in gs['heard_on_freqs'] if 'freq' in sf]
            if freqs:
                station = self.get(gs['gs']['id'], autocreate=True)
                station.last_updated = last_updated
                station.update_source = 'hfnpdu'
                station.name = gs['gs']['name']
                station.stratum = Strata.PERFORMANCE
                station.add_frequencies(freqs)
                any_updated = True
                logger.debug(f'performance update for {station}')
        if any_updated:
            self.publish("event", ("performance", hfdl_packet.station, last_updated))
            super().update(None)


class AirframesStationTable(GroundStationTable):
    def update(self, airframes: dict) -> None:
        for gs in airframes.get('ground_stations', []):
            try:
                station = self.get(gs['id'], autocreate=True)
            except KeyError:
                logger.warning(f'{self} ignoring spurious station `{gs["id"]}`')
            else:
                last_updated = gs['last_updated']
                if last_updated < 0:
                    last_updated += datetime.datetime.now(datetime.timezone.utc).timestamp()
                station.last_updated = last_updated
                station.name = gs['name']
                station.update_source = gs.get('update_source', 'remote')
                station.stratum = Strata(gs.get('stratum', Strata.CACHE))
                station.set_frequencies(gs['frequencies']['active'])
                # logger.debug(f'airframes update for {station}')
        super().update(None)


class SystemTable(GroundStationTable):
    LIFETIME = 10 * 365 * 24 * 3600

    def update(self, station_table: str) -> None:
        # station table is a custom "conf" format. Almost, but not quite, JSON.
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
        data = json.loads(station_table)
        hfdl_stations: dict[int, dict] = {}
        for gs in data['stations']:
            station = self.get(gs['id'], autocreate=True)
            station.name = gs['name']
            station.last_updated = int(datetime.datetime.now(datetime.timezone.utc).timestamp() - GS_EXPIRY)
            station.update_source = 'systable'
            station.longitude = gs['lon']
            station.latitude = gs['lat']
            station.stratum = Strata.SYSTABLE
            freqs = [int(f) for f in gs['frequencies']]
            station.set_frequencies(freqs)
            hfdl_stations.setdefault(gs['id'], {}).update({
                'longitude': station.longitude,
                'latitude': station.latitude,
                'name': station.name,
                'id': station.id,
                'frequencies': freqs
            })
        hfdl_observer.hfdl.STATIONS.update(hfdl_stations)
        super().update(None)


class ObserverTable(GroundStationTable):
    def update(self, source_table: GroundStationTable) -> None:
        for gs in source_table.stations(None):
            if gs.last_updated:  # never add None (conf) or 0 (failed airframes)
                station = self.get(gs.id, autocreate=True)
                station.name = gs.name
                station.last_updated = max(station.last_updated, gs.last_updated)
                station.update_source = station.update_source
                station.stratum = station.stratum
                station.add_frequencies(gs.valid_frequencies)
        super().update(None)


class ActorStats:
    events: dict[str, dict[str, Any]]

    def __init__(self, horizon: int = GS_EXPIRY):
        self.events = collections.defaultdict(dict)
        self.horizon = horizon

    def add_event(self, event: str, actor: str, timestamp: int) -> None:
        self.events[event].setdefault(actor, []).append(timestamp)

    def counts(self, event: str) -> dict[str, int]:
        cutoff = datetime.datetime.now(datetime.timezone.utc).timestamp() - self.horizon
        results = {}
        actor: str
        hits: list[int]
        for actor, hits in self.events[event].items():
            results[actor] = sum(1 for hit in hits if hit >= cutoff)
        return results

    def prune(self, epoch: Optional[int] = None) -> None:
        if epoch is None:
            epoch = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        cutoff = epoch - self.horizon
        for event in self.events.values():
            for actor in event:
                current = event[actor]
                pruned = [hit for hit in current if hit >= cutoff]
                event[actor] = pruned

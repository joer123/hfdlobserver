# hfdl_observer/orm.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import datetime
import functools
import itertools
import logging

from typing import Any, Mapping, Optional, Sequence

import pony.orm as pony

import hfdl_observer.bus as bus
import hfdl_observer.hfdl as hfdl
import hfdl_observer.network as network
import hfdl_observer.data as data
import hfdl_observer.util as util


logger = logging.getLogger(__name__)

db = pony.Database()  # 'file::memory:?cache=shared')
db.bind(provider='sqlite', filename=':memory:')


@functools.cache
@pony.db_session(strict=True)
def pagesize() -> int:
    return int(db.execute("PRAGMA page_size;").fetchone()[0])


# Using `db.Entity` directly won't work, as both MyPy and Pyright won't
# allow inheriting a class from a variable. For Pyright this declaration
# is enough misdirection for it not to complain, but MyPy needs an extra
# `type: ignore` comment above each model declaration to work.
DbEntity = db.Entity


class StationAvailability(DbEntity):  # type: ignore
    station_id = pony.Required(int)
    stratum = pony.Required(int)
    valid_at = pony.Required(datetime.datetime)
    valid_to = pony.Optional(datetime.datetime)
    frequencies = pony.Required(pony.IntArray)
    agent = pony.Required(str)
    from_station = pony.Optional(int)
    # SELECT * from Availability where sid = ? and valid_to > NOW order by stratum desc, valid_at desc
    pony.PrimaryKey(station_id, stratum, valid_at)
    pony.composite_index(station_id, stratum, valid_at, valid_to)

    def as_local(self) -> network.StationAvailability:
        return network.StationAvailability(**self.to_dict())

    @classmethod
    @pony.db_session(strict=True)
    def prune(cls) -> None:
        when = util.make_naive_utc(util.now() - datetime.timedelta(days=1))
        pony.delete(a for a in cls if a.valid_to is not None and a.valid_to < when)
        pages = int(db.execute("PRAGMA page_count;").fetchone()[0])
        logger.debug(f'DB size is {pages * pagesize()}')


class ReceivedPacket(DbEntity):  # type: ignore
    when = pony.Required(datetime.datetime)
    agent = pony.Required(str)
    ground_station = pony.Required(int)
    frequency = pony.Required(int)
    kind = pony.Optional(str)
    uplink = pony.Required(bool)
    latitude = pony.Optional(float)
    longitude = pony.Optional(float)
    # select when // 60, frequency, count(*) from ReceivedPacket where when > ? group by when // 60, frequency
    pony.PrimaryKey(when, frequency)

    def as_local(self) -> data.ReceivedPacket:
        return data.ReceivedPacket(**self.to_dict())

    @classmethod
    @pony.db_session(strict=True)
    def prune(cls, before: datetime.datetime) -> None:
        try:
            initial = int(db.execute("PRAGMA page_count;").fetchone()[0])
            pony.delete(p for p in cls if p.when < before)
            after = int(db.execute("PRAGMA page_count;").fetchone()[0])
            logger.debug(f'DB size was {initial * pagesize()}, now {after * pagesize()}')
        except Exception as err:
            logger.error('cannot prune', exc_info=err)


# Not currently used
class FrequencyWatch(DbEntity):  # type: ignore
    sequence = pony.PrimaryKey(int, auto=True)
    started = pony.Required(datetime.datetime)
    ended = pony.Optional(datetime.datetime)
    station_id = pony.Required(int)
    frequency = pony.Required(int)
    is_targetted = pony.Required(bool)
    observer_id = pony.Optional(str)

    def as_local(self) -> data.FrequencyWatch:
        return data.FrequencyWatch(**self.to_dict())

    @classmethod
    @pony.db_session(strict=True)
    def prune(cls, before: datetime.datetime) -> None:
        pony.delete(p for p in cls if p.ended < before)


# after all entities have been created
db.generate_mapping(create_tables=True)


class NetworkUpdater(network.AbstractNetworkUpdater):

    def add(self, base: network.StationAvailability) -> bool:
        try:
            StationAvailability[base.station_id, base.stratum, base.valid_at]
        except pony.ObjectNotFound:
            StationAvailability(
                valid_at=base.valid_at,
                valid_to=base.valid_to,
                station_id=base.station_id,
                stratum=base.stratum,
                frequencies=base.frequencies,
                agent=base.agent,
                from_station=base.from_station,
            )
            return True
        return False

    def updated(self, availabilities: Optional[Sequence[network.StationAvailability]] = None) -> None:
        db.commit()
        super().updated(availabilities)

    # wrap in a session for database access on delegated functions.
    @pony.db_session(strict=True)
    def on_hfdl(self, packet_info: hfdl.HFDLPacketInfo) -> None:
        super().on_hfdl(packet_info)

    # wrap in a session for database access on delegated functions.
    @pony.db_session(strict=True)
    def on_community(self, airframes: dict) -> None:
        super().on_community(airframes)

    # wrap in a session for database access on delegated functions.
    @pony.db_session(strict=True)
    def on_systable(self, station_table: str) -> None:
        super().on_systable(station_table)

    def _for_station(
        self, station_id: int, at: Optional[datetime.datetime] = None
    ) -> Optional[network.StationAvailability]:
        when = util.make_naive_utc(at or util.now())
        q = pony.select(
            a for a in StationAvailability
            if a.station_id == station_id
        )
        q = q.where(lambda a: (a.valid_to is None or a.valid_to >= when) and a.valid_at <= when)
        q = q.sort_by(pony.desc(StationAvailability.stratum), pony.desc(StationAvailability.valid_at))
        row = q.first()
        return row.as_local() if row else None

    @pony.db_session(strict=True)
    def active(self, at: Optional[datetime.datetime] = None) -> Sequence[network.StationAvailability]:
        found = []
        sids = network.STATIONS.by_id.keys()
        for sid in sids:
            la = self._for_station(sid, at=at)
            if la:
                found.append(la)
                # kind of hacky, but will work in most cases.
                # (active frequencies in cached station data only get updated when the full active list is requested)
                # if not at:
                #     # only update based on most recent data
                #     network.STATIONS[sid].update_active(la.frequencies)
                #     network.STATIONS.refresh()
        return found

    @pony.db_session(strict=True)
    def current(self) -> Sequence[network.StationAvailability]:
        return self.active()

    def current_freqs(self) -> Sequence[int]:
        return list(itertools.chain(*[a.frequencies for a in self.current()]))

    @pony.db_session(strict=True)
    def station(self, station_id: int, at: Optional[datetime.datetime] = None) -> Optional[network.StationAvailability]:
        return self._for_station(station_id, at=at)

    def prune(self, _: Any) -> None:
        StationAvailability.prune()


class PacketWatcher(data.AbstractPacketWatcher):
    periodic_task: Optional[asyncio.Task] = None

    @pony.db_session(strict=True)
    def on_hfdl(self, packet_info: hfdl.HFDLPacketInfo) -> None:
        position = packet_info.position or (None, None)
        ReceivedPacket(
            when=util.make_naive_utc(util.now()),
            agent=packet_info.station,
            ground_station=packet_info.ground_station['id'],
            frequency=packet_info.frequency,
            kind='spdu' if packet_info.is_squitter else 'lpdu',
            uplink=packet_info.is_uplink,
            latitude=position[0],
            longitude=position[1],
        )
        db.commit()

    def recent_packets(cls, when: datetime.datetime) -> Sequence[ReceivedPacket]:
        q = pony.select(r for r in ReceivedPacket).where(lambda r: r.when > when)
        return q[:]

    def prune(self) -> None:
        ReceivedPacket.prune(util.now() - datetime.timedelta(days=1))

    def prune_every(self, period: int) -> None:
        if self.periodic_task:
            self.periodic_task.cancel()
        periodic_callback = bus.PeriodicCallback(period, [self.prune], False)
        self.periodic_task = asyncio.get_event_loop().create_task(periodic_callback.run())

    @pony.db_session(strict=True)
    def packets_by_frequency(cls, bin_size: int, num_bins: int) -> Mapping[int, Sequence[int]]:
        data: dict[int, list[int]] = collections.defaultdict(lambda: [0] * num_bins)
        total_seconds = bin_size * num_bins
        when = util.make_naive_utc(util.now() - datetime.timedelta(seconds=total_seconds))
        for packet in cls.recent_packets(when):
            bin_number = int((when - packet.when).total_seconds() // bin_size)
            data[packet.frequency][bin_number] += 1
        return data

    @pony.db_session(strict=True)
    def packets_by_agent(cls, bin_size: int, num_bins: int) -> Mapping[str, Sequence[int]]:
        data: dict[str, list[int]] = collections.defaultdict(lambda: [0] * num_bins)
        total_seconds = bin_size * num_bins
        when = util.make_naive_utc(util.now() - datetime.timedelta(seconds=total_seconds))
        for packet in cls.recent_packets(when):
            bin_number = int((when - packet.when).total_seconds() // bin_size)
            data[packet.agent or 'unknown'][bin_number] += 1
        return data

    @pony.db_session(strict=True)
    def packets_by_station(cls, bin_size: int, num_bins: int) -> Mapping[str, Sequence[int]]:
        data: dict[str, list[int]] = collections.defaultdict(lambda: [0] * num_bins)
        total_seconds = bin_size * num_bins
        when = util.make_naive_utc(util.now() - datetime.timedelta(seconds=total_seconds))
        for packet in cls.recent_packets(when):
            bin_number = int((when - packet.when).total_seconds() // bin_size)
            station = network.STATIONS[packet.frequency]
            data[f'#{station.station_id}. {station.station_name}'][bin_number] += 1
        return data

    @pony.db_session(strict=True)
    def packets_by_band(cls, bin_size: int, num_bins: int) -> Mapping[int, Sequence[int]]:
        data: dict[int, list[int]] = collections.defaultdict(lambda: [0] * num_bins)
        total_seconds = bin_size * num_bins
        when = util.make_naive_utc(util.now() - datetime.timedelta(seconds=total_seconds))
        for packet in cls.recent_packets(when):
            bin_number = int((when - packet.when).total_seconds() // bin_size)
            data[packet.frequency // 1000][bin_number] += 1
        return data

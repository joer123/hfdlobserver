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

db = pony.Database()
db.bind(provider='sqlite', filename=':memory:')


TS_FACTOR = 1_000_000.0


def to_datetime(timestamp: int) -> datetime.datetime:
    return util.timestamp_to_datetime(timestamp / TS_FACTOR)


def to_datetime_or_none(timestamp: int | None) -> datetime.datetime | None:
    if timestamp is None:
        return None
    return to_datetime(timestamp)


def to_timestamp(when: datetime.datetime) -> int:
    return int(util.datetime_to_timestamp(when) * TS_FACTOR)


def to_timestamp_or_none(when: None | datetime.datetime) -> int | None:
    if when is None:
        return None
    return to_timestamp(when)


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
    valid_at = pony.Required(int, size=64)
    valid_to = pony.Optional(int, size=64)
    frequencies = pony.Required(pony.IntArray)
    agent = pony.Required(str)
    from_station = pony.Optional(int)
    # SELECT * from Availability where sid = ? and valid_to > NOW order by stratum desc, valid_at desc
    pony.PrimaryKey(station_id, stratum, valid_at)
    pony.composite_index(station_id, stratum, valid_at, valid_to)

    def as_local(self) -> network.StationAvailability:
        d = self.to_dict()
        d['valid_at'] = to_datetime(self.valid_at)
        d['valid_to'] = to_datetime_or_none(self.valid_to)
        return network.StationAvailability(**d)

    @classmethod
    @pony.db_session(strict=True)
    def prune(cls) -> None:
        before_prune = pony.count(a for a in StationAvailability)
        horizon = to_timestamp(util.now() - datetime.timedelta(days=1))
        pony.delete(a for a in StationAvailability if a.valid_to is not None and a.valid_to < horizon)
        # pages = int(db.execute("PRAGMA page_count;").fetchone()[0])
        # logger.debug(f'DB size is {pages * pagesize()}')
        after_prune = pony.count(a for a in StationAvailability)
        if after_prune < before_prune:
            logger.info(f'pruned {before_prune - after_prune} StationAvailability records')


class ReceivedPacket(DbEntity):  # type: ignore
    when = pony.Required(int, size=64)
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
        d = self.to_dict()
        d['when'] = to_datetime(self.when)
        return data.ReceivedPacket(**d)

    @classmethod
    @pony.db_session(strict=True)
    def prune(cls, before: datetime.datetime) -> None:
        try:
            before_prune = pony.count(r for r in ReceivedPacket)
            horizon = to_timestamp(before)
            # initial = int(db.execute("PRAGMA page_count;").fetchone()[0])
            pony.delete(p for p in ReceivedPacket if p.when < horizon)
            # after = int(db.execute("PRAGMA page_count;").fetchone()[0])
            # logger.debug(f'DB size was {initial * pagesize()}, now {after * pagesize()}')
            after_prune = pony.count(r for r in ReceivedPacket)
            if after_prune < before_prune:
                logger.info(f'pruned {before_prune - after_prune} ReceivedPacket records')
        except Exception as err:
            logger.error('cannot prune', exc_info=err)


# Not currently used
class FrequencyWatch(DbEntity):  # type: ignore
    sequence = pony.PrimaryKey(int, auto=True)
    started = pony.Required(int, size=64)
    ended = pony.Optional(int, size=64)
    station_id = pony.Required(int)
    frequency = pony.Required(int)
    is_targetted = pony.Required(bool)
    observer_id = pony.Optional(str)

    def as_local(self) -> data.FrequencyWatch:
        d = self.to_dict()
        d['started'] = to_datetime(self.started)
        d['ended'] = to_datetime_or_none(self.ended)
        return data.FrequencyWatch(**self.to_dict())

    @classmethod
    @pony.db_session(strict=True)
    def prune(cls, before: datetime.datetime) -> None:
        horizon = to_timestamp(before)
        pony.delete(p for p in cls if p.ended < horizon)


# after all entities have been created
db.generate_mapping(create_tables=True)


class NetworkUpdater(network.AbstractNetworkUpdater):
    def add(self, base: network.StationAvailability) -> bool:
        try:
            a = StationAvailability.__getitem__((base.station_id, base.stratum, to_timestamp(base.valid_at)))
        except pony.ObjectNotFound:
            StationAvailability(
                valid_at=to_timestamp(base.valid_at),
                valid_to=to_timestamp_or_none(base.valid_to),
                station_id=base.station_id,
                stratum=base.stratum,
                frequencies=base.frequencies,
                agent=base.agent,
                from_station=base.from_station,
            )
            return True
        else:
            if a.frequencies != base.frequencies:
                logger.info(f'{base.station_id} has updated frequencies? {a.frequencies} to {base.frequencies}')
            then = to_timestamp_or_none(base.valid_to)
            if a.valid_to != then:
                logger.info(f'{base.station_id} has updated valid_to {a.valid_to} to {to_timestamp} ({base.valid_to})')
                # a.valid_to = to_timestamp
        return False

    def updated(self, availabilities: Optional[Sequence[network.StationAvailability]] = None) -> None:
        db.commit()
        super().updated(availabilities)

    # wrap in a session for database access on delegated functions.
    @pony.db_session(strict=True)
    def on_hfdl(self, packet_info: hfdl.HFDLPacketInfo) -> None:
        try:
            super().on_hfdl(packet_info)
        except Exception as err:
            logger.error('HFDL processing', exc_info=err)

    # wrap in a session for database access on delegated functions.
    @pony.db_session(strict=True)
    def on_community(self, airframes: dict) -> None:
        try:
            super().on_community(airframes)
        except Exception as err:
            logger.error('community processing', exc_info=err)

    # wrap in a session for database access on delegated functions.
    @pony.db_session(strict=True)
    def on_systable(self, station_table: str) -> None:
        super().on_systable(station_table)

    def _for_station(
        self, station_id: int, at: Optional[datetime.datetime] = None
    ) -> Optional[network.StationAvailability]:
        when = to_timestamp(at or util.now())
        q = pony.select(
            a for a in StationAvailability
            if a.station_id == station_id
        )
        q = q.where(lambda a: a.valid_at <= when)
        q = q.where(lambda a: a.valid_to is None or a.valid_to >= when)
        q = q.sort_by(pony.desc(StationAvailability.stratum), pony.desc(StationAvailability.valid_at))
        row = q.first()
        # if not row:
        #     logger.info(f'No availability for {station_id} at {when} ({at})... {row}')
        return row.as_local() if row else None

    @pony.db_session(strict=True)
    def active(self, at: Optional[datetime.datetime] = None) -> Sequence[network.StationAvailability]:
        found = []
        sids = network.STATIONS.by_id.keys()
        for sid in sids:
            la = self._for_station(sid, at=at)
            if la:
                found.append(la)
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
            when=to_timestamp(util.now()),
            agent=packet_info.station,
            ground_station=packet_info.ground_station['id'],
            frequency=packet_info.frequency,
            kind='spdu' if packet_info.is_squitter else 'lpdu',
            uplink=packet_info.is_uplink,
            latitude=position[0],
            longitude=position[1],
        )
        db.commit()

    def recent_packets(cls, at: datetime.datetime) -> Sequence[ReceivedPacket]:
        when = to_timestamp(at)
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
        when = util.now() - datetime.timedelta(seconds=total_seconds)
        for packet in cls.recent_packets(when):
            bin_number = int((when - to_datetime(packet.when)).total_seconds() // bin_size)
            data[packet.frequency][bin_number] += 1
        return data

    @pony.db_session(strict=True)
    def packets_by_agent(cls, bin_size: int, num_bins: int) -> Mapping[str, Sequence[int]]:
        data: dict[str, list[int]] = collections.defaultdict(lambda: [0] * num_bins)
        total_seconds = bin_size * num_bins
        when = util.now() - datetime.timedelta(seconds=total_seconds)
        for packet in cls.recent_packets(when):
            bin_number = int((when - to_datetime(packet.when)).total_seconds() // bin_size)
            data[packet.agent or 'unknown'][bin_number] += 1
        return data

    @pony.db_session(strict=True)
    def packets_by_station(cls, bin_size: int, num_bins: int) -> Mapping[int, Sequence[int]]:
        data: dict[int, list[int]] = collections.defaultdict(lambda: [0] * num_bins)
        total_seconds = bin_size * num_bins
        when = util.now() - datetime.timedelta(seconds=total_seconds)
        for packet in cls.recent_packets(when):
            bin_number = int((when - to_datetime(packet.when)).total_seconds() // bin_size)
            station = network.STATIONS[packet.frequency]
            data[station.station_id][bin_number] += 1
        return data

    @pony.db_session(strict=True)
    def packets_by_band(cls, bin_size: int, num_bins: int) -> Mapping[int, Sequence[int]]:
        data: dict[int, list[int]] = collections.defaultdict(lambda: [0] * num_bins)
        total_seconds = bin_size * num_bins
        when = util.now() - datetime.timedelta(seconds=total_seconds)
        for packet in cls.recent_packets(when):
            bin_number = int((when - to_datetime(packet.when)).total_seconds() // bin_size)
            data[packet.frequency // 1000][bin_number] += 1
        return data

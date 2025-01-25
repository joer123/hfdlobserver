# hfdl_observer/manage.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import datetime
import functools
import json
import logging

from typing import Any, Callable, Coroutine, Optional, Union

import hfdl_observer.bus
import hfdl_observer.env
import hfdl_observer.data
import hfdl_observer.groundstation
import hfdl_observer.hfdl


logger = logging.getLogger(__name__)


class ActiveGroundStations(hfdl_observer.groundstation.GroundStationStatus):
    last_state: dict
    hfdl_watchers: list[hfdl_observer.groundstation.GroundStationTable]
    startables: list[Callable[[], Coroutine[Any, Any, None]]]
    tasks: list[asyncio.Task]
    systable: Optional[hfdl_observer.groundstation.SystemTable] = None

    def __init__(self, config: dict):
        super().__init__()
        self.will_save = False
        self.config = config
        self.save_path = hfdl_observer.env.as_path(config['state'])
        self.hfdl_watchers = []
        self.tasks = []
        self.startables = []
        self.self_squitter_table = hfdl_observer.groundstation.SelfSquitterTable()
        self.squitter_table = hfdl_observer.groundstation.SquitterTable()
        self.update_table = hfdl_observer.groundstation.UpdateTable()
        for table in [self.self_squitter_table, self.squitter_table, self.update_table]:
            self.hfdl_watchers.append(table)
            self.add_table(table)
            # table.subscribe('event', self.on_event)
        for ix, url_source in enumerate(config.get('station_updates', [])):
            if not isinstance(url_source, dict):
                url_source = {
                    'url': url_source
                }
            table = hfdl_observer.groundstation.AirframesStationTable()
            url_watcher = hfdl_observer.bus.RemoteURLRefresher(
                url_source['url'],
                period=url_source.get('period', 60 + ix)
            )
            url_watcher.subscribe('response', table.update)
            self.startables.append(url_watcher.run)
            self.add_table(table)
        if self.save_path:
            try:
                previous = json.loads(self.save_path.read_text())
            except (json.JSONDecodeError, IOError):
                pass
            else:
                logger.debug("loading previous state")
                previous_table = hfdl_observer.groundstation.AirframesStationTable()
                previous_table.update(previous)
                self.add_table(previous_table)
        for file_source in [hfdl_observer.env.as_path(p) for p in config.get('station_files', [])]:
            table = hfdl_observer.groundstation.SystemTable()
            file_watcher = hfdl_observer.bus.FileRefresher(file_source, period=3600)
            assert file_source.exists(), f'{file_source} does not exist'
            file_watcher.subscribe('text', table.update)
            self.startables.append(file_watcher.run)
            self.add_table(table)
            self.systable = table
        self.subscribe('update', self.schedule_save)
        self.last_state = dict()

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        for startable in self.startables:
            self.tasks.append(loop.create_task(startable()))
        # loop.create_task(url_watcher.run())
        # loop.create_task(file_watcher.run())

    def stop(self) -> None:
        for task in self.tasks:
            task.cancel()

    def on_hfdl(self, packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        for watcher in self.hfdl_watchers:
            watcher.update(packet)

    @property
    def active_station_frequencies(self) -> dict[int, list[int]]:
        return {sid: self.active_frequencies(sid) for sid in self.station_ids}

    @property
    def inactive_station_frequencies(self) -> dict[int, list[int]]:
        active = self.active_station_frequencies
        inactive: dict[int, list[int]] = {}
        if self.systable:
            for sid, alloc in active.items():
                ext_freqs = inactive.setdefault(sid, [])
                sys_station = self.systable[sid]
                for f in sys_station.frequencies():
                    if f.khz not in alloc:
                        ext_freqs.append(f.khz)
        return inactive

    def active_station_data(self, station_key: Union[int, str]) -> dict:
        data: dict[str, Any] = {
            'last_updated': 0,
        }
        station = self[station_key]
        data['id'] = station.id
        name = station.name or hfdl_observer.hfdl.STATIONS.get(station.id).get('name')
        if name is None:
            # hacky way to backfill. Should never be needed?
            station_data = self.cached_station_strata[station.id]
            for station_stratum in station_data.values():
                for node in station_stratum:
                    if node.name is not None:
                        name = node.name
                        break
        data['name'] = name
        data['last_updated'] = station.last_updated
        data['when'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        data['frequencies'] = {
            'active': sorted(self.active_frequencies(station_key)),
            'stratum': station.stratum.value,
            'update_source': station.update_source,
        }
        # for lookup in self.tables:
        #     try:
        #         station = lookup[station_key]
        #     except KeyError:
        #         continue
        #     data['id'] = station.id
        #     data['name'] = station.name
        #     if data['last_updated'] < station.last_updated:
        #         data['last_updated'] = station.last_updated
        #         data['when'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # data['frequencies'] = {
        #     'active': sorted(self.active_frequencies(station_key)),
        #     'stratum': station.stratum.value,
        #     'update_source': station.update_source,
        # }
        return data

    def schedule_save(self, _: Any) -> None:
        if not self.will_save:
            self.will_save = True
            asyncio.get_running_loop().call_later(self.config['save_delay'], self.save)

    def save(self) -> None:
        logger.debug('ground stations frequencies')
        self.publish('frequencies', self.active_station_frequencies)
        if self.save_path:
            data: dict = {
                'ground_stations': list(self.active_station_data(k) for k in sorted(self.station_ids)),
            }
            self.will_save = False
            when = datetime.datetime.now(datetime.timezone.utc).isoformat()
            if data != self.last_state:
                logger.info('saving station data')
                self.last_state = data.copy()
                data['when'] = when
                self.save_path.write_text(json.dumps(data, indent=4) + '\n')


class ReceiverProxy(hfdl_observer.bus.Publisher):
    name: str
    allocation: Optional[hfdl_observer.data.Allocation]

    def __init__(self, name: str, sample_rate: int, other: hfdl_observer.bus.Publisher):
        super().__init__()
        self.name = name
        self.sample_rate = sample_rate
        self.allocation = None
        other.subscribe(f'receiver:{self.name}', self.on_remote_event)

    def connect(self, queue: hfdl_observer.bus.Publisher) -> None:
        queue.subscribe(f'receiver:{self.name}', self.on_local_event)

    def send(self, *params: Any) -> None:
        self.publish(f'receiver:{self.name}', tuple(params))

    def covers(self, allocation: hfdl_observer.data.Allocation) -> bool:
        for x in allocation.frequencies:
            if not self.allocation or x not in self.allocation.frequencies:
                return False
        return True
        # return all(x in self.frequencies for x in allocation.frequencies)

    def on_local_event(self, data: tuple[str, Any]) -> None:
        action, arg = data
        if action == 'listen':
            self.send(data)
            # self.publish(f'receiver:{self.name}', data)

    def on_remote_event(self, data: tuple[str, Any]) -> None:
        action, arg = data
        if action == 'listening':
            self.allocation = hfdl_observer.data.Allocation(self.sample_rate, arg)
            logger.debug(f'{self} updated from remote')
            # self.publish(f'receiver:{name}', data)

    def __str__(self) -> str:
        return f'{self.name} on {self.allocation}'

    def die(self) -> None:
        self.send('die')

    def listen(self, freqs: list[int]) -> None:
        self.send('listen', freqs)


class SimpleConductor(hfdl_observer.bus.Publisher):  # proxyPublisher
    ranked_station_ids: list[int]
    ignored_frequencies: list[tuple[int, int]]
    allowed_allocation_width: int
    proxies: list[ReceiverProxy]

    def __init__(self, config: dict) -> None:
        super().__init__()
        self.config = config
        self.ranked_station_ids = config['ranked_stations']
        ignores = config.get('ignored_frequencies', [])
        self.ignored_frequencies = ignored = []
        for ignore in ignores:
            if ignore:
                if isinstance(ignore, list):
                    ignored.append(tuple((ignore + ignore[-1:])[:2]))
                else:
                    ignored.append((ignore, ignore))
        self.proxies = []
        self.reaper = Reaper()
        self.reaper.subscribe('dead-receiver', self.on_dead_receiver)

    @property
    def parameters(self) -> hfdl_observer.data.Parameters:
        params = hfdl_observer.data.Parameters()
        params.max_sample_rate = self.config['slot_width']
        # params.num_clients = self.config['max_slots']
        return params

    def add_receiver(self, receiver: ReceiverProxy) -> None:
        self.proxies.append(receiver)

    def is_ignored(self, frequency: int) -> bool:
        for ignore in self.ignored_frequencies:
            if ignore[0] <= frequency <= ignore[1]:
                return True
        return False

    def allocate_frequencies(
        self,
        station_frequencies: dict[int, list[int]],
        base_allocations: Optional[list[hfdl_observer.data.Allocation]] = None
    ) -> list[hfdl_observer.data.Allocation]:
        allocations: list[hfdl_observer.data.Allocation] = []
        for a in base_allocations or []:
            allocations.append(self.parameters.allocation(a.frequencies))
        for sid in self.ranked_station_ids:
            for frequency in sorted(station_frequencies.get(sid, [])):
                if self.is_ignored(frequency):
                    continue
                for allocation in allocations:
                    if allocation.maybe_add(frequency):
                        break
                else:
                    allocations.append(self.parameters.allocation([frequency]))
        return allocations

    def orchestrate(
        self,
        allocations: list[hfdl_observer.data.Allocation],
        extended_allocations: list[hfdl_observer.data.Allocation]
    ) -> tuple[list[hfdl_observer.data.Allocation], list[hfdl_observer.data.Allocation]]:
        keeps = {}
        starts = []
        all_freq_count = sum(len(a.frequencies) for a in allocations)
        core_listening_count = 0
        extended_listening_count = 0

        desired_core_allocations = allocations[:len(self.proxies)]
        desired_extended_allocations = extended_allocations[:len(self.proxies)]

        for core_allocation, extended_allocation in zip(desired_core_allocations, desired_extended_allocations):
            # print(allocation)
            for receiver in self.proxies:
                if receiver.covers(extended_allocation):
                    keeps[receiver.name] = (core_allocation, extended_allocation)
                    break
            else:
                starts.append((core_allocation, extended_allocation))

        available = []
        for receiver in self.proxies:
            if receiver.name in keeps:
                core_allocation, extended_allocation = keeps[receiver.name]
                if core_allocation and core_allocation.frequencies:  # by this point sb True, but mypy
                    core_listening_count += len(core_allocation.frequencies)
                    extended_listening_count += len(extended_allocation.frequencies)
                logger.debug(f'keeping {receiver}')
            else:
                available.append(receiver)

        for data, receiver in zip(starts, available, strict=False):
            core_allocation, extended_allocation = data
            core_listening_count += len(core_allocation.frequencies)
            extended_listening_count += len(extended_allocation.frequencies)
            if receiver.allocation:
                self.reaper.remove_allocation(receiver.allocation)
                receiver_freqs = receiver.allocation.frequencies
            else:
                receiver_freqs = []
            receiver.listen(extended_allocation.frequencies)
            logger.info(f'assigned {extended_allocation.frequencies} to {receiver.name} (was {receiver_freqs})')
            self.reaper.add_allocation(extended_allocation)

        diff = extended_listening_count - core_listening_count
        logger.info(f'Listening to {core_listening_count} of {all_freq_count} active frequencies (+{diff} extra)')
        return desired_core_allocations, desired_extended_allocations

    def on_dead_receiver(self, frequencies: list[int]) -> None:
        for receiver in self.proxies:
            if receiver.allocation and receiver.allocation.frequencies == frequencies:
                receiver.die()


REAPER_HORIZON = 3600


class Reaper(hfdl_observer.bus.Publisher):
    allocations: dict[int, hfdl_observer.data.Allocation]
    last_seen: dict[int, int]

    def __init__(self) -> None:
        super().__init__()
        self.allocations = {}
        self.last_seen = {}

    async def run(self) -> None:
        while True:
            await asyncio.sleep(59)

    @functools.cached_property
    def task(self) -> asyncio.Task:
        return asyncio.get_running_loop().create_task(self.run())

    def start(self) -> asyncio.Task:
        return self.task

    def on_hfdl(self, packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        frequency = packet.frequency
        self.last_seen[frequency] = max(packet.timestamp, self.last_seen.get(frequency, 0))

    def add_allocation(self, allocation: hfdl_observer.data.Allocation) -> None:
        logger.info(f'reaper adding allocation {allocation}')
        for freq in allocation.frequencies:
            self.allocations[freq] = allocation

    def remove_allocation(self, allocation: hfdl_observer.data.Allocation) -> None:
        logger.info(f'reaper removing allocation {allocation}')
        for freq in allocation.frequencies:
            if freq in self.allocations:
                del self.allocations[freq]
            if freq in self.last_seen:
                del self.last_seen[freq]

    def check(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        horizon = now - REAPER_HORIZON
        for freq, allocation in self.allocations.items():
            if 0 < self.last_seen.get(freq, 0) < horizon:
                self.publish('dead-receiver', allocation.frequencies)

# hfdl_observer/manage.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import datetime
import functools
import itertools
import json
import logging

from typing import Any, Callable, Coroutine, Optional

import hfdl_observer.bus
import hfdl_observer.env
import hfdl_observer.data
import hfdl_observer.hfdl
import hfdl_observer.network
import hfdl_observer.util


logger = logging.getLogger(__name__)


class NetworkOverview(hfdl_observer.bus.Publisher):
    last_state: dict
    startables: list[Callable[[], Coroutine[Any, Any, None]]]
    tasks: list[asyncio.Task]

    def __init__(self, config: dict, updater: hfdl_observer.network.AbstractNetworkUpdater):
        super().__init__()
        self.last_state = {}
        self.config = config
        self.updater = updater
        self.save_path = hfdl_observer.env.as_path(config['state'])
        self.tasks = []
        self.startables = []
        for file_source in [hfdl_observer.env.as_path(p) for p in config.get('station_files', [])]:
            file_watcher = hfdl_observer.bus.FileRefresher(file_source, period=3600)
            assert file_source.exists(), f'{file_source} does not exist'
            # prime this pump... shouldn't be necessary, but.
            updater.on_systable(file_source.read_text())
            file_watcher.subscribe('text', updater.on_systable)
            self.startables.append(file_watcher.run)
        if self.save_path:
            try:
                previous = json.loads(self.save_path.read_text())
            except (json.JSONDecodeError, IOError):
                pass
            else:
                logger.debug("loading previous state")
                updater.on_community(previous)
        for ix, url_source in enumerate(config.get('station_updates', [])):
            if not isinstance(url_source, dict):
                url_source = {
                    'url': url_source
                }
            url_watcher = hfdl_observer.bus.RemoteURLRefresher(
                url_source['url'],
                period=url_source.get('period', 60 + ix)
            )
            url_watcher.subscribe('response', updater.on_community)
            self.startables.append(url_watcher.run)
        self.will_save = False
        updater.subscribe('availability', self.schedule_save)

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        for startable in self.startables:
            self.tasks.append(loop.create_task(startable()))
        # loop.create_task(url_watcher.run())
        # loop.create_task(file_watcher.run())

    def stop(self) -> None:
        for task in self.tasks:
            task.cancel()

    def schedule_save(self, _: Any) -> None:
        if not self.will_save:
            self.will_save = True
            asyncio.get_running_loop().call_later(self.config['save_delay'], self.save)
        self.publish('frequencies', {s.station_id: s.frequencies for s in self.updater.current()})

    def save(self) -> None:
        logger.debug('saving ground stations frequencies')
        current = self.updater.current()
        if self.save_path:
            self.will_save = False
            out = []
            when = datetime.datetime.now(datetime.timezone.utc).isoformat()
            for station in current:
                sd = {
                    'id': station.station_id,
                    'last_updated': hfdl_observer.util.datetime_to_timestamp(station.valid_at),
                    'name': hfdl_observer.network.STATIONS[station.station_id].station_name,
                    'when': station.valid_at.astimezone(datetime.timezone.utc).isoformat(),
                    'stratum': station.stratum,
                    'update_source': station.from_station or station.agent,
                    'frequencies': {'active': station.frequencies}
                }
                out.append(sd)

            data: dict = {
                'ground_stations': out
            }
            if data != self.last_state:
                logger.info('saving station data')
                self.last_state = data.copy()
                data['when'] = when
                self.save_path.write_text(json.dumps(data, indent=4) + '\n')
            self.publish('state', data)  # maybe should be a deepcopy


class ReceiverProxy(hfdl_observer.bus.Publisher):
    name: str
    channel: Optional[hfdl_observer.data.ObservingChannel]

    def __init__(self, name: str, sample_rate: int, other: hfdl_observer.bus.Publisher):
        super().__init__()
        self.name = name
        self.sample_rate = sample_rate
        self.channel = None
        other.subscribe(f'receiver:{self.name}', self.on_remote_event)

    def connect(self, queue: hfdl_observer.bus.Publisher) -> None:
        queue.subscribe(f'receiver:{self.name}', self.on_local_event)

    def send(self, *params: Any) -> None:
        self.publish(f'receiver:{self.name}', tuple(params))

    def covers(self, channel: hfdl_observer.data.ObservingChannel) -> bool:
        for x in channel.frequencies:
            if not self.channel or x not in self.channel.frequencies:
                return False
        return True
        # return all(x in self.frequencies for x in channel.frequencies)

    def on_local_event(self, data: tuple[str, Any]) -> None:
        action, arg = data
        if action == 'listen':
            self.send(data)
            # self.publish(f'receiver:{self.name}', data)

    def on_remote_event(self, data: tuple[str, Any]) -> None:
        action, arg = data
        if action == 'listening':
            self.channel = hfdl_observer.data.ObservingChannel(self.sample_rate, arg)
            logger.debug(f'{self} updated from remote')
            # self.publish(f'receiver:{name}', data)

    def __str__(self) -> str:
        return f'{self.name} on {self.channel}'

    def die(self) -> None:
        self.send('die')

    def listen(self, freqs: list[int]) -> None:
        self.send('listen', freqs)


class SimpleConductor(hfdl_observer.bus.Publisher):  # proxyPublisher
    ranked_station_ids: list[int]
    ignored_frequencies: list[tuple[int, int]]
    allowed_channel_width: int
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
    def parameters(self) -> hfdl_observer.data.ObserverParameters:
        params = hfdl_observer.data.ObserverParameters()
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

    def channels(
        self,
        station_frequencies: dict[int, list[int]],
        original_channels: Optional[list[hfdl_observer.data.ObservingChannel]] = None,
    ) -> list[hfdl_observer.data.ObservingChannel]:
        channels: list[hfdl_observer.data.ObservingChannel] = [c.clone() for c in original_channels or []]
        for sid in self.ranked_station_ids:
            for frequency in sorted(station_frequencies.get(sid, [])):
                if self.is_ignored(frequency):
                    continue
                for channel in channels:
                    if channel.maybe_add(frequency):
                        break
                else:
                    channels.append(self.parameters.channel([frequency]))
        return channels

    def orchestrate(
        self,
        channels: list[hfdl_observer.data.ObservingChannel]
    ) -> list[hfdl_observer.data.ObservingChannel]:
        keeps = {}
        starts = []
        possible_frequencies = list(itertools.chain(*[c.frequencies for c in channels]))
        active_frequencies = [f for f in possible_frequencies if hfdl_observer.network.STATIONS.is_active(f)]

        chosen_channels = channels[:len(self.proxies)]
        targetted_frequencies = []
        untargetted_frequencies = []
        chosen_frequencies = []
        for channel in chosen_channels:
            logger.info(f'considering {channel}')
            for frequency in channel.frequencies:
                station = hfdl_observer.network.STATIONS[frequency]
                # this calculation is a bit premature, but should be okay. Could also be done for keeps and starts
                # separately.
                chosen_frequencies.append(frequency)
                if station.is_assigned(frequency):
                    if station.is_active(frequency):
                        targetted_frequencies.append(frequency)
                    else:
                        untargetted_frequencies.append(frequency)
            for receiver in self.proxies:
                if receiver.covers(channel):
                    keeps[receiver.name] = channel
                    break
            else:
                logger.info(f'adding {channel} to starts')
                starts.append(channel)

        available = []
        for receiver in self.proxies:
            if receiver.name in keeps:
                channel = keeps[receiver.name]
                logger.debug(f'keeping {receiver}')
            else:
                logger.info(f'marking {receiver} available')
                available.append(receiver)

        for channel, receiver in zip(starts, available, strict=False):
            if receiver.channel:
                self.reaper.remove_channel(receiver.channel)
                receiver_freqs = receiver.channel.frequencies
            else:
                receiver_freqs = []
            receiver.listen(channel.frequencies)
            logger.info(f'assigned {channel.frequencies} to {receiver.name} (was {receiver_freqs})')
            self.reaper.add_channel(channel)

        targetted_count = len(targetted_frequencies)
        untargetted_count = len(untargetted_frequencies)
        active_count = len(active_frequencies)
        extra = untargetted_count
        logger.info(f'Listening to {targetted_count} of {active_count} active frequencies (+{extra} extra).')
        return chosen_channels

    def on_dead_receiver(self, frequencies: list[int]) -> None:
        for receiver in self.proxies:
            if receiver.channel and receiver.channel.frequencies == frequencies:
                receiver.die()


REAPER_HORIZON = 3600


class Reaper(hfdl_observer.bus.Publisher):
    channels: dict[int, hfdl_observer.data.ObservingChannel]
    last_seen: dict[int, int]

    def __init__(self) -> None:
        super().__init__()
        self.channels = {}
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

    def add_channel(self, channel: hfdl_observer.data.ObservingChannel) -> None:
        logger.info(f'reaper adding channel {channel}')
        for freq in channel.frequencies:
            self.channels[freq] = channel

    def remove_channel(self, channel: hfdl_observer.data.ObservingChannel) -> None:
        logger.info(f'reaper removing channel {channel}')
        for freq in channel.frequencies:
            if freq in self.channels:
                del self.channels[freq]
            if freq in self.last_seen:
                del self.last_seen[freq]

    def check(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        horizon = now - REAPER_HORIZON
        for freq, channel in self.channels.items():
            if 0 < self.last_seen.get(freq, 0) < horizon:
                self.publish('dead-receiver', channel.frequencies)

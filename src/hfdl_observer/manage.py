# hfdl_observer/manage.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import collections.abc
import datetime
import functools
import itertools
import json
import logging
import uuid

from typing import Any, Awaitable, Callable, Coroutine, Optional

import hfdl_observer.bus as bus
import hfdl_observer.env
import hfdl_observer.data as data
import hfdl_observer.hfdl
import hfdl_observer.network as network
import hfdl_observer.util as util


logger = logging.getLogger(__name__)
EOL = '\n'


REAPER_HORIZON = 3600


class NetworkOverview(bus.LocalPublisher):
    last_state: dict
    startables: list[Callable[[], Coroutine[Any, Any, None]]]
    tasks: list[asyncio.Task]

    def __init__(self, config: dict, updater: network.AbstractNetworkUpdater):
        super().__init__()
        self.last_state = {}
        self.config = config
        self.updater = updater
        self.save_path = hfdl_observer.env.as_path(config['state'])
        self.tasks = []
        self.startables = []
        for file_source in [hfdl_observer.env.as_path(p) for p in config.get('station_files', [])]:
            file_watcher = bus.FileRefresher(file_source, period=3600)
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
            url_watcher = bus.RemoteURLRefresher(
                url_source['url'],
                period=url_source.get('period', 60 + ix)
            )
            url_watcher.subscribe('response', updater.on_community)
            self.startables.append(url_watcher.run)
        self.will_save = False
        updater.subscribe('availability', self.schedule_save)

    def start(self) -> None:
        logger.info('starting network overview')
        for startable in self.startables:
            self.tasks.append(util.schedule(startable()))

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()

    def schedule_save(self, _: Any) -> None:
        if not self.will_save:
            self.will_save = True
            util.call_later(self.config['save_delay'], self.save)
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
                    'last_updated': util.datetime_to_timestamp(station.valid_at),
                    'name': network.STATIONS[station.station_id].station_name,
                    'when': station.valid_at.astimezone(datetime.timezone.utc).isoformat(),
                    'stratum': station.stratum,
                    'update_source': station.from_station or station.agent,
                    'frequencies': {'active': station.frequencies}
                }
                out.append(sd)

            payload: dict = {
                'ground_stations': out
            }
            if payload != self.last_state:
                logger.info('saving station data')
                self.last_state = payload.copy()
                payload['when'] = when
                self.save_path.write_text(json.dumps(payload, indent=4) + '\n')
            self.publish('state', payload)  # maybe should be a deepcopy


class ReceiverProxy(data.ChannelObserver, bus.GenericRemoteEventDispatcher):
    name: str
    uuid: str
    channel: Optional[data.ObservingChannel]
    last_seen: datetime.datetime
    pings_sent: int = 0
    weight: int = data.DEFAULT_RECEIVER_WEIGHT
    direct_subscriber: bus.RemoteSubscriber | None = None

    def __init__(
        self, name: str, uuid: str, observable_widths: list[int], weight: int = data.DEFAULT_RECEIVER_WEIGHT
    ) -> None:
        self.name = name
        self.uuid = uuid
        self.weight = weight
        self.observable_channel_widths = observable_widths
        self.channel = None
        self.last_seen = util.now()
        self.direct_subscriber = bus.REMOTE_BROKER.subscriber(self.target)
        self.direct_subscriber.add_callback(self.on_remote_event)
        self.direct_subscriber.start()

    @functools.cached_property
    def target(self) -> str:
        return f'@receiver+{self.name}'

    def relay(self, subject: str, payload: Any) -> None:
        bus.REMOTE_BROKER.publish(bus.Message(self.target, subject, payload))

    def keepalive(self) -> None:
        self.pings_sent = 0
        self.last_seen = util.now()

    def covers(self, channel: data.ObservingChannel) -> bool:
        return self.channel is not None and self.channel.matches(channel)

    def on_remote_listening(self, message: bus.Message) -> None:
        payload: dict = message.payload
        frequencies: list[int] = payload["frequencies"]
        if self.uuid == payload['uuid']:
            logger.info(f'{self} (remote) now listening to {len(frequencies)} frequencies')
            self.keepalive()
            self.channel = self.observing_channel_for(frequencies)
            for frequency in frequencies or []:
                network.set_receiver_for_frequency(frequency, self.name)
        else:
            logger.info(f'{self} bad notification. ({payload["uuid"]}) {len(frequencies)} frequencies #{self.pings_sent}')
            del payload['frequencies']
            bus.REMOTE_BROKER.publish(bus.Message(self.target, "deregister", payload))
            if self.direct_subscriber is not None:
                self.direct_subscriber._stop()
                self.direct_subscriber = None
            self.pings_sent += 1  # penalize me.

    def on_remote_pong(self, message: bus.Message) -> None:
        if self.uuid == message.payload.get('src'):
            self.keepalive()

    def __str__(self) -> str:
        return f'{self.name}/{self.uuid} on {self.channel} ({self.last_seen})'

    def die(self) -> None:
        self.relay('die', self.uuid)

    def listen(self, freqs: list[int]) -> None:
        self.pings_sent += 1
        self.relay('listen', freqs)

    def ping(self, from_uuid: str) -> None:
        self.pings_sent += 1
        self.relay('ping', {'dst': self.uuid, 'src': from_uuid})

    def observable_widths(self) -> list[int]:
        return self.observable_channel_widths

    def registered(self) -> None:
        self.relay('registered', self.uuid)

    def deregistered(self) -> None:
        self.relay('deregistered', self.uuid)

    def recently_alive(self) -> bool:
        return self.pings_sent <= 3

    def describe(self) -> str:
        if self.channel and self.channel.frequencies:
            middle = f' @{(self.channel.center_khz)}'
            freqs = len(self.channel.frequencies)
        else:
            middle = ''
            freqs = 0
        return f'{self.name} via {self.uuid} on {freqs} freqs{middle}'


class AbstractOrchestrator(bus.LocalPublisher, data.ChannelObserver):
    ranked_station_ids: list[int]
    ignored_frequencies: list[tuple[int, int]]
    proxies: list[ReceiverProxy]

    def __init__(self, config: dict) -> None:
        super().__init__()
        self.config = config
        self.ranked_station_ids = config['ranked_stations']
        ignores = config.get('ignored_frequencies', [])
        self.ignored_frequencies = util.normalize_ranges(ignores)
        self.proxies = []
        self.reaper = Reaper()
        self.reaper.subscribe('dead-receiver', self.on_dead_receiver)

    def add_receiver(self, receiver: ReceiverProxy) -> None:
        self.proxies.append(receiver)

    def remove_receiver(self, receiver: ReceiverProxy) -> None:
        if receiver in self.proxies:
            self.proxies.remove(receiver)

    def is_ignored(self, frequency: int) -> bool:
        for ignore in self.ignored_frequencies:
            if ignore[0] <= frequency <= ignore[1]:
                return True
        return False

    def observable_widths(self) -> list[int]:
        raise NotImplementedError(str(self.__class__))

    def on_dead_receiver(self, frequencies: list[int]) -> None:
        for receiver in self.proxies:
            if receiver.channel and receiver.channel.frequencies == frequencies:
                receiver.die()


class UniformOrchestrator(AbstractOrchestrator):
    def observable_widths(self) -> list[int]:
        widths: set[int] = set()
        for proxy in self.proxies:
            proxy_widths = set(proxy.observable_widths())
            if not widths:
                widths = proxy_widths
            elif widths != proxy_widths:
                raise ValueError('only uniform channel widths is currently supported')
        return list(widths)

    def pick_channels(
        self,
        station_frequencies: dict[int, list[int]],
        original_channels: Optional[list[data.ObservingChannel]] = None,
    ) -> list[data.ObservingChannel]:
        channels: list[data.ObservingChannel] = [c.clone() for c in original_channels or []]
        for sid in self.ranked_station_ids:
            for frequency in sorted(station_frequencies.get(sid, [])):
                if self.is_ignored(frequency):
                    continue
                for channel in channels:
                    if channel.maybe_add(frequency):
                        break
                else:
                    channels.append(self.observing_channel_for([frequency]))
        return channels

    def merge_channels(self, channels: list[data.ObservingChannel]) -> list[data.ObservingChannel]:
        if not self.proxies:
            return []

        chosen_channels = channels[:len(self.proxies)]
        targetted_frequencies = []
        untargetted_frequencies = []
        chosen_frequencies = []
        for channel in chosen_channels:
            logger.debug(f'considering {channel}')
            for frequency in channel.frequencies:
                station = network.STATIONS[frequency]
                chosen_frequencies.append(frequency)
                if station.is_assigned(frequency):
                    if station.is_active(frequency):
                        targetted_frequencies.append(frequency)
                    else:
                        untargetted_frequencies.append(frequency)

        targetted_count = len(targetted_frequencies)
        untargetted_count = len(untargetted_frequencies)
        possible_frequencies = list(itertools.chain(*[c.frequencies for c in channels]))
        active_count = len([f for f in possible_frequencies if network.STATIONS.is_active(f)])
        extra = untargetted_count
        logger.info(f'Listening to {targetted_count} of {active_count} active frequencies (+{extra} extra).')
        return chosen_channels

    def proxy_sort_key(self, proxy: ReceiverProxy) -> tuple[int, int]:
        return (proxy.weight, max(proxy.observable_widths()))

    def assign_channels(self, channels: list[data.ObservingChannel]) -> None:
        keeps = {}
        starts: list[data.ObservingChannel] = []
        ordered_proxies = sorted(self.proxies, key=self.proxy_sort_key, reverse=True)
        logger.debug(f'Receivers: {list(p.name for p in ordered_proxies)}')
        available = ordered_proxies.copy()
        for channel in channels:
            for receiver in ordered_proxies:
                if receiver.covers(channel):
                    logger.debug(f'keeping {receiver}')
                    keeps[receiver.name] = channel
                    if receiver in available:
                        available.remove(receiver)
                    break
            else:
                logger.debug(f'adding {channel} to starts')
                starts.append(channel)

        for channel in starts:
            for receiver in available:
                if max(receiver.observable_widths()) >= channel.width_hz:
                    if receiver.channel:
                        self.reaper.remove_channel(receiver.channel)
                        receiver_freqs = receiver.channel.frequencies
                    else:
                        receiver_freqs = []
                    receiver.listen(channel.frequencies)
                    logger.debug(f'assigned {channel.frequencies} to {receiver.name} (was {receiver_freqs})')
                    self.reaper.add_channel(channel)
                    available.remove(receiver)
                    break
            else:
                logger.warning(f'cannot allocate a receiver for {channel}')
                logger.info(f'channels\n{EOL.join(repr(c) for c in channels)}')
                logger.info(f'available left\n{EOL.join(str(a) for a in available)}')
                logger.info(f'keeps\n{EOL.join(str(a) for a in keeps.values())}')
                logger.info(f'starts\n{EOL.join(str(s) for s in starts)}')

    def orchestrate(self, targetted: dict[int, list[int]], fill_assigned: bool = False) -> list[data.ObservingChannel]:
        self.validate_proxies()
        if not self.proxies:
            return []
        channels = self.pick_channels(targetted)
        if fill_assigned:
            channels = self.pick_channels(network.STATIONS.assigned(), channels)
        actual_channels = self.merge_channels(channels)
        self.assign_channels(actual_channels)
        return actual_channels

    def validate_proxies(self) -> None:
        for proxy in self.proxies[:]:
            if not proxy.recently_alive():
                logger.warning(f'proxy {proxy.name}:{proxy} is dead. Removing.')
                self.remove_receiver(proxy)


class DiverseOrchestrator(UniformOrchestrator):
    def observable_widths(self) -> list[int]:
        width = 0
        for proxy in self.proxies:
            for proxy_width in proxy.observable_widths():
                if not width:
                    width = proxy_width
                else:
                    width = min(width, proxy_width)
        if not width:
            raise ValueError('conductor has no observable channel widths')
        return [width]

    def merge_channels(self, channels: list[data.ObservingChannel]) -> list[data.ObservingChannel]:
        if not self.proxies:
            return []
        targetted = []
        untargetted = []
        possible_frequencies = list(itertools.chain(*[c.frequencies for c in channels]))
        active_frequencies = [f for f in possible_frequencies if network.STATIONS.is_active(f)]

        actual_channels: list[data.ObservingChannel] = []
        for proxy in sorted(self.proxies, key=self.proxy_sort_key, reverse=True):
            actual_channels.append(data.ObservingChannel(max(proxy.observable_widths()), []))

        # merge the atomic channels into the possible proxy channels.
        for channel in channels:
            for proxy_channel in actual_channels:
                if proxy_channel.maybe_add_all(channel.frequencies):
                    targetted.extend([f for f in channel.frequencies if f in active_frequencies])
                    untargetted.extend([f for f in channel.frequencies if f not in active_frequencies])
                    break
            else:
                logger.debug(f'no proxy channel for {channel}, ignoring')

        targetted_count = len(targetted)
        untargetted_count = len(untargetted)
        assigned = set(itertools.chain.from_iterable(network.STATIONS.assigned().values()))
        active_count = len([f for f in assigned if network.STATIONS.is_active(f)])
        extra = untargetted_count
        logger.info(f'Listening to {targetted_count} of {active_count} active frequencies (+{extra} extra).')

        return actual_channels


class Reaper(bus.LocalPublisher):
    channels: dict[int, data.ObservingChannel]
    last_seen: dict[int, int]

    def __init__(self) -> None:
        super().__init__()
        self.channels = {}
        self.last_seen = {}

    async def run(self) -> None:
        while not util.is_shutting_down():
            await asyncio.sleep(59)

    @functools.cached_property
    def task(self) -> asyncio.Task:
        return util.schedule(self.run())

    def start(self) -> asyncio.Task:
        return self.task

    def on_hfdl(self, packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        frequency = packet.frequency
        self.last_seen[frequency] = max(packet.timestamp, self.last_seen.get(frequency, 0))

    def add_channel(self, channel: data.ObservingChannel) -> None:
        logger.debug(f'reaper adding channel {channel}')
        for freq in channel.frequencies:
            self.channels[freq] = channel

    def remove_channel(self, channel: data.ObservingChannel) -> None:
        logger.debug(f'reaper removing channel {channel}')
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


class ConductorNode(bus.LocalPublisher, bus.GenericRemoteEventDispatcher):
    proxies: dict[str, ReceiverProxy]
    orchestration_task: None | asyncio.Handle = None
    last_orchestrated: datetime.datetime
    listener_info: dict

    def __init__(self, config: collections.abc.Mapping) -> None:
        super().__init__()
        self.config = config
        self.uuid = f'@{uuid.uuid4()}'
        self.proxies = {}
        self.conductor = DiverseOrchestrator(config['conductor'])
        self.recipient_subscriber = self.message_broker().subscriber(self.uuid)
        self.recipient_subscriber.add_callback(self.on_remote_event)
        self.announcer = bus.PeriodicCallback(10, [self.announce], False)
        self.watchdog = bus.PeriodicCallback(30, [self.heartbeat], chatty=False)
        self.last_orchestrated = util.now()

    def message_broker(self) -> bus.RemoteBroker:
        raise NotImplementedError(self.__class__.__name__)

    def announce(self) -> None:
        self.message_broker().publish(bus.Message(
            '/observer',
            'available',
            {
                'name': self.uuid,
                'listener': self.listener_info
            }
        ))

    def add_receiver_proxy(self, proxy: ReceiverProxy) -> None:
        self.proxies[proxy.name] = proxy
        self.conductor.add_receiver(proxy)
        self.maybe_orchestrate()

    def maybe_orchestrate(self) -> None:
        delay = self.config.get('delay', 10)
        next_orchestrate = self.last_orchestrated + datetime.timedelta(seconds=delay)
        current_task = asyncio.current_task()
        if current_task and current_task == self.orchestration_task:
            self.orchestrate()
        elif self.orchestration_task is None:
            if next_orchestrate <= util.now():
                logger.debug('orchestrating soon')
                self.orchestration_task = util.call_soon(self.orchestrate)
            else:
                logger.debug(f'orchestrating in {delay}s')
                self.orchestration_task = util.call_later(delay, self.orchestrate)
        else:
            logger.debug(f'not orchestrating... {self.orchestration_task}')

    def orchestrate(self) -> None:
        if util.is_shutting_down():
            return
        self.last_orchestrated = util.now()
        self.orchestration_task = None
        targetted_freqs = network.STATIONS.active()
        all_active: list[int] = list(itertools.chain.from_iterable(targetted_freqs.values()))
        chosen_channels = self.conductor.orchestrate(targetted_freqs, fill_assigned=True)

        targetted = []
        untargetted = []
        for channel in chosen_channels:
            for frequency in channel.frequencies:
                if network.STATIONS.is_active(frequency):
                    targetted.append(frequency)
                else:
                    untargetted.append(frequency)

        self.publish('orchestrated', {
            'targetted': targetted,
            'untargetted': untargetted,
            'active': all_active,
        })

        self.publish('active', all_active)
        self.publish('observing', (targetted, untargetted))
        self.publish('frequencies', targetted_freqs)

    def outstanding_awaitables(self) -> list[Awaitable]:
        outstanding: list[Awaitable] = [
            self.announcer.stop(),
            self.watchdog.stop(),
            self.recipient_subscriber.stop(),
        ]
        return outstanding

    async def stop(self) -> None:
        logger.warning(f'{self} stopped')
        await asyncio.gather(*self.outstanding_awaitables(), return_exceptions=True)
        logger.info(f'{self} tasks halted')

    def heartbeat(self) -> None:
        horizon = util.now() - datetime.timedelta(seconds=100)
        for name, proxy in list(self.proxies.items()):
            if proxy.last_seen < horizon:
                if proxy.pings_sent > 3:
                    logger.warning(f'proxy {name}:{proxy} appears dead. Removing.')
                    self.deregister_remote(proxy.name, proxy.uuid)
                else:
                    proxy.ping(self.uuid)

    def start(self) -> None:
        self.recipient_subscriber.start()
        self.announcer.start()
        self.watchdog.start()
        # reaper is currently not used: self.conductor.reaper.start()

    def register_remote(self, name: str, uuid: str, widths: list[int], weight: int) -> None:
        try:
            old_proxy = self.proxies[name]
        except KeyError:
            pass
        else:
            if old_proxy.uuid != uuid:
                self.message_broker().publish(bus.Message(old_proxy.target, 'deregister', old_proxy.uuid))
                self.conductor.remove_receiver(old_proxy)
                del self.proxies[name]
        proxy = ReceiverProxy(name, uuid, widths, weight)
        self.add_receiver_proxy(proxy)
        proxy.registered()

    def deregister_remote(self, name: str, uuid: str) -> None:
        try:
            proxy = self.proxies[name]
        except KeyError:
            pass
        else:
            if proxy.uuid == uuid:
                self.conductor.remove_receiver(proxy)
                del self.proxies[name]
            proxy.deregistered()

    def on_remote_register(self, message: bus.Message) -> None:
        payload: dict = message.payload
        self.register_remote(
            payload['name'], payload['uuid'], payload['widths'], payload.get('weight', data.DEFAULT_RECEIVER_WEIGHT)
        )

    def on_remote_deregister(self, message: bus.Message) -> None:
        payload: dict = message.payload
        self.deregister_remote(payload['name'], payload['uuid'])

    def on_frequencies(self, targetted_freqs: dict[int, list[int]]) -> None:
        self.maybe_orchestrate()

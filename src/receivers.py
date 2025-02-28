# receivers.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import collections.abc
import datetime
import functools
import logging
import random
import uuid

from typing import Any, Coroutine, Optional

import hfdl_observer
import hfdl_observer.bus as bus
import hfdl_observer.data as data
import hfdl_observer.manage
import hfdl_observer.process
import hfdl_observer.util as util

import decoders
import iqsources
import settings


logger = logging.getLogger(__name__)


class ReceiverError(Exception):
    pass


class LocalReceiver(bus.LocalPublisher, data.ChannelObserver, bus.GenericRemoteEventDispatcher):
    frequencies: list[int]
    name: str
    tasks: list[asyncio.Task]
    conductor: Optional[str] = None
    last_seen: datetime.datetime
    registered: bool = False

    def __init__(self, name: str, config: collections.abc.MutableMapping) -> None:
        self.uuid = str(uuid.uuid4())
        self.config = config
        self.name = name
        self.logger = logger.getChild(self.name)
        self.frequencies = []
        self.tasks = []
        self.last_seen = util.now()
        self.recipient_subscriber = bus.REMOTE_BROKER.subscriber(self.target)
        self.recipient_subscriber.add_callback(self.on_remote_event)
        self.broadcast_subscriber = bus.REMOTE_BROKER.subscriber('/')
        self.broadcast_subscriber.add_callback(self.on_remote_event, lambda t: t.startswith('available'))
        self.broadcast_subscriber.add_callback(self.on_remote_event, lambda t: t.startswith('unavailable'))
        self.watchdog = bus.PeriodicCallback(60, [self.heartbeat], chatty=True)
        super().__init__()

    def payload(self, **data: Any) -> dict:
        _payload = {
            'name': self.name,
            'uuid': self.uuid,
        }
        _payload.update(data)
        return _payload

    @functools.cached_property
    def target(self) -> str:
        return f'@receiver+{self.name}'

    def on_remote_listen(self, payload: list[int]) -> None:
        self.keepalive()
        asyncio.get_running_loop().create_task(self.listen(payload))

    def on_remote_registered(self, uuid: str) -> None:
        self.keepalive()
        if uuid == self.uuid:
            self.registered = True

    def on_remote_deregistered(self, uuid: str) -> None:
        self.keepalive()
        if uuid == self.uuid:
            self.deregistered()

    def on_remote_ping(self, uuid: str) -> None:
        if uuid == self.uuid:
            self.keepalive()
            bus.REMOTE_BROKER.publish(self.target, 'pong', self.payload())

    def on_remote_die(self, _: Any) -> None:
        # self.keepalive()
        logger.warning(f'{self} received DIE order')
        self.deregister()

    def on_remote_available(self, payload: dict) -> None:
        self.keepalive()
        if not self.registered:
            self.conductor = payload['name']
            self.listener = data.ListenerConfig(payload['listener'])
            self.register()

    def on_remote_unavailable(self, conductor: str) -> None:
        self.deregister()

    def on_observer_start(self) -> None:
        self.recipient_subscriber.start()
        self.broadcast_subscriber.start()
        self.watchdog.start()

    def register(self) -> None:
        if self.conductor is not None:
            self.setup_harnesses()
            bus.REMOTE_BROKER.publish(self.conductor, 'register', self.payload(widths=self.observable_widths()))

    def deregister(self) -> None:
        if self.conductor:
            logger.info(f'{self} deregistering')
            bus.REMOTE_BROKER.publish(self.conductor, 'deregister', self.payload())
            task = asyncio.get_running_loop().create_task(self.stop())
            task.add_done_callback(self.deregistered)

    def deregistered(self, _: Any = None) -> None:
        self.registered = False
        self.conductor = None

    def setup_harnesses(self) -> None:
        raise NotImplementedError(str(self.__class__))

    def keepalive(self) -> None:
        self.last_seen = util.now()

    def heartbeat(self) -> None:
        # called regularly. Should check to see if the (controlling) observer has been seen lately.
        # If not... do something drastic.
        # only need to care if we're registered!
        if self.registered:
            horizon = util.now() - datetime.timedelta(seconds=60)
            if self.last_seen < horizon:
                logger.warning(f'controller {self.conductor} may be dead. {self.last_seen}')
                self.deregister()

    def covers(self, freqs: list[int]) -> bool:
        # in this implementation it must be exact.
        return set(freqs) == set(self.frequencies)

    async def kill(self) -> None:
        pass

    async def listen(self, frequencies: list[int]) -> None:
        if set(frequencies) == set(self.frequencies):
            self.logger.info(f'already listening to {frequencies}')
            bus.REMOTE_BROKER.publish(self.target, 'listening', self.payload(frequencies=frequencies))
            return
        self.logger.info(f'switching to {frequencies} from {self.frequencies}')
        await self.stop()
        self.frequencies = frequencies
        self.channel = self.observing_channel_for(frequencies)
        await self.start()
        self.logger.info(f'switched to {frequencies}')
        # bus.REMOTE_BROKER.publish(self.target, 'listening', self.payload(frequencies=frequencies))
        # self.publish(f'receiver:{self.name}', ('listening', self.channel.frequencies))

    async def start(self) -> None:
        self.task = asyncio.get_running_loop().create_task(self.run())

    async def stop(self) -> None:
        pass

    async def run(self) -> None:
        raise NotImplementedError(str(self.__class__))

    def on_task_done(self, task: asyncio.Task) -> None:
        exc = task.exception()
        if exc:
            self.publish('fatal', (str(self), str(exc)))
        if task in self.tasks:
            # we have not been asked to stop or kill, so this task has ended prematurely.
            self.tasks.remove(task)
            if not self.tasks:
                # there are no more tasks, so there's no valid channel
                self.frequencies = []
                self.channel = self.observing_channel_for([])
                bus.REMOTE_BROKER.publish(self.target, 'listening', self.payload(frequencies=self.frequencies))
                # self.publish(f'receiver:{self.name}', ('listening', self.frequencies))

    def __str__(self) -> str:
        return f'({self.__class__.__name__}) {self.name} on {self.frequencies}'


class Web888Receiver(LocalReceiver):
    shell: bool = False

    def __init__(self, name: str, config: collections.abc.MutableMapping) -> None:
        super().__init__(name, config)

    def observable_widths(self) -> list[int]:
        return [12]  # hardcoded to the value that kiwisdr uses.


class DummyReceiver(Web888Receiver):

    def setup_harnesses(self) -> None:
        self.client = iqsources.DummyClient(self.name, self.config.get('client', {}))
        self.decoder = decoders.DummyDecoder(self.name, self.config.get('decoder', {}), self.listener)

    async def run(self) -> None:
        bus.REMOTE_BROKER.publish(self.target, 'listening', self.payload(frequencies=self.channel.frequencies))
        # self.publish(f'receiver:{self.name}', ('listening', self.channel.frequencies))


class Web888ExecReceiver(Web888Receiver):
    client: iqsources.KiwiClientProcess
    decoder: decoders.IQDecoderProcess

    def setup_harnesses(self) -> None:
        self.client = iqsources.KiwiClientProcess(self.name, self.config.get('client', {}))
        self.decoder = decoders.IQDecoderProcess(self.name, self.config.get('decoder', {}), self.listener)

    async def run(self) -> None:
        bus.REMOTE_BROKER.publish(self.target, 'listening', self.payload(frequencies=self.channel.frequencies))
        # self.publish(f'receiver:{self.name}', ('listening', self.channel.frequencies))
        await asyncio.sleep(random.randrange(1, 20) / 10.0)   # thundering herd dispersal
        client_task = await self.client.listen(self.channel)
        async with self.client.running_condition:
            self.tasks.append(client_task)
            client_task.add_done_callback(self.on_task_done)
            await self.client.running_condition.wait()
            self.decoder.iq_fd = self.client.pipe.read
            decoder_task = await self.decoder.listen(self.channel)
            self.tasks.append(decoder_task)
            decoder_task.add_done_callback(self.on_task_done)

    async def stop(self) -> None:
        self.logger.debug('Stopping')
        self.tasks = []  # don't care about these tasks anymore
        await self.client.stop()
        await self.decoder.stop()

    async def kill(self) -> None:
        self.logger.debug('Killing')
        self.tasks = []  # don't care about these tasks anymore
        await self.client.kill()
        await self.decoder.kill()


class Web888PipeReceiver(Web888Receiver):
    client: iqsources.KiwiClient
    decoder: decoders.IQDecoder

    def setup_harnesses(self) -> None:
        self.client = iqsources.KiwiClient(self.name, self.config.get('client', {}))
        self.decoder = decoders.IQDecoder(self.name, self.config.get('decoder', {}), self.listener)
        self.receiver_pipe = ReceiverPipe(self.client.commandline() + ['|'] + self.decoder.commandline())

    async def run(self) -> None:
        await self.receiver_pipe.start()

    async def stop(self) -> None:
        self.logger.debug('Stopping')
        await self.receiver_pipe.stop()

    async def kill(self) -> None:
        self.logger.debug('Killing')
        await self.receiver_pipe.kill()


class ReceiverPipe(hfdl_observer.process.ProcessHarness):
    cmd: list[str]

    def __init__(self, cmd: list[str]) -> None:
        super().__init__()
        self.shell = True
        self.cmd = cmd
        # self.settle_time = ???
        # recoverable errors?
        # unrecoverable errors?

    def commandline(self) -> list[str]:
        return self.cmd


class DirectReceiver(LocalReceiver):
    observable_channel_widths: list[int]
    decoder: decoders.DirectDecoder

    def setup_harnesses(self) -> None:
        decoder_type = self.config['decoder']['type']
        decoder_class = getattr(decoders, decoder_type)
        self.decoder = decoder_class(self.name, self.config.get('decoder', {}), self.listener)
        if not isinstance(self.decoder, decoders.DirectDecoder):
            raise ValueError(f'{self.decoder} is not an expected Decoder')
        self.observable_channel_widths = self.decoder.observable_channel_widths()
        logger.info(f'observable channel widths {self.observable_channel_widths}')

    async def run(self) -> None:
        bus.REMOTE_BROKER.publish(self.target, 'listening', self.payload(frequencies=self.channel.frequencies))
        self.logger.info('queued')
        await asyncio.sleep(random.randrange(1, 20) / 10.0)   # thundering herd dispersal
        decoder_task = await self.decoder.listen(self.channel)
        self.tasks.append(decoder_task)
        decoder_task.add_done_callback(self.on_task_done)

    async def stop(self) -> None:
        self.logger.debug('Stopping')
        self.tasks = []  # don't care about these tasks anymore
        await self.decoder.stop()

    async def kill(self) -> None:
        self.logger.debug('Killing')
        self.tasks = []  # don't care about these tasks anymore
        if hasattr(self, 'decoder'):
            await self.decoder.kill()

    def observable_widths(self) -> list[int]:
        return self.observable_channel_widths


class ReceiverNode():
    local_receivers: list[LocalReceiver]

    def __init__(self, config: collections.abc.Mapping) -> None:
        self.config = config
        self.local_receivers = []

    def message_broker(self) -> bus.RemoteBroker:
        raise NotImplementedError(self.__class__.__name__)

    def build_local_receiver(self, receiver_name: str) -> LocalReceiver:
        receiver_base = self.config['all_receivers'][receiver_name]
        receiver_config = settings.flatten(receiver_base, 'receiver')
        typename = receiver_config['type']
        klass = globals()[typename]
        receiver: LocalReceiver = klass(receiver_name, receiver_config)
        receiver.subscribe('fatal', self.on_fatal_error)
        self.local_receivers.append(receiver)
        return receiver

    def start(self) -> None:
        logger.debug(f'starting {len(self.local_receivers)} local receivers')
        for receiver in self.local_receivers:
            receiver.on_observer_start()

    def killables(self) -> list[Coroutine]:
        outstanding = [receiver.kill() for receiver in self.local_receivers]
        return outstanding

    async def kill(self) -> None:
        logger.warning(f'{self} killed')
        await asyncio.gather(*self.killables())
        logger.info(f'{self} tasks halted')

    def on_fatal_error(self, _: Any) -> None:
        pass

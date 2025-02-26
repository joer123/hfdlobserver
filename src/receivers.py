# receivers.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import collections.abc
import functools
import logging
import random
import uuid

from typing import Any, Optional

import hfdl_observer
import hfdl_observer.bus as bus
import hfdl_observer.data as data
import hfdl_observer.manage
import hfdl_observer.process

import decoders
import iqsources


logger = logging.getLogger(__name__)


class ReceiverError(Exception):
    pass


class LocalReceiver(bus.LocalPublisher, data.ChannelObserver, bus.GenericRemoteEventDispatcher):
    frequencies: list[int]
    name: str
    tasks: list[asyncio.Task]
    conductor: Optional[str] = None
    registered: bool = False

    def __init__(self, name: str, config: collections.abc.MutableMapping, listener: data.ListenerConfig) -> None:
        self.uuid = str(uuid.uuid4())
        self.config = config
        self.name = name
        self.logger = logger.getChild(self.name)
        self.listener = listener
        self.frequencies = []
        self.tasks = []
        self.recipient_subscriber = bus.REMOTE_BROKER.subscriber(self.target)
        self.recipient_subscriber.add_callback(self.on_remote_event)
        self.broadcast_subscriber = bus.REMOTE_BROKER.subscriber('/')
        self.broadcast_subscriber.add_callback(self.on_remote_event, lambda t: t.startswith('available'))
        self.broadcast_subscriber.add_callback(self.on_remote_event, lambda t: t.startswith('unavailable'))
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
        asyncio.get_running_loop().create_task(self.listen(payload))

    def on_remote_registered(self, uuid: str) -> None:
        if uuid == self.uuid:
            self.registered = True

    def on_remote_deregistered(self, uuid: str) -> None:
        if uuid == self.uuid:
            self.registered = False

    def on_remote_ping(self, _: Any) -> None:
        pass

    def on_remote_die(self, _: Any) -> None:
        logger.warning(f'{self} received DIE order')
        # self.kill()
        # self.deregister()

    def on_remote_available(self, conductor: str) -> None:
        if not self.registered:
            self.register(conductor)

    def on_remote_unavailable(self, conductor: str) -> None:
        self.registered = False

    def on_observer_start(self) -> None:
        self.recipient_subscriber.start()
        self.broadcast_subscriber.start()

    def register(self, conductor: str) -> None:
        self.conductor = conductor
        bus.REMOTE_BROKER.publish(conductor, 'register', self.payload(widths=self.observable_widths()))

    def deregister(self) -> None:
        if self.conductor:
            bus.REMOTE_BROKER.publish(self.conductor, 'deregister', self.payload())

    @functools.cached_property
    def proxy(self) -> hfdl_observer.manage.ReceiverProxy:
        _proxy = hfdl_observer.manage.ReceiverProxy(self.name, self.uuid, self.observable_widths())
        # _proxy.subscribe(f'receiver:{self.name}', self.on_remote_event)
        # a bit presumptuous.
        # self.subscribe(f'receiver:{self.name}', _proxy.on_remote_event)
        return _proxy

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

    def __init__(self, name: str, config: collections.abc.MutableMapping, listener: data.ListenerConfig) -> None:
        super().__init__(name, config, listener)
        self.setup_harnesses()

    def setup_harnesses(self) -> None:
        raise NotImplementedError(str(self.__class__))

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

    def __init__(self, name: str, config: collections.abc.MutableMapping, listener: data.ListenerConfig) -> None:
        super().__init__(name, config, listener)
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
        # self.publish(f'receiver:{self.name}', ('listening', self.channel.frequencies))
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
        await self.decoder.kill()

    def observable_widths(self) -> list[int]:
        return self.observable_channel_widths

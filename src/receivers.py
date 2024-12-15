# receivers.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import collections.abc
import functools
import logging
import random

from typing import Any

import hfdl_observer
import hfdl_observer.bus
import hfdl_observer.manage
import hfdl_observer.process

import decoders
import iqsources


logger = logging.getLogger(__name__)


class ReceiverError(Exception):
    pass


class LocalReceiver(hfdl_observer.bus.Publisher):
    frequencies: list[int]

    name: str

    def __init__(
        self,
        name: str,
        config: collections.abc.MutableMapping,
        listener: hfdl_observer.data.ListenerConfig,
        parameters: hfdl_observer.data.Parameters
    ) -> None:
        self.config = config
        self.name = name
        self.logger = logger.getChild(self.name)
        self.listener = listener
        self.parameters = parameters
        self.frequencies = []
        super().__init__()

    def on_remote_event(self, _: Any) -> None:
        pass

    @functools.cached_property
    def proxy(self) -> hfdl_observer.manage.ReceiverProxy:
        _proxy = hfdl_observer.manage.ReceiverProxy(self.name, self.parameters.max_sample_rate, self)
        _proxy.subscribe(f'receiver:{self.name}', self.on_remote_event)
        # a bit presumptuous.
        self.subscribe(f'receiver:{self.name}', _proxy.on_remote_event)
        return _proxy

    def kill(self) -> None:
        pass


class Web888Receiver(LocalReceiver):
    tasks: list[asyncio.Task]
    shell: bool = False

    def __init__(
        self,
        name: str,
        config: collections.abc.MutableMapping,
        listener: hfdl_observer.data.ListenerConfig,
        parameters: hfdl_observer.data.Parameters
    ) -> None:
        super().__init__(name, config, listener, parameters)
        self.tasks = []
        self.setup_harnesses()

    def setup_harnesses(self) -> None:
        raise NotImplementedError()

    def covers(self, freqs: list[int]) -> bool:
        # in this implementation it must be exact.
        return freqs == self.frequencies

    def listen(self, frequencies: list[int]) -> None:
        self.logger.debug(f'switching to {frequencies} from {self.frequencies}')
        self.stop()
        self.frequencies = frequencies
        self.allocation = self.parameters.allocation(frequencies)
        self.start()
        self.logger.debug(f'switched to {frequencies}')

        # self.publish(f'receiver:{self.name}', ('listening', self.allocation.frequencies))

    def start(self) -> None:
        # if self.tasks:
        #     raise ReceiverError('previous tasks still running')
        self.task = asyncio.get_running_loop().create_task(self.run())

    def stop(self) -> None:
        pass

    async def run(self) -> None:
        raise NotImplementedError()

    def on_remote_event(self, event: tuple[str, Any]) -> None:
        action, arg = event
        if action == 'listen':
            self.listen(arg)
        elif action == 'ping':
            self.publish(f'receiver:{self.name}', ('pong', None))

    def __str__(self) -> str:
        return f'({self.__class__.__name__}) {self.name} on {self.frequencies}'


class DummyReceiver(Web888Receiver):

    def setup_harnesses(self) -> None:
        self.client = iqsources.DummyClient(self.name, self.config.get('client', {}))
        self.decoder = decoders.DummyDecoder(self.name, self.config.get('decoder', {}), self.listener)

    async def run(self) -> None:
        pass


class Web888ExecReceiver(Web888Receiver):
    client: iqsources.KiwiClientProcess
    decoder: decoders.IQDecoderProcess

    def setup_harnesses(self) -> None:
        self.client = iqsources.KiwiClientProcess(self.name, self.config.get('client', {}))
        self.decoder = decoders.IQDecoderProcess(self.name, self.config.get('decoder', {}), self.listener)

    def on_task_done(self, task: asyncio.Task) -> None:
        exc = task.exception()
        if exc:
            self.publish('fatal', (str(self), str(exc)))
        if task in self.tasks:
            # we have not been asked to stop or kill, so this task has ended prematurely.
            self.tasks.remove(task)
            if not self.tasks:
                # there are no more tasks, so there's no valid allocation
                self.frequencies = []
                self.allocation = self.parameters.allocation([])
                self.publish(f'receiver:{self.name}', ('listening', self.frequencies))

    async def run(self) -> None:
        self.publish(f'receiver:{self.name}', ('listening', self.allocation.frequencies))
        await asyncio.sleep(random.randrange(1, 20) / 10.0)   # thundering herd dispersal
        client_task = self.client.listen(self.allocation)
        async with self.client.running_condition:
            self.tasks.append(client_task)
            client_task.add_done_callback(self.on_task_done)
            await self.client.running_condition.wait()
            self.decoder.iq_fd = self.client.pipe.read
            decoder_task = self.decoder.listen(self.allocation)
            self.tasks.append(decoder_task)
            decoder_task.add_done_callback(self.on_task_done)

    def stop(self) -> None:
        self.logger.debug('Stopping')
        self.tasks = []  # don't care about these tasks anymore
        self.client.stop()
        self.decoder.stop()

    def kill(self) -> None:
        self.logger.debug('Killing')
        self.tasks = []  # don't care about these tasks anymore
        self.client.kill()
        self.decoder.kill()


class Web888PipeReceiver(Web888Receiver):
    client: iqsources.KiwiClient
    decoder: decoders.IQDecoder

    def setup_harnesses(self) -> None:
        self.client = iqsources.KiwiClient(self.name, self.config.get('client', {}))
        self.decoder = decoders.IQDecoder(self.name, self.config.get('decoder', {}), self.listener)
        self.receiver_pipe = ReceiverPipe(self.client.commandline() + ['|'] + self.decoder.commandline())

    async def run(self) -> None:
        self.receiver_pipe.start()

    def stop(self) -> None:
        self.logger.debug('Stopping')
        self.receiver_pipe.stop()

    def kill(self) -> None:
        self.logger.debug('Killing')
        self.receiver_pipe.kill()


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

# iqsources.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import collections.abc
import logging
import os
import pathlib

from typing import Any, Optional

import hfdl_observer.data
import hfdl_observer.env as env
import hfdl_observer.process


logger = logging.getLogger(__name__)


Pipe = collections.namedtuple('Pipe', 'read write')


class KiwiClientCommand(hfdl_observer.process.Command):
    pass


class KiwiClient:
    config: collections.abc.Mapping
    channel: Optional[hfdl_observer.data.ObservingChannel] = None
    pipe: Pipe

    def __init__(self, name: str, config: collections.abc.MutableMapping):
        self.name = name
        self.config = config
        super().__init__()
        # recoverable error? 'Too busy now. Reconnecting after 15 seconds'

    def agc_file(self, center_freq: float) -> pathlib.Path:
        agc = self.config['agc_files']
        band = center_freq // 1000
        for k in [band, '*']:
            agc_file = env.as_path(agc.get(k))
            if agc_file.exists():
                return agc_file
        return env.as_path('agc.yaml')

    def commandline(self) -> list[str]:
        if not self.channel or not self.channel.frequencies:
            return []
        # python3 kiwirecorder.py --nc -s n4dkd.asuscomm.com -p 8901 --log info -f 8927 -m iq --tlimit 60 --user kiwi_nc
        # | dumphfdl --iq-file - --sample-rate 12000 --sample-format CS16 --read-buffer-size 9600 --centerfreq 8927 8927
        # find the executable.
        return [
            str(env.as_executable_path(self.config['recorder_path'])),
            '--nc',
            '--log', 'info',
            '-s', self.config['address'],
            '-p', str(self.config['port']),
            '-f', str(self.channel.center),
            '-m', 'iq',
            '-L', '-8000', '-H', '8000',
            '--OV',
            '--agc-yaml', str(self.agc_file(self.channel.center)),
            '--user', self.config['username'],
            # '--tlimit', '120',
        ]

    def __str__(self) -> str:
        return f'{self.__class__.__name__}@{self.name}'


class KiwiClientProcess(hfdl_observer.process.ProcessHarness, KiwiClient):

    def __init__(self, name: str, config: collections.abc.MutableMapping):
        KiwiClient.__init__(self, name, config)
        hfdl_observer.process.ProcessHarness.__init__(self)
        self.settle_time = config.get('settle_time', 0)

    def commandline(self) -> list[str]:
        return KiwiClient.commandline(self)

    def execution_arguments(self) -> dict:
        return {
            'stdout': self.pipe.write
        }

    def on_execute(self, process: asyncio.subprocess.Process, context: Any) -> None:
        os.close(self.pipe.write)
        pass

    async def listen(self, channel: hfdl_observer.data.ObservingChannel) -> asyncio.Task:
        self.channel = channel
        logger.debug(f'{self} starting {channel}')
        logger.debug(f'{self} {self.commandline()}')
        return await self.start()

    def create_command(self) -> KiwiClientCommand:
        self.pipe = Pipe(*os.pipe())
        command = KiwiClientCommand(
            self.logger,
            self.commandline(),
            self.execution_arguments(),
            on_prepare=self.on_prepare,
            on_running=self.on_execute,
            recoverable_errors=[
                'Too busy now. Reconnecting after 15 seconds',
                'server closed the connection unexpectedly. Reconnecting after 5 seconds',
            ],
            unrecoverable_errors=['Errno 32.*Broken pipe'],
            valid_return_codes=[0, -11, -15],  # -11 is speculative. Some weirdness on odroid
        )
        return command

    @property
    def running_condition(self) -> asyncio.Condition:
        if not self.command:
            raise ValueError('Inconsistent state. Asking for a Running Condition with no Command')
        return self.command.running_condition


class Web888ClientProcess(KiwiClientProcess):
    pass


class DummyClient(KiwiClientProcess):
    async def run(self) -> None:
        self.killed = False
        self.process = await asyncio.subprocess.create_subprocess_exec('ls')
        async with self.running_condition:
            self.running_condition.notify_all()
        logger.debug(f'{self} dummy run completed')

# iqsources.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import collections.abc
import logging
import pathlib
import random

from typing import Any, Awaitable, Optional

import hfdl_observer.data
import hfdl_observer.env as env
import hfdl_observer.process
import hfdl_observer.util as util


logger = logging.getLogger(__name__)


class KiwiClientCommand(hfdl_observer.process.Command):
    pass


class KiwiClient:
    config: collections.abc.Mapping
    channel: Optional[hfdl_observer.data.ObservingChannel] = None
    # pipe: Pipe

    def __init__(self, name: str, config: collections.abc.MutableMapping):
        self.name = name
        self.config = config
        super().__init__()
        # recoverable error? 'Too busy now. Reconnecting after 15 seconds'

    def agc_file(self, center_freq: float) -> Optional[pathlib.Path]:
        agc = self.config['agc_files']
        band = center_freq // 1000
        for k in [band, '*']:
            try:
                agc_file = env.as_path(agc[k])
                if agc_file.exists():
                    return agc_file
            except KeyError:
                pass
        return None

    def commandline(self) -> list[str]:
        if not self.channel or not self.channel.frequencies:
            return []
        # python3 kiwirecorder.py --nc -s n4dkd.asuscomm.com -p 8901 --log info -f 8927 -m iq --tlimit 60 --user kiwi_nc
        # | dumphfdl --iq-file - --sample-rate 12000 --sample-format CS16 --read-buffer-size 9600 --centerfreq 8927 8927
        # find the executable.
        args = [
            str(env.as_executable_path(self.config['recorder_path'])),
            '--nc',
            '--log', 'info',
            '-s', self.config['address'],
            '-p', str(self.config['port']),
            '-f', str(self.channel.center_khz),
            '-m', 'iq',
            '-L', '-8000', '-H', '8000',
            '--OV',
            '--user', self.config['username'],
            # '--tlimit', '120',
        ]
        agc_file = self.agc_file(self.channel.center_khz)
        if agc_file:
            args.extend(['--agc-yaml', str(agc_file)])
        return args

    def __str__(self) -> str:
        return f'{self.__class__.__name__}@{self.name}'


class KiwiClientProcess(hfdl_observer.process.ProcessHarness, KiwiClient):
    pipe: util.Pipe

    def __init__(self, name: str, config: collections.abc.MutableMapping):
        KiwiClient.__init__(self, name, config)
        hfdl_observer.process.ProcessHarness.__init__(self)
        self.settle_time = config.get('settle_time', 0) + random.randrange(1, 1000) / 1000.0

    def commandline(self) -> list[str]:
        return KiwiClient.commandline(self)

    def execution_arguments(self) -> dict:
        return {
            'stdout': self.pipe.write
        }

    def on_execute(self, process: asyncio.subprocess.Process, context: Any) -> None:
        self.pipe.close_write()

    async def listen(self, channel: hfdl_observer.data.ObservingChannel) -> None:
        self.channel = channel
        logger.debug(f'{self} starting {channel}')
        await self.start()

    def create_command(self) -> KiwiClientCommand:
        # self.pipe = Pipe(*os.pipe())
        cmd = self.commandline()
        # self.logger.info(f'CMD: {cmd}')
        command = KiwiClientCommand(
            self.logger,
            cmd,
            self.execution_arguments(),
            on_prepare=self.on_prepare,
            on_running=self.on_execute,
            recoverable_errors=[
                'Too busy now. Reconnecting after 15 seconds',
                'server closed the connection unexpectedly. Reconnecting after 5 seconds',
            ],
            unrecoverable_errors=[
                'Errno 9.*Bad file descriptor',
                'Errno 22.*Invalid argument',
                'Errno 32.*Broken pipe',
            ],
            valid_return_codes=[0, -11, -15],  # -11 is speculative. Some weirdness on odroid
        )
        return command

    @property
    def running_condition(self) -> asyncio.Condition:
        if not self.command:
            raise ValueError('Inconsistent state. Asking for a Running Condition with no Command')
        return self.command.running_condition

    async def when_ready(self, awaitable: Awaitable) -> None:
        try:
            async with self.running_condition:
                await self.running_condition.wait()
                self.pipe.close_write()
                await awaitable
        except asyncio.CancelledError:
            pass


class Web888ClientProcess(KiwiClientProcess):
    pass


class DummyClient(KiwiClientProcess):
    async def run(self) -> None:
        self.killed = False
        self.process = await asyncio.subprocess.create_subprocess_exec('ls')
        async with self.running_condition:
            self.running_condition.notify_all()
        logger.debug(f'{self} dummy run completed')

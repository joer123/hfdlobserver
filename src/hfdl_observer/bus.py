# hfdl_observer/bus.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import json
import logging
import pathlib

from typing import Any, AsyncGenerator, Callable, Optional, Union

import requests


logger = logging.getLogger(__name__)


class Publisher:
    _subscribers: dict[str, list[Callable]]

    def __init__(self) -> None:
        self._subscribers = {}

    def subscribe(self, subject: str, callback: Callable) -> None:
        if self._subscribers is None:
            self._subscribers = collections.defaultdict(list)
        self._subscribers.setdefault(subject, []).append(callback)

    def publish(self, subject: str, body: Any) -> None:
        loop = asyncio.get_event_loop()
        for subscriber in (self._subscribers or {}).get(subject, []):
            loop.call_soon(subscriber, body)


class JSONWatcher(Publisher):
    def jsonify(self, text: str) -> None:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning(f'ignoring JSON decode error for {self}', exc_info=e)
        else:
            self.publish('json', data)


class RoutineTask(Publisher):
    loop = asyncio.get_event_loop()
    enabled = False
    task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self.enabled = True
        self.task = self.loop.create_task(self.run())

    def stop(self) -> None:
        self.enabled = False
        if self.task:
            self.task.cancel()
            self.task = None

    async def run(self) -> None:
        pass


class PeriodicTask():
    def __init__(self, period: int):
        super().__init__()
        self.period = period
        self.enabled = False

    def prepare(self) -> None:
        pass

    async def execute(self) -> None:
        pass

    async def run(self) -> None:
        self.enabled = True
        self.prepare()
        while self.enabled:
            logger.info(f'{self} executing')
            await self.execute()
            await asyncio.sleep(self.period)


class RemoteURLRefresher(PeriodicTask, Publisher):
    def __init__(self, url: str, period: int = 60):
        PeriodicTask.__init__(self, period)
        Publisher.__init__(self)
        self.url = url

    async def execute(self) -> None:
        data = {}
        try:
            response = await asyncio.to_thread(requests.get, self.url)
        except requests.exceptions.ReadTimeout as e:
            logger.warning(f'suppressing timeout on {self.url}.', exc_info=e)
            return
        try:
            txt = response.text
            data = json.loads(txt)
        except (json.JSONDecodeError, requests.JSONDecodeError):
            logger.warning(f'update for {self.url} failed. ignoring...')
            return
        self.publish('response', data)

    def __str__(self) -> str:
        return f"<RemoteURLRefresher: `{self.url}` @ {self.period}>"


class FileRefresher(PeriodicTask, Publisher):
    def __init__(self, path: Union[pathlib.Path, str], period: int = 60):
        PeriodicTask.__init__(self, period)
        Publisher.__init__(self)
        self.path = pathlib.Path(path)

    async def execute(self) -> None:
        try:
            text = self.path.read_text()
        except IOError as e:
            logger.warning(f'suppressing file read error at {self.path}.', exc_info=e)
        else:
            self.publish('text', text)

    def __str__(self) -> str:
        return f"<FileRefresher: `{self.path}` @ {self.period}>"


class JSONFileRefresher(FileRefresher, JSONWatcher):
    def __init__(self, path: Union[pathlib.Path, str], period: int = 60):
        JSONWatcher.__init__(self)
        FileRefresher.__init__(self, path, period)
        self.subscribe('text', self.jsonify)

    def __str__(self) -> str:
        return f"<JSONFileRefresher: `{self.path}` @ {self.period}>"


class StreamWatcher(RoutineTask, Publisher):
    debug_logger: Optional[logging.Logger]

    def __init__(self, stream: AsyncGenerator, debug_logger: Optional[logging.Logger] = None):
        RoutineTask.__init__(self)
        Publisher.__init__(self)
        self.stream = stream
        self.debug_logger = debug_logger

    async def run(self) -> None:
        logger.debug(f'watching {self.stream}')
        async for data in self.stream:
            line = data.decode('utf8').rstrip()
            if self.debug_logger:
                self.debug_logger.info(line)
            self.publish('line', line)
            if not self.enabled:
                break
            await asyncio.sleep(0)
        logger.info(f'finished watching {self.stream}')


class JSONStreamWatcher(StreamWatcher, JSONWatcher):
    def __init__(self, stream: AsyncGenerator):
        JSONWatcher.__init__(self)
        StreamWatcher.__init__(self, stream)
        self.subscribe('line', self.jsonify)

# hfdl_observer/bus.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import functools
import json
import logging
import pathlib
import weakref

from typing import Any, AsyncGenerator, Callable, Optional, Union

import requests

import hfdl_observer.util as util
import hfdl_observer.zero as zero

Message = util.Message
logger = logging.getLogger(__name__)


class LocalSubscriber(util.Subscriber):
    def start(self) -> asyncio.Task:
        # return a noop task
        return util.schedule(asyncio.sleep(0.1))

    def _stop(self) -> None:
        pass


class TopicSubscriber:
    topic: str
    subscriber: weakref.ReferenceType[util.Subscriber]

    def __init__(self, subscriber: util.Subscriber, topic: str = '') -> None:
        self.topic = topic
        self.subscriber = weakref.ref(subscriber)

    def receive(self, message: Message) -> bool:
        subscriber = self.subscriber()
        if subscriber is not None:
            subscriber.receive(message)
        return subscriber is not None

    def is_subscribed(self, topic: str) -> bool:
        return self.topic == '' or topic.startswith(self.topic)


class LocalBroker(util.Publisher, util.Broker):

    subscribers: list[TopicSubscriber]

    def __init__(self) -> None:
        self.subscribers = []

    def subscribe(self, subscriber: util.Subscriber, topic: str = '') -> None:
        self.subscribers.append(TopicSubscriber(subscriber, topic))

    def subscriber(self, topic: str = '') -> util.Subscriber:
        subscriber = LocalSubscriber()
        self.subscribe(subscriber, topic)
        return subscriber

    def publisher(self) -> util.Publisher:
        return self

    async def publish(self, message: Message) -> None:
        dead = []
        topic = f'{message.target}|{message.subject}'
        logger.debug(f'publish {topic}')
        for subscriber in self.subscribers:
            if subscriber.is_subscribed(topic):
                if not subscriber.receive(message):
                    dead.append(subscriber)
        for dead_subscriber in dead:
            self.subscribers.remove(dead_subscriber)


LOCAL_BROKER: None | LocalBroker = None


class RemotePublisher(zero.ZeroPublisher, util.Publisher):
    pass


class RemoteSubscriber(zero.ZeroSubscriber):
    task: asyncio.Task

    def start(self) -> asyncio.Task:
        self.task = util.schedule(self.run())
        return self.task


class RemoteBroker(util.Broker):
    def __init__(self, config: dict) -> None:
        if not config:
            # with no config, this becomes a dummy
            self.configured = False
            return
        self.host = config.get('host', 'localhost')
        self.pub_port = config.get('pub_port', 5559)
        self.sub_port = config.get('sub_port', 5560)
        self.context = zero.get_thread_context()
        self._publisher = self.publisher()
        self.configured = True

    def subscriber(self, target: str) -> util.Subscriber:
        url = f'tcp://{self.host}:{self.sub_port}'
        logger.debug(f'subscriber {url}/{target}')
        return RemoteSubscriber(url, target, context=self.context)

    def publisher(self) -> util.Publisher:
        logger.debug(f'publisher {self.host}:{self.pub_port}')
        return RemotePublisher(self.host, self.pub_port, context=self.context)

    async def publish(self, message: Message) -> None:
        if not hasattr(self, '_publisher'):  # race condition if we're asked to publish before we're initialized.
            return None
        try:
            logger.debug(f'queuing {message}')
            await self._publisher.publish(message)
        except AttributeError:
            logger.debug(f'dropping {message}')
        return None


class NullBroker(RemoteBroker):
    def __init__(self) -> None:
        super().__init__({})


class GenericRemoteMessageDispatcher:

    def __init__(self) -> None:
        # this ties the cache to the instance. If decorating directly, instances are not necessarily GCd. ever.
        self.get_remote_handler = functools.cache(self.get_remote_handler)  # type: ignore[method-assign]

    def get_remote_handler(self, name: str) -> None | Callable:
        return getattr(self, f'on_remote_{name.strip()}', None)

    def on_remote_event(self, message: Message) -> None:
        handler = self.get_remote_handler(message.subject)
        if callable(handler):
            logger.debug(f'dispatching {message} via {handler}')
            handler(message)
        else:
            pass
            # logger.debug(f'ignoring on_remote_{message.subject.strip()}')


REMOTE_BROKER: RemoteBroker = NullBroker()


def brokers() -> list[util.Broker]:
    brokers: list[util.Broker] = []
    if LOCAL_BROKER is not None:
        brokers.append(LOCAL_BROKER)
    if REMOTE_BROKER is not None and REMOTE_BROKER.configured:
        brokers.append(REMOTE_BROKER)
    return brokers


async def publish(message: Message) -> None:
    for broker in brokers():
        await broker.publish(message)


def publish_soon(message: Message) -> None:
    util.schedule(publish(message))


class EventNotifier:
    _watchers: dict[str, list[Callable]]

    def __init__(self) -> None:
        self._watchers = {}

    def watch_event(self, subject: str, callback: Callable) -> None:
        if self._watchers is None:
            self._watchers = collections.defaultdict(list)
        self._watchers.setdefault(subject, []).append(callback)

    def notify_event(self, subject: str, body: Any) -> None:
        loop = asyncio.get_event_loop()
        for subscriber in (self._watchers or {}).get(subject, []):
            loop.call_soon(subscriber, body)


class JSONWatcher(EventNotifier):
    def jsonify(self, text: str) -> None:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning(f'ignoring JSON decode error for {self}', exc_info=e)
        else:
            self.notify_event('json', data)


class RoutineTask(EventNotifier):
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
    chatty: bool = True

    def __init__(self, period: float):
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
        while self.enabled and not util.is_shutting_down():
            if self.chatty:
                logger.debug(f'{self} executing')
            try:
                await self.execute()
            except asyncio.CancelledError:
                break
            except Exception as err:
                logger.error(f'{self} encountered error {err}')
            await asyncio.sleep(self.period)

    def start(self) -> None:
        if not hasattr(self, 'task'):
            self.task = util.schedule(self.run())

    async def stop(self) -> None:
        task = getattr(self, 'task')
        if task:
            self.enabled = False
            task.cancel()
            # await task


class PeriodicCallback(PeriodicTask):
    def __init__(self, period: float, callbacks: list[Callable], chatty: bool = True) -> None:
        super().__init__(period)
        self.chatty = chatty
        self.callbacks = callbacks or []

    async def execute(self) -> None:
        for callback in self.callbacks:
            if callable(callback):
                try:
                    callback()
                except Exception as err:
                    logger.error(f'{self} {callback} encountered error {err}')

    def __str__(self) -> str:
        return f'<PeriodicCallback@{self.period} [{";".join(str(c) for c in self.callbacks)}]>'


class RemoteURLRefresher(PeriodicTask, EventNotifier):
    def __init__(self, url: str, period: int = 60):
        PeriodicTask.__init__(self, period=period)
        EventNotifier.__init__(self)
        self.url = url

    async def execute(self) -> None:
        data = {}
        try:
            response = await util.in_thread(requests.get, self.url)
        except requests.exceptions.ReadTimeout as e:
            logger.warning(f'suppressing timeout on {self.url}.', exc_info=e)
            return
        try:
            txt = response.text
            data = json.loads(txt)
        except (json.JSONDecodeError, requests.JSONDecodeError):
            logger.warning(f'update for {self.url} failed. ignoring...')
            return
        self.notify_event('response', data)

    def __str__(self) -> str:
        return f"<RemoteURLRefresher: `{self.url}` @ {self.period}>"


class FileRefresher(PeriodicTask, EventNotifier):
    def __init__(self, path: Union[pathlib.Path, str], period: int = 60):
        PeriodicTask.__init__(self, period)
        EventNotifier.__init__(self)
        self.path = pathlib.Path(path)

    async def execute(self) -> None:
        try:
            text = self.path.read_text()
        except IOError as e:
            logger.warning(f'suppressing file read error at {self.path}.', exc_info=e)
        else:
            self.notify_event('text', text)

    def __str__(self) -> str:
        return f"<FileRefresher: `{self.path}` @ {self.period}>"


class JSONFileRefresher(FileRefresher, JSONWatcher):
    def __init__(self, path: Union[pathlib.Path, str], period: int = 60):
        JSONWatcher.__init__(self)
        FileRefresher.__init__(self, path, period)
        self.watch_event('text', self.jsonify)

    def __str__(self) -> str:
        return f"<JSONFileRefresher: `{self.path}` @ {self.period}>"


class StreamWatcher(RoutineTask, EventNotifier):
    debug_logger: Optional[logging.Logger]

    def __init__(self, stream: AsyncGenerator, debug_logger: Optional[logging.Logger] = None):
        RoutineTask.__init__(self)
        EventNotifier.__init__(self)
        self.stream = stream
        self.debug_logger = debug_logger

    async def run(self) -> None:
        logger.debug(f'watching {self.stream}')
        async with util.aclosing(self.stream) as stream:
            async for data in stream:
                line = data.decode('utf8').rstrip()
                if self.debug_logger:
                    self.debug_logger.info(line)
                self.notify_event('line', line)
                await asyncio.sleep(0)
                if not self.enabled:
                    break
        logger.debug(f'finished watching {self.stream}')


class JSONStreamWatcher(StreamWatcher, JSONWatcher):
    def __init__(self, stream: AsyncGenerator):
        JSONWatcher.__init__(self)
        StreamWatcher.__init__(self, stream)
        self.watch_event('line', self.jsonify)

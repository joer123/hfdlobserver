import asyncio
import dataclasses
import json
import logging
import threading

from typing import Any, Callable, Optional

import zmq
import zmq.asyncio

Context = zmq.asyncio.Context
GLOBAL_CONTEXT = Context.instance()
logger = logging.getLogger(__name__)


class ZeroBroker:
    # feeling cute, might delete later.
    thread: Optional[threading.Thread] = None

    def __init__(self, host: str = '*', pub_port: int = 5559, sub_port: int = 5560) -> None:
        self.host = host
        self.pub_port = pub_port
        self.sub_port = sub_port

    def run(self) -> None:
        context = zmq.Context()

        # Socket facing clients
        xpub = context.socket(zmq.XPUB)
        xpub.bind(f"tcp://{self.host}:{self.sub_port}")

        # Socket facing services
        xsub = context.socket(zmq.XSUB)
        xsub.bind(f"tcp://{self.host}:{self.pub_port}")

        zmq.proxy(xpub, xsub)

        # We never get here...
        xpub.close()
        xsub.close()
        context.term()

    def start(self, daemon: bool = True) -> None:
        if not self.thread:
            self.thread = threading.Thread(target=self.run, daemon=daemon)
            self.thread.start()


@dataclasses.dataclass
class Message:
    target: str
    subject: str
    payload: Any

    def __str(self) -> str:
        body: str
        if isinstance(self.payload, str):
            body = self.payload
        else:
            body = self.payload.__class__.__name__
            if hasattr(self.payload, '__len__'):
                body = f'{body} l={len(self.payload)}'
        return f'<Message: t={self.target} s={self.subject} b={body}>'


class ZeroSubscriber:
    url: str
    channel: str
    callbacks: list[tuple[Optional[Callable[[Message], bool]], Callable[[Message], None]]]
    context: Context
    socket: Optional[zmq.Socket] = None

    def __init__(self, url: str, channel: str, context: Optional[zmq.asyncio.Context] = None):
        self.context = context or GLOBAL_CONTEXT
        self.callbacks = []
        self.channel = channel
        self.url = url

    def add_callback(
        self, callback: Callable[[Message], None], filter: Optional[Callable[[Message], bool]] = None
    ) -> None:
        self.callbacks.append((filter, callback))

    async def run(self) -> None:
        if self.socket:
            return
        self.running = True
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(self.url)
        # logger.info(f'{self} subscribing to "{self.channel}"')
        self.socket.setsockopt(zmq.SUBSCRIBE, self.channel.encode())

        try:
            while self.running:
                parts = await self.socket.recv_multipart()
                header, body = parts
                payload = json.loads(body.decode())
                target, subject = header.decode().split('|', 1)
                message = Message(target=target, subject=subject, payload=payload)
                logger.debug(f'received {message}')
                for filter, callback in self.callbacks:
                    if not filter or filter(message):
                        asyncio.get_running_loop().call_soon(callback, message)
        finally:
            # logger.info(f'no longer subscribed to {self.url}/{self.channel}')
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None

    async def stop(self) -> None:
        self.running = False


class ZeroPublisher:
    host: str
    port: int
    channel: str
    socket: Optional[zmq.Socket] = None

    def __init__(self, host: str, port: int, context: Optional[zmq.asyncio.Context] = None):
        self.context = context or GLOBAL_CONTEXT
        self.host = host
        self.port = port

    def start(self) -> None:
        if self.socket is None:
            self.socket = self.context.socket(zmq.PUB)
            self.socket.connect(f'tcp://{self.host}:{self.port}')

    def stop(self) -> None:
        if self.socket is not None:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None

    async def publish(self, message: Message) -> None:
        await self._publish(f'{message.target}|{message.subject}', json.dumps(message.payload))

    async def multi_publish(self, targets: list[str], subject: str, payload: Any) -> None:
        json_payload = json.dumps(payload)
        await asyncio.gather(*[self._publish(f'{target}|{subject}', json_payload) for target in targets])

    async def _publish(self, channel: str, payload: str) -> None:
        encoded_channel = channel.encode()
        encoded_payload = payload.encode()
        if self.socket is None:
            # logger.info('publisher not connected')
            self.start()
        if self.socket is not None:
            logger.debug(f'publish c={channel} l={len(encoded_payload)}')
            await self.socket.send_multipart([encoded_channel, encoded_payload])

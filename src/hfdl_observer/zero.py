import asyncio
import json
import threading

from typing import Any, Callable, Optional

import zmq
import zmq.asyncio

Context = zmq.asyncio.Context
GLOBAL_CONTEXT = Context.instance()


class ZeroBroker:
    # feeling cute, might delete later.
    thread: Optional[threading.Thread] = None

    def run(self) -> None:
        context = zmq.Context()

        # Socket facing clients
        outputs = context.socket(zmq.XPUB)
        outputs.bind("tcp://*:5559")

        # Socket facing services
        inputs = context.socket(zmq.XSUB)
        inputs.bind("tcp://*:5560")

        zmq.proxy(outputs, inputs)

        # We never get here...
        outputs.close()
        inputs.close()
        context.term()

    def start(self, daemon: bool = True) -> None:
        if not self.thread:
            self.thread = threading.Thread(target=self.run, daemon=daemon)
            self.thread.start()


class ZeroSubscriber:
    url: str
    channel: str
    callbacks: list[tuple[Optional[Callable[[str], bool]], Callable[[str, Any], None]]]
    context: Context
    socket: Optional[zmq.Socket] = None

    def __init__(self, url: str, channel: str, context: Optional[zmq.asyncio.Context] = None):
        self.context = context or GLOBAL_CONTEXT
        self.callbacks = []
        self.channel = channel
        self.url = url

    def add_callback(
        self, callback: Callable[[str, Any], None], filter: Optional[Callable[[str], bool]] = None
    ) -> None:
        self.callbacks.append((filter, callback))

    async def run(self) -> None:
        if self.socket:
            return
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(self.url)
        self.socket.setsockopt(zmq.SUBSCRIBE, self.channel.encode())

        try:
            while True:
                message = await self.socket.recv_multipart()
                header, body = message
                payload = json.loads(body.decode())
                target, subject = header.decode().split('|', 1)
                for filter, callback in self.callbacks:
                    if not filter or filter(subject):
                        asyncio.get_running_loop().call_soon(callback, subject, payload)
        finally:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()


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
            self.socket.bind(f'tcp://{self.host}:{self.port}')

    def stop(self) -> None:
        if self.socket is not None:
            self.socket.setsockopt(zmq.LINGER, 0)
            self.socket.close()
            self.socket = None

    async def publish(self, target: str, subject: str, payload: Any) -> None:
        await self._publish(f'{target}|subject', json.dumps(payload))

    async def multi_publish(self, targets: list[str], subject: str, payload: Any) -> None:
        json_payload = json.dumps(payload)
        await asyncio.gather(*[self._publish(f'{target}|{subject}', json_payload) for target in targets])

    async def _publish(self, channel: str, payload: str) -> None:
        encoded_channel = channel.encode() + b'\0'
        encoded_payload = payload.encode()
        if self.socket is not None:
            await self.socket.send_multipart([encoded_channel, encoded_payload])

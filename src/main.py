#!/usr/bin/env python3
# main.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import asyncio.protocols
import collections
import collections.abc
import itertools
import logging
import logging.handlers
import pathlib
import sys

from signal import SIGINT, SIGTERM
from typing import Callable, Optional

import click

import hfdl_observer.bus
import hfdl_observer.data
import hfdl_observer.hfdl
import hfdl_observer.listeners
import hfdl_observer.manage

import receivers
import settings
import packet_stats


logger = logging.getLogger(sys.argv[0].rsplit('/', 1)[-1].rsplit('.', 1)[0] if __name__ == '__main__' else __name__)


class Observer888(hfdl_observer.bus.Publisher):
    local_receivers: list[receivers.LocalReceiver]
    proxies: list[hfdl_observer.manage.ReceiverProxy]
    parameters: hfdl_observer.data.Parameters
    running: bool = True

    def __init__(self, config: collections.abc.Mapping) -> None:
        super().__init__()
        self.config = config
        self.active_ground_stations = hfdl_observer.manage.ActiveGroundStations(config['tracker'])
        self.active_ground_stations.subscribe('frequencies', self.on_frequencies)
        self.hfdl_listener = hfdl_observer.listeners.HFDLListener(config.get('hfdl_listener', {}))
        self.hfdl_consumers = [
            hfdl_observer.listeners.HFDLPacketConsumer(
                [hfdl_observer.listeners.HFDLPacketConsumer.any_in('spdu', 'freq_data')],
                [self.active_ground_stations.on_hfdl],
            ),
            hfdl_observer.listeners.HFDLPacketConsumer(
                [lambda line: True],
                [self.on_hfdl],
            ),
        ]
        self.conductor = hfdl_observer.manage.SimpleConductor(config['conductor'])
        self.parameters = self.conductor.parameters

        self.proxies = []
        self.local_receivers = []

        for rname in config['local_receivers']:
            receiver_base = config['all_receivers'][rname]
            receiver_config = settings.flatten(receiver_base, 'receiver')
            typename = receiver_config['type']
            klass = getattr(receivers, typename)
            receiver = klass(rname, receiver_config, self.hfdl_listener.listener, self.parameters)
            self.add_receiver(receiver)

    def add_receiver(self, receiver: receivers.LocalReceiver) -> None:
        receiver.subscribe('fatal', self.on_fatal_error)
        self.local_receivers.append(receiver)
        proxy = receiver.proxy
        proxy.connect(self.conductor)
        self.proxies.append(proxy)
        self.conductor.add_receiver(proxy)

    def on_frequencies(self, stations: dict[int, list[int]]) -> None:
        allocations = self.conductor.allocate_frequencies(stations)
        allocated = self.conductor.orchestrate(allocations)
        self.publish('observing', list(itertools.chain.from_iterable(a.frequencies for a in allocated)))
        self.publish('frequencies', stations)

    def on_hfdl(self, packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        self.publish('packet', packet)

    def on_fatal_error(self, data: tuple[str, str]) -> None:
        receiver, error = data
        logger.error(f'Bailing due to error on receiver {receiver}: {error}')
        self.running = False

    def start(self) -> None:
        self.active_ground_stations.start()
        self.hfdl_listener.start(self.hfdl_consumers)  # self.active_ground_stations.on_hfdl)

    def kill(self) -> None:
        logger.warning(f'{self} killed')
        for receiver in self.local_receivers:
            receiver.kill()


async def async_observe(observer: Observer888) -> None:
    logger.info("Starting observer")

    try:
        observer.start()
        while observer.running:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.error('Observer loop cancelled')
        observer.kill()


def cancel_all_tasks() -> None:
    try:
        for t in asyncio.all_tasks():
            t.cancel()
    except RuntimeError:
        pass


def observe(on_observer: Optional[Callable[[Observer888], None]] = None) -> None:
    loop = asyncio.get_event_loop()

    observer = Observer888(settings.registry['observer888'])
    if on_observer:
        on_observer(observer)
    else:
        log_counter = packet_stats.PacketCounter()
        log_counter.start(loop)
        observer.subscribe('packet', log_counter.on_hfdl)
        observer.subscribe('observing', log_counter.on_observing)
        observer.subscribe('frequencies', log_counter.on_frequencies)

    main_task = asyncio.ensure_future(async_observe(observer))
    for signal in [SIGINT, SIGTERM]:
        loop.add_signal_handler(signal, cancel_all_tasks)
    try:
        loop.run_until_complete(main_task)
    finally:
        logger.info('Observer loop closing')
        cancel_all_tasks()
        loop.close()
        sys.exit()


def setup_logging(loghandler: Optional[logging.Handler], debug: bool = True) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]  # default: stderr
    if loghandler:
        handlers.append(loghandler)
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        handlers=handlers,
        force=True,
    )


@click.command
@click.option('--headless', help='Run headless; with no CUI.', is_flag=True)
@click.option('--debug', help='Output debug/extra information.', is_flag=True)
@click.option(
    '--log',
    help='log output to this file',
    type=click.Path(
        path_type=pathlib.Path,
        writable=True,
        file_okay=True,
        dir_okay=False,
    ),
    default=None)
@click.option(
    '--config',
    help='load settings from this file',
    type=click.Path(
        path_type=pathlib.Path,
        readable=True,
        file_okay=True,
        dir_okay=False,
        exists=True,
    ), default='settings.yaml'
)
def command(headless: bool, debug: bool, log: Optional[pathlib.Path], config: pathlib.Path) -> None:

    settings.load(config)
    handler = logging.handlers.TimedRotatingFileHandler(log, when='d', interval=1) if log else None

    # if not executed in a tty-like thing, headless is forced.
    headless = headless or not sys.stdout.isatty()
    if headless:
        setup_logging(handler, debug)
        observe()
    else:
        import cui
        cui.screen(handler, debug)


if __name__ == '__main__':
    command()

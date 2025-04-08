#!/usr/bin/env python3
# main.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import asyncio.protocols
import collections
import collections.abc
import logging
import logging.handlers
import pathlib
import sys

# from signal import SIGINT, SIGTERM
from typing import Any, Awaitable, Callable, Optional

import click

import hfdl_observer.bus as bus
import hfdl_observer.data
import hfdl_observer.heat
import hfdl_observer.hfdl
import hfdl_observer.listeners
import hfdl_observer.manage as manage
import hfdl_observer.network as network
import hfdl_observer.settings
import hfdl_observer.util as util
import hfdl_observer.zero as zero

import hfdl_observer.orm as orm

import receivers

if sys.version_info < (3, 11):
    from backports.asyncio.runner import Runner
else:
    from asyncio import Runner


logger = logging.getLogger(sys.argv[0].rsplit('/', 1)[-1].rsplit('.', 1)[0] if __name__ == '__main__' else __name__)
TRACEMALLOC = False


class HFDLObserverNode(receivers.ReceiverNode):
    def __init__(self, config: collections.abc.Mapping) -> None:
        super().__init__(config)

        for receiver_config in config['local_receivers']:
            self.build_local_receiver(receiver_config)

    def start(self) -> None:
        self.running = True
        super().start()

    def on_fatal_error(self, data: tuple[str, str]) -> None:
        receiver, error = data
        logger.error(f'Bailing due to error on receiver {receiver}: {error}')
        self.running = False

    def message_broker(self) -> bus.RemoteBroker:
        return bus.REMOTE_BROKER

    async def shutdown(self) -> None:
        awaitables = [receiver.stop() for receiver in self.local_receivers]
        await asyncio.gather(*awaitables, return_exceptions=True)
        self.local_receivers = []


class HFDLObserverController(manage.ConductorNode, receivers.ReceiverNode):
    running: bool = True
    packet_watcher: orm.PacketWatcher
    previous_description: list[str] | None = None

    def __init__(self, config: collections.abc.Mapping) -> None:
        manage.ConductorNode.__init__(self, config)
        receivers.ReceiverNode.__init__(self, config)
        self.packet_watcher = orm.PacketWatcher()
        hfdl_observer.data.PACKET_WATCHER = self.packet_watcher
        self.network_overview = manage.NetworkOverview(config['tracker'], network.UPDATER)
        self.network_overview.subscribe('state', network.UPDATER.prune)
        self.network_overview.subscribe('frequencies', self.on_frequencies)
        self.subscribe('orchestrated', self.maybe_describe_receivers)

        self.hfdl_listener = hfdl_observer.listeners.HFDLListener(config.get('hfdl_listener', {}))
        self.hfdl_consumers = [
            hfdl_observer.listeners.HFDLPacketConsumer(
                [hfdl_observer.listeners.HFDLPacketConsumer.any_in('spdu', 'freq_data')],
                [network.UPDATER.on_hfdl],
            ),
            hfdl_observer.listeners.HFDLPacketConsumer(
                [lambda line: True],
                [self.on_hfdl, self.packet_watcher.on_hfdl],
            ),
        ]
        self.listener_info = self.hfdl_listener.connection_info

        for receiver_config in config['local_receivers']:
            self.build_local_receiver(receiver_config)

    def on_hfdl(self, packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        self.publish('packet', packet)

    def on_fatal_error(self, data: tuple[str, str]) -> None:
        receiver, error = data
        logger.error(f'Bailing due to error on receiver {receiver}: {error}')
        self.running = False

    def start(self) -> None:
        self.running = True
        manage.ConductorNode.start(self)
        receivers.ReceiverNode.start(self)
        self.packet_watcher.prune_every(60)
        self.network_overview.start()
        self.hfdl_listener.start(self.hfdl_consumers)

    def outstanding_awaitables(self) -> list[Awaitable]:
        outstanding: list[Awaitable] = [
            self.packet_watcher.stop_pruning(),
            self.network_overview.stop(),
        ]
        outstanding.extend(receivers.ReceiverNode.outstanding_awaitables(self))
        outstanding.extend(manage.ConductorNode.outstanding_awaitables(self))
        return outstanding

    async def stop(self) -> None:
        logger.info(f'{self} stopped')
        self.hfdl_listener.stop()
        await asyncio.gather(*self.outstanding_awaitables(), return_exceptions=True)
        logger.info(f'{self} tasks halted')

    def message_broker(self) -> bus.RemoteBroker:
        return bus.REMOTE_BROKER

    def ministats(self, _: Any) -> None:
        table = hfdl_observer.heat.TableByFrequency(60, 10)
        for line in str(table).split('\n'):
            logger.info(f'{line}')

    def maybe_describe_receivers(self, *_: Any) -> None:
        current = list(r.describe() for r in self.local_receivers)
        if self.previous_description != current:
            for line in current:
                logger.info(line)
            self.previous_description = current

    async def shutdown(self) -> None:
        awaitables = [receiver.stop() for receiver in self.local_receivers]
        await asyncio.gather(*awaitables, return_exceptions=True)
        self.local_receivers = []


async def async_observe(observer: HFDLObserverController | HFDLObserverNode) -> None:
    logger.info("Starting observer")

    if TRACEMALLOC:
        import tracemalloc
        tracemalloc.start(2)
        last_snapshot = tracemalloc.take_snapshot()
        logger.warning("tracemalloc on")
    count = 0
    observer.start()
    while observer.running:
        await asyncio.sleep(1)
        count += 1
        if TRACEMALLOC and count > 300:
            count = 0
            p = pathlib.Path('memory.trace')
            with p.open("a", encoding='utf8') as f:
                f.write('====\n')
                f.write(f'{util.now()}\n')
                snapshot = tracemalloc.take_snapshot()
                try:
                    diff = snapshot.compare_to(last_snapshot, 'lineno')
                    for e in diff:
                        f.write(f'{e.size}|{e.size_diff}|{e.count}|{";".join(str(x) for x in e.traceback.format(5))}\n')
                    # fname = f'memory-{util.now().isoformat().replace(':', '').replace('-', '')}.trace'
                    # snapshot.dump(fname)
                except Exception as err:
                    logger.error('error in tracemallocry', exc_info=err)
                else:
                    logger.info('tracemalloc checkpoint')
                last_snapshot = snapshot


def observe(
    on_observer: Optional[Callable[[
        HFDLObserverController,
        network.CumulativePacketStats
    ], None]] = None,
    as_controller: bool = True
) -> None:
    key = 'observer' if as_controller else 'node'
    settings = getattr(hfdl_observer.settings, key)
    broker_config = settings.get('messaging', {})
    remote_broker = bus.RemoteBroker(broker_config)
    bus.REMOTE_BROKER = remote_broker  # this might be a bit of a hack...

    observer: HFDLObserverController | HFDLObserverNode

    try:
        with Runner() as runner:
            util.thread_local.loop = runner.get_loop()
            util.thread_local.runner = runner

            if as_controller:
                message_broker = zero.ZeroBroker(**broker_config)
                message_broker.start()  # daemon thread, not async.
                network.UPDATER = orm.NetworkUpdater()
                observer = HFDLObserverController(settings)
                cumulative = network.CumulativePacketStats()
                observer.subscribe('packet', cumulative.on_hfdl)
                if on_observer:
                    on_observer(observer, cumulative)
                else:
                    # initialize headless
                    observer.network_overview.subscribe('state', observer.ministats)
            else:  # just a node for receivers.
                observer = HFDLObserverNode(settings)
            try:
                runner.run(async_observe(observer))
            except asyncio.CancelledError:
                logger.error('Observer loop cancelled')
                try:
                    runner.run(observer.shutdown())
                    runner.run(observer.stop())
                except asyncio.CancelledError:
                    logger.error(f'could not kill all the things: {list(asyncio.all_tasks())}')
                except RecursionError:
                    logger.error(f'could not kill all the things: {list(asyncio.all_tasks())}')

        logger.info("HFDLObserver done.")
    except Exception as exc:
        logger.error("Fatal error encountered", exc_info=exc)
    finally:
        util.thread_local.runner = None
        util.thread_local.loop = None
        logger.info('HFDLObserver exiting.')


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
@click.option('--node', help='run as node only to connect with a remote Observer (implies headless)', is_flag=True)
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
    ), default=None,
)
@click.option('--trace', hidden=True, is_flag=True)
def command(
    headless: bool, debug: bool, node: bool, log: Optional[pathlib.Path], config: Optional[pathlib.Path], trace: bool
) -> None:
    global TRACEMALLOC
    TRACEMALLOC = trace

    hfdl_observer.settings.load(config or (pathlib.Path(__file__).parent.parent / 'settings.yaml'))
    # old_settings.load(config or (pathlib.Path(__file__).parent.parent / 'settings.yaml'))
    handler = logging.handlers.TimedRotatingFileHandler(log, when='d', interval=1) if log else None
    if handler is not None:
        handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s'))

    # if not executed in a tty-like thing, headless is forced.
    try:
        headless = headless or node or not sys.stdout.isatty()
        if headless:
            setup_logging(handler, debug)
            observe(as_controller=not node)
        else:
            import cui
            cui.screen(handler, debug)
    except Exception as exc:
        # will this catch the annoying libzmq assertion failures?
        print(f'exiting due to exception: {exc}')
        print(str(exc.__traceback__))


if __name__ == '__main__':
    command()

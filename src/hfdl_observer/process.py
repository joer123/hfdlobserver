# hfdl_observer/hfdl.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import asyncio.subprocess
import contextlib
import os
import re
import shlex
import signal

from copy import copy
from typing import Any, Awaitable, Callable, Coroutine, Optional

import logging

import hfdl_observer.util as util


logger = logging.getLogger(__name__)


class Command:
    process: Optional[asyncio.subprocess.Process] = None
    execution_event: Optional[asyncio.Event] = None
    killed: bool = False
    terminated: bool = False
    executor: Optional[Awaitable] = None
    unrecoverable_errors: list[str]
    recoverable_errors: list[str]
    recoverable_error_count: int = 0
    recoverable_error_limit: int = 10
    fire_once: bool = True

    def __init__(
        self,
        parent_logger: logging.Logger,
        cmd: list[str],
        execution_arguments: dict = {},
        execution_context_manager: Optional[Callable] = None,
        shell: bool = False,
        fire_once: bool = True,
        on_prepare: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
        on_running: Optional[Callable[[asyncio.subprocess.Process, Any], None]] = None,
        on_exited: Optional[Callable[[int | None], None]] = None,
        recoverable_errors: Optional[list[str]] = None,
        unrecoverable_errors: Optional[list[str]] = None,
        valid_return_codes: Optional[list[int]] = None,
    ) -> None:
        self.cmd = cmd
        self.execution_arguments = copy(execution_arguments)
        self.prepared_condition = asyncio.Condition()
        self.running_condition = asyncio.Condition()
        self.exited_condition = asyncio.Condition()
        self.logger = parent_logger
        self.shell = shell
        self.fire_once = fire_once
        if execution_context_manager:
            self.execution_context_manager = execution_context_manager
        else:
            self.execution_context_manager = self.noop_execution_context_manager
        self.on_prepare = on_prepare
        self.on_running = on_running
        self.on_exited = on_exited
        self.unrecoverable_errors = unrecoverable_errors or []
        self.recoverable_errors = recoverable_errors or []
        self.valid_return_codes = valid_return_codes or [0]
        self.process_logger = logger

    @contextlib.contextmanager
    def noop_execution_context_manager(self) -> Any:
        # no op.
        yield None

    async def execute(self) -> None:
        if self.execution_event:
            self.logger.warning('Command has been executed, joining its execution')
            await self.execution_event.wait()
        self.execution_event = asyncio.Event()
        self.logger.debug('executing command')
        try:
            while not self.killed:
                # execution context allows you to set up such things as temp fifos for stdout parsing or managing
                # piping/resources between processes/awaitables. Basically anything that can't go into the on_prepare
                # and on_execute callback hooks.
                retcode: int | None = None
                with self.execution_context_manager() as execution_context:
                    if self.on_prepare:
                        await self.on_prepare()
                    async with self.prepared_condition:
                        self.prepared_condition.notify_all()

                    self.logger.debug('gathering options')
                    exec_args = {
                        'stderr': asyncio.subprocess.PIPE
                    }
                    exec_args.update(self.execution_arguments)
                    self.logger.info(f'starting {self}')
                    self.logger.debug(f'$ `{shlex.join(self.cmd)}`')
                    self.logger.debug(f'  {exec_args}')
                    self.recoverable_error_count = 0
                    if self.shell:
                        shell_cmd = shlex.join(self.cmd)
                        process = (
                            await asyncio.subprocess.create_subprocess_shell(shell_cmd, **exec_args)  # type: ignore
                        )
                    else:
                        process = (
                            await asyncio.subprocess.create_subprocess_exec(*self.cmd, **exec_args)  # type: ignore
                        )
                    self.process = process
                    self.process_logger = self.logger.getChild(f'pid#{process.pid}')
                    self.process_logger.debug('process started')

                    if self.on_running:
                        self.on_running(process, execution_context)
                    async with self.running_condition:
                        self.running_condition.notify_all()

                    try:
                        self.process_logger.debug('starting error watcher')
                        watcher_task = util.schedule(self.watch_stderr(process.pid, process.stderr))
                        retcode = await process.wait()
                        if retcode not in self.valid_return_codes and not (self.terminated or self.killed):
                            self.process_logger.error(f'exited with {retcode}')
                        else:
                            self.process_logger.info(f'exited with {retcode}')

                    except Exception as e:
                        self.process_logger.error(f'process {process.pid} aborted.', exc_info=e)
                        raise
                    finally:
                        if self.on_exited is not None:
                            self.process_logger.debug('exited')
                            self.on_exited(retcode)
                        async with self.exited_condition:
                            self.exited_condition.notify_all()
                        await util.cleanup_task(watcher_task)
                    if self.fire_once:
                        break

                self.logger.debug('execution context freed')
        except Exception as e:
            self.logger.error('encountered an error', exc_info=e)
        finally:
            self.execution_event.set()
            self.logger.debug('run completed')
            self.process = None

    async def terminate(self, process: Optional[asyncio.subprocess.Process] = None) -> None:
        execution_event = self.execution_event
        process = process or getattr(self, 'process', None)
        if process and execution_event and not execution_event.is_set():
            self.terminated = True
            self.process_logger.info('terminating')
            try:
                process.terminate()
                if execution_event is not None:
                    await asyncio.wait_for(execution_event.wait(), timeout=3)
            except asyncio.TimeoutError:
                self.process_logger.warning('process did not end cleanly. killing.')
                await self.kill(process)
                self.process_logger.info('kill completed')
            except asyncio.CancelledError:
                self.process_logger.info('termination cancelled')
            except ProcessLookupError as e:
                self.process_logger.warning(f'problem terminating: {e}')
        else:
            self.process_logger.debug('no process, cannot terminate.')

    async def kill(self, process: Optional[asyncio.subprocess.Process] = None) -> None:
        execution_event = self.execution_event
        process = process or getattr(self, 'process', None)
        if process and execution_event and not execution_event.is_set():
            self.process_logger.warning('killing')
            self.killed = True
            try:
                process.kill()
                if execution_event is not None:
                    await asyncio.wait_for(execution_event.wait(), timeout=3)
                self.process_logger.info('process kill returned')
            except asyncio.TimeoutError:
                self.process_logger.warning('process may be zombified. This cannot yet be handled.')
                if process is not None:
                    try:
                        os.kill(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            except asyncio.CancelledError:
                self.process_logger.info('kill cancelled')
            except ProcessLookupError as e:
                self.process_logger.warning(f'problem killing: {e}')
        else:
            self.process_logger.debug('no process, cannot kill.')

    async def watch_stderr(self, pid: int, stream: Optional[asyncio.StreamReader]) -> None:
        if stream:
            stream_logger = self.process_logger
            try:
                async for data in stream:
                    try:
                        line = data.decode('utf8').rstrip()
                    except UnicodeDecodeError:
                        continue
                    stream_logger.info(line)
                    if any(re.search(pattern, line) for pattern in self.unrecoverable_errors):
                        stream_logger.warning(f'encountered unrecoverable error: "{line}".')
                        # terminate, subclasses can restart process if desired.
                        await self.terminate()
                        break
                    if any(re.search(pattern, line) for pattern in self.recoverable_errors):
                        self.recoverable_error_count += 1
                        stream_logger.debug(f'recoverable error detected. Current count {self.recoverable_error_count}')
                        if self.recoverable_error_count > self.recoverable_error_limit:
                            stream_logger.warning(f'received too many recoverable errors `{line}`.')
                            await self.terminate()
                            break
                    if not self.process:
                        break
                    await asyncio.sleep(0)
            except OSError as err:
                if util.is_bad_file_descriptor(err):
                    # file was closed elsewhere. should mean the watched process exited as well.
                    self.logger.info(f'ignoring {err} on error watcher')
                    await self.terminate()
                else:
                    raise
        (stream_logger or self.logger).info(f'finished watching {stream}')

    def reset_recoverable_error_count(self, *_: Any) -> None:
        self.recoverable_error_count = 0


class ProcessHarness:
    logger: logging.Logger
    settle_time: float = 0
    backoff_time: float = 0
    command: Optional[Command] = None

    def __init__(self) -> None:
        self.logger = logging.getLogger(str(self))

    def create_command(self) -> Command:
        raise NotImplementedError()

    async def on_prepare(self) -> None:
        if self.settle_time:
            self.logger.info(f'settling for {self.settle_time} seconds')
            await asyncio.sleep(self.settle_time)
        if self.backoff_time:
            self.logger.info(f'additional (one-time) backoff {self.backoff_time} seconds')
            await asyncio.sleep(self.backoff_time)
            self.backoff_time = 0

    def on_execute(self, process: asyncio.subprocess.Process, context: Any) -> None:
        pass

    def cleanup(self) -> None:
        self.command = None

    async def start(self) -> None:
        if self.command:
            await self.command.kill()
        command: Command = self.create_command()
        self.command = command
        try:
            await command.execute()
        finally:
            self.cleanup()

    async def stop(self) -> None:
        if self.command:
            await self.command.terminate()
        return None

    async def kill(self) -> None:
        if self.command:
            await self.command.kill()
        return None

    def reset_recoverable_error_count(self, *_: Any) -> None:
        self.recoverable_error_count = 0

# hfdl_observer/hfdl.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import asyncio.subprocess
import contextlib
import re
import shlex

from copy import copy
from typing import Any, Callable, Coroutine, Optional

import logging

import hfdl_observer.util as util


logger = logging.getLogger(__name__)


class Command:
    process: asyncio.subprocess.Process
    killed: bool = False
    terminated: bool = False
    task: Optional[asyncio.Task] = None
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
        on_exited: Optional[Callable[[], None]] = None,
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

    # async def on_prepare(self) -> None:
    #     pass

    # def on_running(self, process: asyncio.subprocess.Process, execution_context: Any) -> None:
    #     pass

    # def on_exited(self) -> None:
    #     pass

    @contextlib.contextmanager
    def noop_execution_context_manager(self) -> Any:
        # no op.
        yield None

    async def execute(self) -> None:
        if self.task:
            raise Exception('Command has already been run.')
        self.logger.debug('executing command')
        self.task = asyncio.current_task()
        try:
            while not self.killed:
                # execution context allows you to set up such things as temp fifos for stdout parsing or managing
                # piping between processes.
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
                        asyncio.get_running_loop().create_task(self.watch_stderr(process.pid, process.stderr))
                        retcode = await process.wait()
                        if retcode not in self.valid_return_codes and not (self.terminated or self.killed):
                            self.process_logger.error(f'exited with {retcode}')
                        else:
                            self.process_logger.info(f'exited with {retcode}')
                    except Exception as e:
                        self.process_logger.error(f'process {process.pid} aborted.', exc_info=e)
                        raise
                    finally:
                        if self.on_exited:
                            self.process_logger.debug('exited')
                            self.on_exited()
                        async with self.exited_condition:
                            self.exited_condition.notify_all()
                    if self.fire_once:
                        break

                self.logger.debug('execution context freed')
        except Exception as e:
            self.logger.error('encountered an error', exc_info=e)
            raise
        finally:
            self.logger.debug('run completed')

    async def terminate(self) -> Optional[asyncio.Task]:
        task = self.task
        process = getattr(self, 'process', None)
        if process:
            self.terminated = True
            self.process_logger.info('terminating')
            try:
                process.terminate()
                if task is not None:
                    await asyncio.wait_for(task, timeout=3)
            except asyncio.TimeoutError:
                self.process_logger.warning('process did not end cleanly. killing.')
                await self.kill()  # SUS: have to ensure at this point we're killing the right process.
                self.process_logger.info('kill completed')
            except ProcessLookupError as e:
                self.process_logger.warning('problem', exc_info=e)
        else:
            self.process_logger.debug('no process, cannot terminate.')
        return task

    async def kill(self) -> Optional[asyncio.Task]:
        task = self.task
        # SUS may need more protection on killing the correct process
        process = getattr(self, 'process', None)
        if process:
            self.process_logger.warning('killing')
            self.killed = True
            try:
                process.kill()
                if task is not None:
                    await asyncio.wait_for(task, timeout=3)
                    self.process_logger.info('process kill returned')
            except ProcessLookupError as e:
                self.process_logger.warning('problem', exc_info=e)
        else:
            self.process_logger.debug('no process, cannot kill.')
        return task

    def start(self) -> asyncio.Task:
        if not self.task:
            self.logger.debug("starting command task")
            task = asyncio.get_running_loop().create_task(self.execute())
            return task
        return self.task

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
    command: Optional[Command] = None

    def __init__(self) -> None:
        self.logger = logging.getLogger(str(self))

    def create_command(self) -> Command:
        raise NotImplementedError()

    async def on_prepare(self) -> None:
        if self.settle_time:
            self.logger.info(f'settling for {self.settle_time} seconds')
            await asyncio.sleep(self.settle_time)

    def on_execute(self, process: asyncio.subprocess.Process, context: Any) -> None:
        pass

    def cleanup(self, _: Any) -> None:
        self.command = None

    async def start(self) -> asyncio.Task:
        if self.command:
            await self.command.kill()
        command: Command = self.create_command()
        self.command = command
        task = command.start()
        task.add_done_callback(self.cleanup)
        return task

    async def stop(self) -> Optional[asyncio.Task]:
        if self.command:
            return await self.command.terminate()
        return None

    async def kill(self) -> Optional[asyncio.Task]:
        if self.command:
            return await self.command.kill()
        return None

    def reset_recoverable_error_count(self, *_: Any) -> None:
        self.recoverable_error_count = 0


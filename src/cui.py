# cui.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import collections
import functools
import datetime
import logging

from typing import Any, Generic, Callable, Iterable, Optional, Sequence, TypeVar, Union

import rich.console
import rich.highlighter
import rich.layout
import rich.live
import rich.logging
import rich.markdown
import rich.style
import rich.table
import rich.text

import hfdl_observer.bus as bus
import hfdl_observer.heat as heat
import hfdl_observer.hfdl as hfdl
import hfdl_observer.manage as manage
import hfdl_observer.network as network
import hfdl_observer.settings as settings
import hfdl_observer.util as util

import hfdlobserver

logger = logging.getLogger(__name__)
start = datetime.datetime.now()
SCREEN_REFRESH_RATE = 2
MAP_REFRESH_PERIOD = 32.0 / 19.0  # every HFDL slot

PANE_BAR = rich.style.Style.parse('bright_white on bright_black')
SUBDUED_TEXT = rich.style.Style.parse('grey50 on black')
NORMAL_TEXT = rich.style.Style.parse('white on black')
PROMINENT_TEXT = rich.style.Style.parse('bright_white on black')
BASIC_CELL_STYLE = rich.style.Style.parse('bright_black on black')


CellText = tuple[str | None, str | rich.style.Style | None]


class ObserverDisplay:
    status: Optional[rich.table.Table] = None
    totals: Optional[rich.table.Table] = None
    counts: Optional[rich.table.Table] = None
    tty_bar: Optional[rich.table.Table] = None
    tty: Optional[rich.table.Table] = None
    forecast: rich.text.Text
    uptime_text: rich.text.Text
    totals_text: rich.text.Text
    garbage: collections.deque[rich.table.Table]

    def __init__(
        self,
        console: rich.console.Console,
        heatmap: 'HeatMap',
        cumulative_line: 'CumulativeLine',
        forecaster: bus.RemoteURLRefresher,
    ) -> None:
        self.garbage = collections.deque()
        self.console = console
        self.heatmap = heatmap
        self.cumulative_line = cumulative_line
        self.root = rich.layout.Layout("HFDL Observer")
        self.heatmap.display = self
        self.cumulative_line.display = self
        self.uptime_text = rich.text.Text("STARTING")
        self.forecast = rich.text.Text('(space weather unavailable)')
        self.setup_status()
        self.totals_text = rich.text.Text('', style='white on black')
        self.setup_totals()
        self.update_status()
        self.update_tty_bar()
        forecaster.subscribe('response', self.on_forecast)

    def update(self) -> None:
        t = rich.table.Table.grid(expand=True, pad_edge=False, padding=(0, 0))
        if self.status:
            t.add_row(self.status)
        if self.totals:
            t.add_row(self.totals)
        if self.counts:
            t.add_row(self.counts)
        if self.tty:
            if self.tty_bar:
                t.add_row(self.tty_bar)
            t.add_row(self.tty)
        if t.row_count:
            self.root.update(t)

    def setup_status(self) -> None:
        if self.status:
            self.garbage.append(self.status)
        table = rich.table.Table.grid(expand=True)
        table.add_column()
        table.add_column(justify="center")
        table.add_column(justify="right")
        text = rich.text.Text()
        text.append(' 📡 ')
        text.append('HFDL Observer', style='bold')
        table.add_row(text, self.forecast, self.uptime_text, style='on dark_green')
        self.status = table

    def setup_totals(self) -> None:
        if self.totals:
            self.garbage.append(self.totals)
        table = rich.table.Table.grid(expand=True)
        table.add_column()  # title
        table.add_column(justify='right')   # Grand Total
        table.add_row(
            rich.text.Text(" Totals (since start)", style='bold bright_white'),
            self.totals_text,
            style='white on black'
        )
        self.totals = table

    def update_status(self) -> None:
        if not hasattr(self, 'uptime_text'):
            return
        uptime = datetime.datetime.now() - start
        uptime -= datetime.timedelta(0, 0, uptime.microseconds)
        self.uptime_text.plain = f'UP {uptime}'

    def update_tty_bar(self) -> None:
        if self.tty_bar:
            self.garbage.append(self.tty_bar)
        table = rich.table.Table.grid(expand=True)
        table.add_row(' 📰 Log', style=PANE_BAR)
        self.tty_bar = table

    def update_totals(self, cumulative: network.CumulativePacketStats) -> None:
        actives = str(self.cumulative_line.active) if self.cumulative_line.active is not None else '?'
        targets = str(self.cumulative_line.target_observed) if self.cumulative_line.target_observed is not None else '?'
        untargets = f' +{self.cumulative_line.bonus_observed}' if self.cumulative_line.bonus_observed else ''
        self.totals_text.plain = (
            f"⏬{cumulative.from_air} ⏫{cumulative.from_ground}  "
            f"|  🌐{cumulative.with_position} ❔{cumulative.no_position}  "
            f"|  📰{cumulative.squitters}  "
            f"|  🔎{targets}/{actives}{untargets}  "
            f"|  📶{cumulative.packets}  "
        )

    def update_log(self, ring: collections.deque) -> None:
        # WARNING: do not use any logger from within this method.
        if self.tty:
            self.garbage.append(self.tty)
        table = rich.table.Table.grid(expand=True)
        available_space = (
            self.current_height
            - (self.counts.row_count if self.counts else 0)
            - (self.status.row_count if self.status else 0)
            - (self.tty_bar.row_count if self.tty_bar else 0)
            - (self.totals.row_count if self.totals else 0)
            - 1  # trailing blank
        )
        if available_space > 0:
            entries = list(ring)[-available_space:]
            for row in entries:
                table.add_row(row)
            self.tty = table
        else:
            self.tty = None

    def update_counts(self, table: rich.table.Table) -> None:
        if table.row_count:
            if self.counts is not None:
                self.garbage.append(self.counts)
            self.counts = table

    def on_forecast(self, forecast: Any) -> None:
        try:
            styles = {
                "extreme": "yellow1 on dark_red",
                "severe": "black on red1",
                "strong": "black on dark_orange",
                "moderate": "black on orange1",
                "minor": "black on gold1",
                "none": "white on bright_black",
                None: "white on bright_black",
            }
            recent = forecast['-1']
            current = forecast['0']
            forecast1d = forecast['1']
            text = self.forecast
            text.plain = ''
            text.append(f'R{recent["R"]["Scale"] or "-"}', style=styles[recent["R"]["Text"]])
            text.append('|')
            text.append(f'S{recent["S"]["Scale"] or "-"}', style=styles[recent["S"]["Text"]])
            text.append('|')
            text.append(f'G{recent["G"]["Scale"] or "-"}', style=styles[recent["G"]["Text"]])
            text.append('  ')
            text.append(f'R{current["R"]["Scale"] or "-"}', style=styles[current["R"]["Text"]])
            text.append('|')
            text.append(f'S{current["S"]["Scale"] or "-"}', style=styles[current["S"]["Text"]])
            text.append('|')
            text.append(f'G{current["G"]["Scale"] or "-"}', style=styles[current["G"]["Text"]])
            text.append('  ')
            text.append(f'R{forecast1d["R"]["MinorProb"]}/{forecast1d["R"]["MajorProb"]}', styles["none"]),
            text.append('|')
            text.append(f'S{forecast1d["S"]["Prob"]}', styles["none"]),
            text.append('|')
            text.append(f'G{forecast1d["G"]["Scale"] or "-"}', styles[forecast1d["G"]["Text"]]),
        except Exception as err:
            logger.warning('ignoring forecaster error', exc_info=err)

    @property
    def current_width(self) -> int:
        return self.console.options.size.width

    @property
    def current_height(self) -> int:
        return self.console.options.size.height or 25

    def clear_table(self, table: Optional[rich.table.Table]) -> None:
        # dubious, attempt to voodoo patch a possible memory leak in Rich
        with self.root._lock:
            if table is not None:
                if hasattr(table.rows, 'clear'):
                    table.rows.clear()
                if hasattr(table.columns, 'clear'):
                    table.columns.clear()

    def on_render(self) -> None:
        while len(self.garbage) > 0:
            try:
                garbage = self.garbage.popleft()
            except IndexError:
                break
            self.clear_table(garbage)


class CumulativeLine:
    display: ObserverDisplay
    target_observed: Optional[int] = None
    bonus_observed: Optional[int] = None
    active: Optional[int] = None

    def register(self, observer: hfdlobserver.HFDLObserverController, cumulative: network.CumulativePacketStats) -> None:
        self.cumulative = cumulative
        cumulative.subscribe('update', self.on_update)
        observer.subscribe('active', self.on_active)
        observer.subscribe('observing', self.on_observing)

    def on_update(self, _: Any) -> None:
        if self.display:
            self.display.update_totals(self.cumulative)

    def on_observing(self, observed: tuple[Sequence[int], Sequence[int]]) -> None:
        targetted, untargetted = observed
        self.target_observed = len(targetted)
        self.bonus_observed = len(untargetted)

    def on_active(self, active_frequencies: Sequence[int]) -> None:
        self.active = len(active_frequencies)


STROKES: dict[int, None | str] = collections.defaultdict(lambda: None)
STROKES.update({0: '┇', 5: '¦'})


TableSourceT = TypeVar('TableSourceT', bound='heat.Table')


class AbstractHeatMapFormatter(Generic[TableSourceT]):
    source: TableSourceT
    strokes = STROKES

    @functools.cached_property
    def max_count(self) -> int:
        return max(
            max(c.value if c else 0 for c in row or [0]) for row in self.source.bins.values()  # type: ignore
        )

    @property
    def is_empty(self) -> bool:
        return len(self.source.bins) == 0

    @functools.cache
    def symbol(self, amount: int) -> str:
        # there are 13 slots per 32 second frame.
        # Assuming 1 minute bins and 1 packet per slot on average:
        # we should not expect more than 25 packets per minute
        # However, other bin sizes are possible as well so we have as many single character symbols as practical.
        if amount == 0:
            return '·'
        if amount < 10:
            return str(amount)
        if amount < 36:
            return chr(87 + amount)
        if amount < 62:
            return chr(29 + amount)
        return '✽'

    @functools.cache
    def style(self, amount: int) -> rich.style.Style:
        rgb = util.spectrum_colour(amount, max(25, self.max_count))
        return rich.style.Style(bgcolor=f'rgb({",".join(str(i) for i in rgb)})', color='black')
        # return rich.style.Style.parse(f'bright_black on rgb({",".join(str(i) for i in rgb)})')

    def cumulative(self, row: Sequence[heat.Cell]) -> CellText:
        return (f'{sum(cell.value for cell in row): >4}', None)

    def column_headers(self, root_str: str) -> list[CellText]:
        columns: list[CellText] = [
            (f" 📊 per {root_str}           "[:18], None),
            ('NOW', None)
        ]
        for i in range(1, len(self.source.column_headers)):
            title = self.strokes[i % 10] or ' '  # ] '┇    ¦    '[i % 10]
            columns.append((f' {title} ', None))
        columns.append((None, None))
        return columns

    def row_header(
        self, header: heat.RowHeader, row: Sequence[heat.Cell]
    ) -> CellText:
        raise NotImplementedError()

    def cell(
        self, index: int, cell: heat.Cell, row_header: heat.RowHeader
    ) -> CellText:
        style: Union[rich.style.Style, str] = BASIC_CELL_STYLE
        stroke = self.strokes[index % 10]
        if cell.value:
            style = self.style(cell.value)
            text = self.symbol(cell.value)
        else:
            text = stroke or "·"
        return (f' {text} ', style)

    def row(self, row_id: Union[str, int], row_data: Sequence[heat.Cell]) -> list[CellText]:
        row_header = self.source.row_headers[row_id]
        cells = [self.row_header(row_header, row_data)]
        cells.extend(self.cell(ix, cell, row_header) for ix, cell in enumerate(row_data))
        cells.append(self.cumulative(row_data))
        return cells

    def rows(self) -> Iterable[tuple[Union[int, str], Sequence[heat.Cell]]]:
        return list(row for row in self.source)


class HeatMapByFrequencyFormatter(AbstractHeatMapFormatter[heat.TableByFrequency]):
    def __init__(
        self,
        bin_size: int,
        num_bins: int,
        targetted: Sequence[int],
        untargetted: Sequence[int],
        all_active: bool,
        show_active_line: bool,
        show_confidence: bool,
        show_targetting: bool,
        show_quiet: bool,
    ) -> None:
        self.bin_size = bin_size
        self.source = heat.TableByFrequency(self.bin_size, num_bins)

        def rowheader_factory(key: Union[int, str], tags: Sequence[str]) -> heat.RowHeader:
            return heat.RowHeader(str(key), station_id=network.STATIONS[key].station_id, tags=tags)

        self.source.tag_rows(targetted, ['targetted'], default_factory=rowheader_factory)
        self.source.tag_rows(untargetted, ['untargetted'], default_factory=rowheader_factory)
        self.source.tag_rows(
            network.UPDATER.current_freqs(), ['active'], default_factory=rowheader_factory if all_active else None
        )
        self.show_active_line = show_active_line
        self.show_confidence = show_confidence
        self.show_targetting = show_targetting
        self.show_quiet = show_quiet
        self.source.fill_active_state()

    def row_header(
        self, header: heat.RowHeader, row: Sequence[heat.Cell]
    ) -> CellText:
        infix = ''
        style = NORMAL_TEXT
        if header.station_id:
            infix = network.STATION_ABBREVIATIONS[header.station_id]
        if header.is_tagged('targetted') or any(cell.is_tagged('targetted') for cell in row):
            if any(cell.value for cell in row):
                symbol = '▣'  # ▣🞔□⬚
                style = PROMINENT_TEXT
            else:
                symbol = '🞔'
        elif header.is_tagged('active') or any(cell.is_tagged('active') for cell in row):
            symbol = '□'
        else:
            symbol = '⬚'
            infix = infix.lower()
            style = SUBDUED_TEXT
        if not self.show_targetting:
            symbol = ' '
        stratum = ' '
        if self.show_confidence:
            if header.is_tagged('local'):
                stratum = '●'  # ◉
            elif header.is_tagged('network'):
                stratum = '◐'  # ◒⊙⬓
            elif header.is_tagged('guess'):
                stratum = '○'
        return (f'{symbol}{infix: >9}{header.label: >6} {stratum} ', style)

    def row(self, row_id: Union[str, int], row_data: Sequence[heat.Cell]) -> list[CellText]:
        if self.show_quiet or any(cell.value for cell in row_data):
            return super().row(row_id, row_data)
        return []

    def cell(
        self, index: int, cell: heat.Cell, row_header: heat.RowHeader
    ) -> CellText:
        style: Union[rich.style.Style, str] = BASIC_CELL_STYLE
        stroke = self.strokes[index % 10]
        if cell.value:
            style = self.style(cell.value)
            text = ' ' + self.symbol(cell.value) + ' '
        elif self.show_active_line and cell.is_tagged('active'):
            if row_header.is_tagged('targetted'):
                text = f'─{stroke or "─"}─'
            else:
                text = f'⠒{stroke or "⠒"}⠒'  # ┄┄┄' # ╴╴╴'  # ┈┈┈
        else:
            text = f' {stroke or "·"} '
        return (text, style)


class HeatMapByBandFormatter(AbstractHeatMapFormatter[heat.TableByBand]):
    def __init__(
        self,
        bin_size: int,
        num_bins: int,
        all_bands: bool,
    ) -> None:
        self.bin_size = bin_size
        self.source = heat.TableByBand(self.bin_size, num_bins)

        def rowheader_factory(key: Union[int, str], tags: Sequence[str]) -> heat.RowHeader:
            return heat.RowHeader(str(key), tags=tags)

        if all_bands:
            bands: set[int] = set()
            for allocated in network.STATIONS.assigned().values():
                bands.update(int(f // 1000) for f in allocated)
            self.source.tag_rows(bands, ['band'], default_factory=rowheader_factory)

    def row_header(
        self, header: heat.RowHeader, row: Sequence[heat.Cell]
    ) -> CellText:
        if any(cell.value for cell in row):
            style = PROMINENT_TEXT
        else:
            style = NORMAL_TEXT
        return (f' {header.label: >13} MHz ', style)


class HeatMapByStationFormatter(AbstractHeatMapFormatter[heat.TableByStation]):
    def __init__(
        self,
        bin_size: int,
        num_bins: int,
    ) -> None:
        self.bin_size = bin_size
        self.source = heat.TableByStation(self.bin_size, num_bins)

    def row_header(
        self, header: heat.RowHeader, row: Sequence[heat.Cell]
    ) -> CellText:
        if any(cell.value for cell in row):
            style = PROMINENT_TEXT
        else:
            style = NORMAL_TEXT
        return (f' {header.label.split(",", 1)[0].strip(): >17} ', style)


class HeatMapByAgentFormatter(AbstractHeatMapFormatter[heat.TableByAgent]):
    def __init__(
        self,
        bin_size: int,
        num_bins: int,
    ) -> None:
        self.bin_size = bin_size
        self.source = heat.TableByAgent(self.bin_size, num_bins)

    def row_header(
        self, header: heat.RowHeader, row: Sequence[heat.Cell]
    ) -> CellText:
        if any(cell.value for cell in row):
            style = PROMINENT_TEXT
        else:
            style = NORMAL_TEXT
        return (f' {header.label: >17} ', style)


class HeatMapByReceiverFormatter(AbstractHeatMapFormatter[heat.TableByReceiver]):
    def __init__(
        self,
        bin_size: int,
        num_bins: int,
        proxies: list[manage.ReceiverProxy],
    ) -> None:
        self.bin_size = bin_size
        self.source = heat.TableByReceiver(self.bin_size, num_bins)

    def row_header(
        self, header: heat.RowHeader, row: Sequence[heat.Cell]
    ) -> CellText:
        if any(cell.value for cell in row):
            style = PROMINENT_TEXT
        else:
            style = NORMAL_TEXT
        label = header.label[-16:]
        return (f' {label: >17} ', style)


class HeatMap:
    config: dict
    last_render_time: float = 0
    bin_size: int = 60
    data_source: Callable[[int], AbstractHeatMapFormatter]
    display: ObserverDisplay
    refresh_period = 64  # two frames.
    task: Optional[asyncio.Task] = None
    targetted_frequencies: Sequence[int]
    untargetted_frequencies: Sequence[int]

    def __init__(self, config: dict) -> None:
        self.config = config
        mode = self.config.get("display_mode", "frequency")
        callable_mode = getattr(self, f'by_{mode}', None)
        if not callable(callable_mode):
            raise ValueError(f'display mode not supported: {mode}')
        self.data_source = callable_mode
        self.bin_size = min(3600, max(60, int(config.get('bin_size', 60))))
        self.refresh_period = min(self.refresh_period, self.bin_size)
        self.targetted_frequencies = []
        self.untargetted_frequencies = []

    def by_frequency(self, num_bins: int) -> AbstractHeatMapFormatter:
        return HeatMapByFrequencyFormatter(
            self.bin_size,
            num_bins,
            self.targetted_frequencies,
            self.untargetted_frequencies,
            util.tobool(self.config.get('show_all_active', False)),
            util.tobool(self.config.get('show_active_line', True)),
            util.tobool(self.config.get('show_confidence', True)),
            util.tobool(self.config.get('show_targetting', True)),
            util.tobool(self.config.get('show_quiet', True)),
        )

    def by_band(self, num_bins: int) -> AbstractHeatMapFormatter:
        return HeatMapByBandFormatter(
            self.bin_size,
            num_bins,
            util.tobool(self.config.get('show_all_bands', True))
        )

    def by_station(self, num_bins: int) -> AbstractHeatMapFormatter:
        return HeatMapByStationFormatter(self.bin_size, num_bins)

    def by_agent(self, num_bins: int) -> AbstractHeatMapFormatter:
        return HeatMapByAgentFormatter(self.bin_size, num_bins)

    def by_receiver(self, num_bins: int) -> AbstractHeatMapFormatter:
        return HeatMapByReceiverFormatter(self.bin_size, num_bins, list(self.observer.proxies.values()))

    def register(self, observer: hfdlobserver.HFDLObserverController) -> None:
        self.observer = observer
        observer.subscribe('packet', self.on_hfdl)
        observer.subscribe('observing', self.on_observing)

    def on_hfdl(self, packet: hfdl.HFDLPacketInfo) -> None:
        if not self.task:
            self.start()
        self.maybe_render()

    def on_observing(self, observed: tuple[list[int], list[int]]) -> None:
        self.targetted_frequencies, self.untargetted_frequencies = observed
        if not self.task:
            self.start()

    def maybe_render(self) -> None:
        now = util.now().timestamp()
        if now - self.last_render_time > MAP_REFRESH_PERIOD:
            self.render()

    def celltexts_to_text(self, texts: list[CellText], style: Optional[rich.style.Style] = None) -> rich.text.Text:
        elements: list[tuple[str, str | rich.style.Style | None]] = []
        for celltext in texts:
            text, textstyle = (celltext[0] if celltext[0] else '   ', celltext[1])
            if elements and elements[-1][1] == textstyle:   # if the styles are the same, they can be merged.
                elements[-1] = (elements[-1][0] + text, textstyle)
            else:
                elements.append((text, textstyle))
        result = rich.text.Text(style=style or '')
        for element in elements:
            result.append(*element)
        return result

    def render(self) -> None:
        # Hypothetically, we could represent each bin/ on each row as a "cell" Rich's table display. This would be
        # slightly less complex on code in this module. Rich is a good framework, but it is generic and does a lot of
        # extra work to figure out the layout when it's already statically known. Instead, this code (and its helpers)
        # combines fragments into the smallest number of Text segments possible, and represents each row on the table
        # with a single Text. Not ideal, but the concept of Cells is retained as it its flexibility
        #
        self.last_render_time = util.now().timestamp()
        width = self.display.current_width - (3 + 9 + 6 + 4 + 1)
        possible_bins = width // 3
        if self.bin_size > 60:
            bin_str = f'{self.bin_size}s'
        else:
            bin_str = 'minute'
        source = self.data_source(possible_bins)
        if not source.is_empty:
            table = rich.table.Table.grid()

            columns = source.column_headers(bin_str)
            header_text = self.celltexts_to_text(columns)
            table.add_row(header_text, style=PANE_BAR)

            for key, row in source.rows():
                cells = source.row(key, row)
                if cells:
                    row_text = self.celltexts_to_text(cells, None)
                    table.add_row(row_text, style="white on black")
            # for reasons I don't understand, source.source will not get garbage collected. Since we're done with the
            # source, it can be del'd, but it still feels dirty. del'ing `source` alone is insufficient.
            del source.source
            del columns
        else:
            table = rich.table.Table.grid(expand=True)
            table.add_row(f" 📊 per {bin_str}", style=PANE_BAR)
            table.add_row(" Awaiting data...")
        del source
        self.display.update_counts(table)

    async def run(self) -> None:
        try:
            while True:
                self.render()
                await asyncio.sleep(self.refresh_period)
        except Exception as exc:
            logger.error('oops', exc_info=exc)
            raise

    def stop(self) -> None:
        if self.task:
            self.task.cancel()
            self.task = None

    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        if not self.task:
            if loop:
                self.task = loop.create_task(self.run())
            else:
                self.task = util.schedule(self.run())


class ConsoleRedirector(rich.console.Console):
    ring: collections.deque
    output: Optional[Callable[[collections.deque], None]] = None

    def print(self, something: Any) -> None:  # type: ignore   # shut up, mypy.
        self.ring.append(something)
        if self.output is not None:
            self.output(self.ring)
        else:
            super().print(something)

    def ensure_size(self, size: int) -> None:
        if self.ring.maxlen and size > self.ring.maxlen:
            self.ring = collections.deque(self.ring, size)

    @classmethod
    def create(cls, size: int) -> 'ConsoleRedirector':
        that = cls()
        that.ring = collections.deque(maxlen=size)
        return that


class RichLive(rich.live.Live):
    pre_refresh: Optional[Sequence[Callable]] = None
    post_refresh: Optional[Sequence[Callable]] = None

    def refresh(self) -> None:
        with self._lock:
            for callback in self.pre_refresh or []:
                callback()
            try:
                super().refresh()
            except AssertionError as err:
                logger.warning(f'ignoring {err}')
            for callback in self.post_refresh or []:
                callback()


def screen(loghandler: Optional[logging.Handler], debug: bool = True) -> None:
    cui_settings = settings.cui
    console = rich.console.Console()
    console.clear()
    logging_console = ConsoleRedirector.create(max(console.options.size.height or 50, 50))
    logging_handler = rich.logging.RichHandler(
        console=logging_console,
        show_time=True,
        highlighter=rich.highlighter.NullHighlighter(),
        enable_link_path=False,
    )
    ticker = HeatMap(cui_settings['ticker'])
    cumulative_line = CumulativeLine()

    forecaster = bus.RemoteURLRefresher('https://services.swpc.noaa.gov/products/noaa-scales.json', 617)

    display = ObserverDisplay(console, ticker, cumulative_line, forecaster)

    # setup logging
    logging_console.output = display.update_log
    FORMAT = "%(message)s"
    handlers: list[logging.Handler] = [logging_handler]
    if loghandler:
        handlers.append(loghandler)
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format=FORMAT,
        datefmt="[%X]",
        handlers=handlers,
        force=True
    )
    display_updater = bus.PeriodicCallback(
        1.0 / SCREEN_REFRESH_RATE,
        [display.update_status, display.update],
        False
    )

    def observing(
        observer: hfdlobserver.HFDLObserverController,
        cumulative: network.CumulativePacketStats,
    ) -> None:
        ticker.register(observer)
        cumulative_line.register(observer, cumulative)
        util.schedule(forecaster.run())
        util.schedule(display_updater.run())

    with RichLive(
        display.root, refresh_per_second=SCREEN_REFRESH_RATE, console=console, transient=True, screen=True,
        redirect_stderr=False, redirect_stdout=False, vertical_overflow="crop",
    ) as live:
        live.pre_refresh = [  # type: ignore[attr-defined]
            # display.update_status,
            # display.update,
        ]
        live.post_refresh = [  # type: ignore[attr-defined]
            display.on_render,
        ]
        hfdlobserver.observe(on_observer=observing)


if __name__ == '__main__':
    screen(None)

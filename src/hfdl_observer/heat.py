# hfdl_observer/heat.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import datetime
import logging
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence

import hfdl_observer.data as data
import hfdl_observer.network as network
import hfdl_observer.util


logger = logging.getLogger(__name__)


class Taggable:
    _tags: set[str] | None = None

    def tag(self, tag: str) -> None:
        if self._tags is None:
            self._tags = set()
        self._tags.add(tag)

    def is_tagged(self, tag: str) -> bool:
        return self._tags is not None and tag in self._tags

    def tags_as_str(self) -> str:
        tags = "".join(t[0] for t in self._tags or [])
        if tags:
            return f'[{tags}]'
        return ''


class RowHeader(Taggable):
    label: str = ''
    station_id: Optional[int] = None

    def __init__(self, label: str, station_id: Optional[int] = None, tags: Optional[Sequence[str]] = None) -> None:
        self.label = label
        for tag in tags or []:
            self.tag(tag)
        self.station_id = station_id

    def __str__(self) -> str:
        if self.station_id:
            sid = f'#{self.station_id}:'
        else:
            sid = ''
        return f'{self.tags_as_str()} {sid}{self.label}'


class ColumnHeader:
    index: int
    label: str
    when: datetime.datetime
    size: int
    offset: int

    def __init__(self, index: int, when: datetime.datetime, size: int) -> None:
        self.index = index
        self.when = when
        self.size = size
        self.offset = index * size
        self.label = str(index)

    def __str__(self) -> str:
        if self.offset:
            return f'{self.label}@{self.offset}'
        return 'NOW'


class Cell(Taggable):
    value: int

    def __init__(self, value: int, tags: Optional[Sequence[str]] = None) -> None:
        self.value = value

    def __str__(self) -> str:
        return f'{self.value}{self.tags_as_str()}'


DataRows = dict[int | str, Sequence[Cell]]


class Table:
    column_headers: Sequence[ColumnHeader]
    row_headers: dict[int | str, RowHeader]
    bins: DataRows

    def __init__(
        self, counts: Mapping[int | str, Sequence[int]], bin_size: int, start: Optional[datetime.datetime] = None
    ) -> None:
        self.bins = {k: [Cell(v) for v in r] for k, r in counts.items()}
        self.row_headers = {k: RowHeader(str(k)) for k in counts.keys()}
        when = start if start is not None else hfdl_observer.util.now()
        if counts:
            num_columns = max(len(r) for r in counts.values())
            self.column_headers = [
                ColumnHeader(n, when - datetime.timedelta(seconds=n * bin_size), bin_size) for n in range(num_columns)
            ]
        else:
            self.column_headers = []

    def rows_matching(self, condition: Callable[[int | str, Sequence[Cell]], bool]) -> DataRows:
        out = {}
        for k, cells in self:
            if callable(condition) and condition(k, cells):
                out[k] = cells
        return out

    def tag_rows(
        self,
        keys: Iterable[int | str],
        tags: Optional[Sequence[str]],
        default_factory: Optional[Callable[[int | str, Sequence[str]], RowHeader]] = None,
    ) -> None:
        for key in keys:
            if key in self.bins:
                for tag in tags or []:
                    self.row_headers[key].tag(tag)
            elif default_factory:
                self.bins[key] = [Cell(0) for col in self.column_headers]
                self.row_headers[key] = default_factory(key, tags or [])

    def key_for_row(self, row_id: int | str) -> Any:
        # default sorts by the row_id itself.
        return row_id

    def __iter__(self) -> Iterator[tuple[int | str, Sequence[Cell]]]:
        order = sorted(self.bins.keys(), key=self.key_for_row)
        for k in order:
            yield (k, self.bins[k])

    def __str__(self) -> str:
        out = []
        out.append('\t' + '\t'.join(str(header) for header in self.column_headers))
        for k, cells in self:
            cells_text = "\t".join(str(cell) for cell in cells)
            out.append('\t'.join([str(self.row_headers[k]), cells_text]))
        return '\n'.join(out)


class TableByFrequency(Table):

    def __init__(self, bin_size: int, num_bins: int) -> None:
        packets = data.PACKET_WATCHER.packets_by_frequency(bin_size, num_bins)
        super().__init__(packets, bin_size)

    def fill_active_state(self) -> None:
        for ix, column in enumerate(self.column_headers):
            when = column.when if column.index else None  # column 0 is "NOW", which triggers different active logic.
            column_active: dict[int, network.StationAvailability] = {}
            active = network.UPDATER.active_for_frame(when)
            for a in active:
                for f in a.frequencies:
                    column_active[f] = a
            for freq, cells in self.bins.items():
                cell = cells[ix]
                row_header = self.row_headers[freq]
                station = column_active.get(int(freq), None)

                if station:
                    row_header.station_id = station.station_id
                    if station.frequencies and freq in station.frequencies:
                        cell.tag('active')
                        row_header.tag('active')
                    match (station.stratum):
                        case (network.Strata.SELF.value):
                            row_header.tag('local')
                        case (network.Strata.SQUITTER.value):
                            row_header.tag('network')
                        case (None):
                            pass
                        case (_):
                            row_header.tag('guess')
                else:
                    row_header.station_id = network.STATIONS[freq].station_id


class TableByBand(Table):
    def __init__(self, bin_size: int, num_bins: int) -> None:
        packets = data.PACKET_WATCHER.packets_by_band(bin_size, num_bins)
        super().__init__(packets, bin_size)


class TableByStation(Table):
    def __init__(self, bin_size: int, num_bins: int) -> None:
        packets = data.PACKET_WATCHER.packets_by_station(bin_size, num_bins)
        super().__init__(packets, bin_size)
        for k, rh in self.row_headers.items():
            rh.station_id = int(k)
            rh.label = f'#{k}. {network.STATIONS[k].station_name}'

    def key_for_row(self, row_id: int | str) -> Any:
        # we need to sort by station ID. so, indirect lookup
        return self.row_headers[row_id].station_id or 0


class TableByAgent(Table):
    def __init__(self, bin_size: int, num_bins: int) -> None:
        packets = data.PACKET_WATCHER.packets_by_agent(bin_size, num_bins)
        super().__init__(packets, bin_size)


class TableByFrequencySet(Table):
    def __init__(self, bin_size: int, num_bins: int, frequency_sets: dict[int, str]) -> None:
        # this is tough, since the receiver data is indirect, linked by frequency
        packets = data.PACKET_WATCHER.packets_by_frequency_set(bin_size, num_bins, frequency_sets)
        super().__init__(packets, bin_size)


class TableByReceiver(Table):
    def __init__(self, bin_size: int, num_bins: int) -> None:
        packets = data.PACKET_WATCHER.packets_by_receiver(bin_size, num_bins)
        super().__init__(packets, bin_size)

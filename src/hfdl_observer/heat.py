# hfdl_observer/heat.py
# copyright 2025 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import datetime
import logging
from typing import Callable, Iterable, Mapping, Optional, Sequence, Union

import hfdl_observer.data as data
import hfdl_observer.network as network
import hfdl_observer.util


logger = logging.getLogger(__name__)


class RowHeader:
    label: str = ''
    station_id: Optional[int] = None
    tags: set[str]

    def __init__(self, label: str, station_id: Optional[int] = None, tags: Optional[Sequence[str]] = None) -> None:
        self.label = label
        self.tags = set(tags or [])
        self.station_id = station_id

    def __str__(self) -> str:
        return f'[{"".join(t[0] for t in self.tags)}] #{self.station_id or "n/a"}:{self.label}'


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
        return f'{self.label}/{self.offset}'


class Cell:
    value: int
    tags: set[str]

    def __init__(self, value: int, tags: Optional[Sequence[str]] = None) -> None:
        self.value = value
        self.tags = set()
        if tags:
            self.tags.update(tags)

    def __str__(self) -> str:
        return f'{self.value}[{"".join(t[0] for t in self.tags)}]'


DataRows = dict[Union[int, str], Sequence[Cell]]


class Table:
    column_headers: Sequence[ColumnHeader]
    row_headers: dict[Union[int, str], RowHeader]
    data: DataRows

    def __init__(
        self, data: Mapping[Union[str, int], Sequence[int]], bin_size: int, start: Optional[datetime.datetime] = None
    ) -> None:
        self.data = {k: [Cell(v) for v in r] for k, r in data.items()}
        self.row_headers = {k: RowHeader(str(k)) for k in data.keys()}
        when = start if start is not None else hfdl_observer.util.now()
        if data:
            num_columns = max(len(r) for r in data.values())
            self.column_headers = [
                ColumnHeader(n, when - datetime.timedelta(seconds=n * bin_size), bin_size) for n in range(num_columns)
            ]
        else:
            self.column_headers = []

    def rows_matching(self, condition: Callable[[Union[int, str], Sequence[Cell]], bool]) -> DataRows:
        out = {}
        for k, cells in self.data.items():
            if callable(condition) and condition(k, cells):
                out[k] = cells
        return out

    def nonzero_rows(self) -> DataRows:
        return self.rows_matching(lambda key, cells: sum(cell.value for cell in cells) > 0)

    def tag_rows(
        self,
        keys: Iterable[Union[int, str]],
        tags: Optional[Sequence[str]],
        default_factory: Optional[Callable[[Union[int, str], Sequence[str]], RowHeader]] = None,
    ) -> None:
        for key in keys:
            if key in self.data:
                self.row_headers[key].tags.update(tags or [])
            elif default_factory:
                self.data[key] = [Cell(0) for col in self.column_headers]
                self.row_headers[key] = default_factory(key, tags or [])

    def sort(self) -> None:
        ordered = {k: self.data[k] for k in sorted(self.data.keys())}
        self.data = ordered

    def __str__(self) -> str:
        out = []
        out.append('\t\t' + '\t'.join(str(header) for header in self.column_headers))
        for k, cells in self.data.items():
            out.append(f'{self.row_headers[k]}\t{"\t".join(str(cell) for cell in cells)}')
        return '\n'.join(out)


class TableByFrequency(Table):

    def __init__(self, bin_size: int, num_bins: int) -> None:
        packets = data.PACKET_WATCHER.packets_by_frequency(bin_size, num_bins)
        super().__init__(packets, bin_size)

    def fill_active_state(self) -> None:
        for ix, column in enumerate(self.column_headers):
            when = column.when
            column_active: dict[int, network.StationAvailability] = {}
            for a in network.UPDATER.active_for_frame(when):
                for f in a.frequencies:
                    column_active[f] = a
            for freq, cells in self.data.items():
                cell = cells[ix]
                if cell.value is None:
                    continue
                row_header = self.row_headers[freq]
                station = column_active.get(int(freq), None)

                if station:
                    row_header.station_id = station.station_id
                    if station.frequencies and freq in station.frequencies:
                        cell.tags.add('active')
                        row_header.tags.add('active')
                    match (station.stratum):
                        case (network.Strata.SELF.value):
                            row_header.tags.add('local')
                        case (network.Strata.SQUITTER.value):
                            row_header.tags.add('network')
                        case (None):
                            pass
                        case (_):
                            row_header.tags.add('guess')
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


class TableByAgent(Table):
    def __init__(self, bin_size: int, num_bins: int) -> None:
        packets = data.PACKET_WATCHER.packets_by_agent(bin_size, num_bins)
        super().__init__(packets, bin_size)

# packet_stats.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

import asyncio
import bisect
import collections
import datetime
import itertools
import logging

from typing import Optional, Union

import hfdl_observer.hfdl


logger = logging.getLogger()


Sample = collections.namedtuple('Sample', ['when', 'freq', 'snr'])

STATION_ABBREVIATIONS = {
    1: 'SANFRAN',
    2: 'MOLOKAI',
    3: 'REYKJVK',
    4: 'RIVERHD',
    5: 'AUCKLAND',
    6: 'HATYAI',
    7: 'SHANNON',
    8: 'JOBURG',
    9: 'BARROW',
    10: 'MUAN',
    11: 'ALBROOK',
    13: 'SNTACRUZ',
    14: 'KRASNOY',
    15: 'MUHARRAQ',
    16: 'AGANA',
    17: 'CANARIAS',
}


class PacketCounter:
    samples: list[Sample]
    horizon: int = 86400
    refresh_period: int = 60
    task: Optional[asyncio.Task] = None
    observed_frequencies: list[int]
    observed_stations: dict[int, dict]

    def __init__(self) -> None:
        self.samples = []
        self.observed_frequencies = []
        self.observed_stations = collections.defaultdict(lambda: {'id': 0})

    def on_hfdl(self, packet: hfdl_observer.hfdl.HFDLPacketInfo) -> None:
        p = Sample(packet.timestamp, packet.frequency, packet.snr)
        bisect.insort(self.samples, p)
        if packet.ground_station:
            self.observed_stations[packet.frequency] = packet.ground_station

    def on_observing(self, observed_frequencies: list[int]) -> None:
        self.observed_frequencies = sorted(observed_frequencies)

    def on_frequencies(self, assigned_frequencies: dict[int, list[int]]) -> None:
        for sid, freqs in assigned_frequencies.items():
            for freq in freqs:
                self.observed_stations.setdefault(freq, {'id': sid, 'pending': True})

    def prune(self) -> None:
        # original = len(self.samples)
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        then = now - self.horizon
        first = bisect.bisect_left(self.samples, Sample(then, None, None))
        if first > 0:
            self.samples = self.samples[first:]
        # logger.debug(f'samples pruned from {original} to {len(self.samples)}')

    # this only does a count of hits per frequency per bin. Later may add SNR statistics.
    def bins(self, since: int, size: int) -> dict[int, dict[int, int]]:
        self.prune()
        now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        if since < 0:
            then = now + since
        else:
            then = since
        # ensure the furthest bucket is always a full one
        then -= then % size
        first = bisect.bisect_left(self.samples, then, key=lambda e: e[0])
        now_bin = now // size
        _rows: dict[int, dict[int, int]] = {}
        for sample in self.samples[first:]:
            sample_bin = now_bin - int(sample.when) // size
            _rows.setdefault(sample.freq, {}).setdefault(sample_bin, 0)
            _rows[sample.freq][sample_bin] += 1
        return _rows

    def sample_counts(self, counts: dict[int, dict[int, int]]) -> tuple[list[int], dict[int, list[int]]]:
        freqs = sorted(counts.keys())
        # sorted_counts = sorted(counts, key=lambda e:e[0])
        ages = sorted(set(itertools.chain(*(c.keys() for c in counts.values()))))
        cols = list(range(0, max(ages) + 1))

        rows = {}
        for freq in freqs:
            bins = counts[freq]
            row = [0] * len(cols)
            for binno, count in bins.items():
                binix = ages.index(binno)
                row[binix] = count
            rows[freq] = row
        return (cols, rows)

    def active_symbol(self, freq: int, counts: int) -> str:
        if freq:
            return '◉' if counts else '○'
        return '◌'

    def count_symbol(self, amount: int) -> str:
        # with 13 slots per 32 second frame, we should not expect more than 25 packets per minute, but cover some
        # other bin sizes as well, with some reasonable symbols.
        if amount == 0:
            return '·'
        if amount < 10:
            return str(amount)
        if amount < 36:
            return chr(87 + amount)
        return '◈'

    def decorate_counts(
        self, rows: dict[int, list[int]]
    ) -> dict[int, dict[str, Union[list[int], list[str], str, int]]]:
        decorated: dict[int, dict[str, Union[list[int], list[str], str, int]]] = {}
        for freq, bins in rows.items():
            tot = sum(bins)
            symbols = list(self.count_symbol(b) for b in bins)
            decorated[freq] = {
                'active': self.active_symbol(freq, tot),
                'counts': bins,
                'symbols': symbols,
                'total': sum(bins),
            }
        return decorated

    def decorated_counts_table(
        self, headers: list[int], rows: dict[int, list[int]]
    ) -> dict[int, dict[str, Union[list[int], list[str], str, int]]]:
        s = set(self.observed_frequencies)
        s.update(rows.keys())
        zeroes = [0] * len(headers)
        all_rows = {}
        for freq in sorted(s):
            all_rows[freq] = rows.get(freq, zeroes)
        decorated_rows = self.decorate_counts(all_rows)
        return decorated_rows

    def log_counts(self) -> None:
        if self.samples:
            binned_samples = self.bins(-1800, 60)
            headers, rows = self.sample_counts(binned_samples)
            decorated_table = self.decorated_counts_table(headers, rows)

            fxaxis = ['        '] + [f'{h: >3}' for h in headers]
            logger.info("".join(fxaxis))

            for freq, data in decorated_table.items():
                tot = data["total"]
                bins: list[int] = data["symbols"]  # type: ignore
                row = f'{data["active"]: <2}{freq: >6}{"".join(f"{c: >3}" for c in bins)}{f"{tot: >4}" if tot else ""}'
                logger.info(row)

    def render(self) -> None:
        # DEFAULT. Override this to send the stats someplace else.
        self.log_counts()

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
            loop = loop or asyncio.get_running_loop()
            self.task = loop.create_task(self.run())

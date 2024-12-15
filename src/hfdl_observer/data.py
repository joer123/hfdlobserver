# hfdl_observer/data.py
# copyright 2024 Kuupa Ork <kuupaork+github@hfdl.observer>
# see LICENSE (or https://github.com/hfdl-observer/hfdlobserver888/blob/main/LICENSE) for terms of use.
# TL;DR: BSD 3-clause
#

from typing import Optional

import hfdl_observer.hfdl


class ListenerConfig:
    proto: str = 'udp'
    address: str = '127.0.0.1'
    port: int = 5542


class Allocation:
    _frequencies: set[int]
    allowed_width: int

    def __init__(self, allowed_width: int, frequencies: Optional[list[int]] = None):
        self.allowed_width = allowed_width
        self._frequencies = set(frequencies) if frequencies else set()

    @property
    def min(self) -> float:
        return min(self._frequencies)

    @property
    def max(self) -> float:
        return max(self._frequencies)

    @property
    def center(self) -> float:
        return (self.min + self.max) / 2.0

    @property
    def width(self) -> float:
        if self.frequencies:
            channel_width: float = hfdl_observer.hfdl.HFDL_CHANNEL_WIDTH
            return self.max - self.min + channel_width
        else:
            return 0.0

    @property
    def frequencies(self) -> list[int]:
        return sorted(self._frequencies)

    def maybe_add(self, frequency: int) -> bool:
        peephole_width = self.allowed_width - hfdl_observer.hfdl.HFDL_CHANNEL_WIDTH
        if not self._frequencies or (
            abs(frequency - self.min) <= peephole_width and abs(frequency - self.max) <= peephole_width
        ):
            self._frequencies.add(frequency)
            return True
        return False

    def __str__(self) -> str:
        return f'{self.frequencies}'

    def __repr__(self) -> str:
        return f'Allocation({self.allowed_width}, {self.frequencies})'


class Parameters:
    max_sample_rate: int
    num_clients: int

    def allocation(self, initial_frequencies: list[int]) -> Allocation:
        return Allocation(self.max_sample_rate, initial_frequencies)

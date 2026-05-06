from __future__ import annotations

from dataclasses import dataclass
import random
from typing import Callable, Iterable


@dataclass(frozen=True)
class LoadPoint:
    timestamp: int
    target_concurrency: int
    phase: str


class LoadCurveModel:
    def __init__(self, seed: int, minimum_concurrency: int = 1) -> None:
        self._rng = random.Random(seed)
        self._points: list[LoadPoint] = []
        self._cursor = 0
        self.seed = seed
        self.minimum_concurrency = max(0, int(minimum_concurrency))

    def _append(self, duration: int, phase: str, producer: Callable[[int, int], int]) -> "LoadCurveModel":
        steps = max(1, int(duration))
        for i in range(steps):
            target = max(self.minimum_concurrency, int(producer(i, steps)))
            self._points.append(
                LoadPoint(
                    timestamp=self._cursor + i,
                    target_concurrency=target,
                    phase=phase,
                )
            )
        self._cursor += steps
        return self

    def ramp_up(self, duration: int, start: int, end: int) -> "LoadCurveModel":
        s = int(start)
        e = int(end)

        def producer(i: int, steps: int) -> int:
            if steps == 1:
                return e
            ratio = i / float(steps - 1)
            return round(s + (e - s) * ratio)

        return self._append(duration, "ramp_up", producer)

    def plateau(self, duration: int, level: int) -> "LoadCurveModel":
        lvl = int(level)
        return self._append(duration, "plateau", lambda _i, _steps: lvl)

    def burst(self, duration: int, spikes: int, amplitude: int) -> "LoadCurveModel":
        steps = max(1, int(duration))
        if not self._points:
            base = max(self.minimum_concurrency, int(amplitude))
        else:
            base = self._points[-1].target_concurrency
        spike_count = max(1, min(steps, int(spikes)))
        spike_positions = set(self._rng.sample(range(steps), spike_count))
        amp = max(1, int(amplitude))

        def producer(i: int, _steps: int) -> int:
            if i in spike_positions:
                jitter = self._rng.randint(0, max(1, amp // 5))
                return base + amp + jitter
            return base

        return self._append(duration, "burst", producer)

    def decay(self, duration: int, end_level: int) -> "LoadCurveModel":
        end = int(end_level)
        start = self._points[-1].target_concurrency if self._points else end

        def producer(i: int, steps: int) -> int:
            if steps == 1:
                return end
            ratio = i / float(steps - 1)
            return round(start + (end - start) * ratio)

        return self._append(duration, "decay", producer)

    def with_poisson_spikes(self, duration: int, base: int, lam: float, amplitude: int) -> "LoadCurveModel":
        base_level = max(self.minimum_concurrency, int(base))
        lambda_value = max(0.0, float(lam))
        spike_amplitude = max(1, int(amplitude))

        def producer(_i: int, _steps: int) -> int:
            spikes = self._poisson(lambda_value)
            return base_level + (spikes * spike_amplitude)

        return self._append(duration, "poisson_spike", producer)

    def _poisson(self, lam: float) -> int:
        if lam <= 0.0:
            return 0
        threshold = pow(2.718281828459045, -lam)
        count = 0
        product = 1.0
        while product > threshold:
            count += 1
            product *= self._rng.random()
        return max(0, count - 1)

    def to_schedule(self) -> Iterable[dict[str, int]]:
        return (
            {
                "timestamp": p.timestamp,
                "target_concurrency": p.target_concurrency,
                "phase": p.phase,
            }
            for p in self._points
        )

    @property
    def duration_seconds(self) -> int:
        return len(self._points)

    def describe(self) -> dict[str, int | str]:
        return {
            "type": "stable_curve",
            "seed": self.seed,
            "duration_seconds": self.duration_seconds,
            "schedule_points": len(self._points),
        }

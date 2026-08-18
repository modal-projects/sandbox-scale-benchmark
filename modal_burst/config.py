from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    total: int = 1000
    shard_size: int = 5000
    group_size: int = 50  # batch size for the create ramp
    ramp_s: float = 0.0  # spread a shard's creates over this many seconds; 0 = all at once
    clients: int = 0  # Go client/connection pool per shard; 0 = auto (~size/100)
    wait_for_shard_creates: bool = False
    sandbox_timeout_s: int = 3600  # inner-sandbox lifetime; the backstop for a running workload
    shard_cpu: float = 4.0
    shard_memory_mb: int = 4096
    shard_timeout_s: int = 3600

    @property
    def num_shards(self) -> int:
        return max(1, math.ceil(self.total / self.shard_size))

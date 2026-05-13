"""Shared Redis client, consumed by all shared modules and fm files."""

from __future__ import annotations

import redis

REDIS_URL = "redis://localhost:6379/0"

client: redis.Redis = redis.Redis.from_url(REDIS_URL)

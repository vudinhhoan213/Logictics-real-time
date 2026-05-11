import json
import logging
import os
from typing import Dict, Any, Optional, List
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RedisWriter")


class RedisWriter:
    """
    Hệ thống ghi/đọc dữ liệu giao thông thời gian thực vào Redis.
    Dùng chung cho Spark Streaming, backend adapter và route optimization.
    """

    KEY_PREFIX = "edge"
    BLOCKED_KEY = "blocked_edges"

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: int = 0,
        password: Optional[str] = None,
    ):
        self._host = host or os.getenv("REDIS_HOST", "redis")
        self._port = port or int(os.getenv("REDIS_PORT", 6379))
        self._db = db
        self._password = password
        self._client: Optional[redis.Redis] = None

        self._connect()

    def _connect(self):
        try:
            pool = redis.ConnectionPool(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password,
                decode_responses=True,
                socket_timeout=5.0,
                socket_keepalive=True,
                retry_on_timeout=True,
            )
            self._client = redis.Redis(connection_pool=pool)
            self._client.ping()
            logger.info(f"✅ Redis Connected: {self._host}:{self._port}")
        except Exception as e:
            logger.error(f"❌ Redis Connection Failed: {e}")
            self._client = None

    def set_edge_state(self, edge_id: str, payload: Dict[str, Any], ttl: int = 120) -> bool:
        if not self._client:
            return False

        key = f"{self.KEY_PREFIX}:{edge_id}"

        try:
            pipe = self._client.pipeline()
            pipe.set(key, json.dumps(payload), ex=ttl)

            if payload.get("is_congested"):
                pipe.sadd(self.BLOCKED_KEY, edge_id)
            else:
                pipe.srem(self.BLOCKED_KEY, edge_id)

            pipe.execute()
            return True

        except Exception as e:
            logger.error(f"Lỗi ghi Key {key}: {e}")
            return False

    def pipeline_set_many(self, records: List[Dict[str, Any]], ttl: int = 120) -> int:
        if not self._client or not records:
            return 0

        batch_size = 500
        total_written = 0

        try:
            for i in range(0, len(records), batch_size):
                chunk = records[i:i + batch_size]
                pipe = self._client.pipeline(transaction=False)

                blocked_add = []
                blocked_remove = []

                for rec in chunk:
                    edge_id = rec.get("edge_id")
                    if not edge_id:
                        continue

                    key = f"{self.KEY_PREFIX}:{edge_id}"
                    value = json.dumps(rec, separators=(",", ":"))

                    pipe.set(key, value, ex=ttl)

                    if rec.get("is_congested"):
                        blocked_add.append(edge_id)
                    else:
                        blocked_remove.append(edge_id)

                    total_written += 1

                if blocked_add:
                    pipe.sadd(self.BLOCKED_KEY, *blocked_add)

                if blocked_remove:
                    pipe.srem(self.BLOCKED_KEY, *blocked_remove)

                pipe.execute()

            return total_written

        except Exception as e:
            logger.error(f"Lỗi Pipeline: {e}", exc_info=True)
            return total_written

    def get_edge_state(self, edge_id: str) -> Optional[Dict[str, Any]]:
        if not self._client:
            return None

        key = f"{self.KEY_PREFIX}:{edge_id}"

        try:
            raw = self._client.get(key)
            if not raw:
                return None

            return json.loads(raw)

        except Exception as e:
            logger.error(f"Lỗi đọc Key {key}: {e}")
            return None

    def get_blocked_edges(self) -> List[str]:
        if not self._client:
            return []

        try:
            return list(self._client.smembers(self.BLOCKED_KEY))

        except Exception as e:
            logger.error(f"Lỗi đọc {self.BLOCKED_KEY}: {e}")
            return []

    def get_all_edge_states(self) -> Dict[str, Any]:
        if not self._client:
            return {}

        result = {}

        try:
            cursor = 0

            while True:
                cursor, keys = self._client.scan(
                    cursor=cursor,
                    match=f"{self.KEY_PREFIX}:*",
                    count=500,
                )

                if keys:
                    values = self._client.mget(keys)

                    for key, value in zip(keys, values):
                        if value:
                            edge_id = key.split(":", 1)[1]
                            result[edge_id] = json.loads(value)

                if cursor == 0:
                    break

        except Exception as e:
            logger.error(f"Lỗi Scan: {e}")

        return result

    def close(self):
        if self._client:
            self._client.close()
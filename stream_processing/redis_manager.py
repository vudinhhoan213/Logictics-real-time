"""
Redis Writer

"""

import json
import logging
from typing import Dict, Any, Optional, List
import os
import redis

logger = logging.getLogger("RedisWriter")


class RedisWriter:
    """
    Wrapper đơn giản  để ghi / đọc trạng thái edge vào Redis.

    Thiết kế:
    - Dùng SET + EXPIRE thay  vì HSET để dễ serialize JSON phức tạp
    - Hỗ trợ pipeline ghi nhiều edge một lúc (pipeline_set_many)
    - Tự retry kết nối nếu Redis chưa sẵn sàng
    """

    KEY_PREFIX   = "edge"
    BLOCKED_KEY  = "blocked_edges"     # Set chứa các edge đang bị tắc

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db:   int = 0,
        password: Optional[str] = None,
        socket_timeout: float = 3.0,
    ):
        self._host = host
        self._port = port
        self._client: Optional[redis.Redis] = None
        self._connect(host, port, db, password, socket_timeout)

    def _connect(self, host, port, db, password, timeout):
        try:
            self._client = redis.Redis(
                host=host, port=port, db=db,
                password=password,
                socket_timeout=timeout,
                decode_responses=True,
            )
            self._client.ping()
            logger.info(f"Kết nối Redis thành công: {host}:{port}")
        except redis.ConnectionError as e:
            logger.error(f"Không kết nối được Redis ({host}:{port}): {e}")
            self._client = None

    # ── Ghi một edge ─────────────────────────────────────────────────────────

    def set_edge_state(
        self,
        edge_id: str,
        payload: Dict[str, Any],
        ttl_seconds: int = 120,
    ) -> bool:
        """
        Ghi trạng thái của một edge vào Redis.

        Args:
            edge_id:     ID của edge, vd: "E_123_456"
            payload:     Dict chứa avg_speed, vehicle_count, …
            ttl_seconds: Thời gian sống của key (giây)

        Returns:
            True nếu thành công, False nếu lỗi.
        """
        if self._client is None:
            logger.warning("Redis chưa kết nối, bỏ qua ghi.")
            return False

        key   = f"{self.KEY_PREFIX}:{edge_id}"
        value = json.dumps(payload, ensure_ascii=False)

        try:
            pipe = self._client.pipeline()
            pipe.set(key, value, ex=ttl_seconds)

            # Cập nhật set blocked_edges
            if payload.get("is_congested"):
                pipe.sadd(self.BLOCKED_KEY, edge_id)
                pipe.expire(self.BLOCKED_KEY, ttl_seconds)
            else:
                pipe.srem(self.BLOCKED_KEY, edge_id)

            pipe.execute()
            return True

        except redis.RedisError as e:
            logger.error(f"Lỗi ghi Redis [{key}]: {e}")
            return False

    # ── Ghi nhiều edge một lúc (pipeline) ────────────────────────────────────

    def pipeline_set_many(
        self,
        records: List[Dict[str, Any]],
        ttl_seconds: int = 120,
    ) -> int:
        """
        Ghi batch nhiều edge trong một pipeline Redis.

        Args:
            records:     Danh sách dict, mỗi dict phải có trường 'edge_id'
            ttl_seconds: TTL chung

        Returns:
            Số lượng edge đã ghi thành công.
        """
        if self._client is None or not records:
            return 0

        written = 0
        try:
            pipe = self._client.pipeline()
            blocked_to_add   = []
            blocked_to_remove = []

            for rec in records:
                edge_id = rec.get("edge_id")
                if not edge_id:
                    continue
                key   = f"{self.KEY_PREFIX}:{edge_id}"
                value = json.dumps(rec, ensure_ascii=False)
                pipe.set(key, value, ex=ttl_seconds)
                written += 1

                if rec.get("is_congested"):
                    blocked_to_add.append(edge_id)
                else:
                    blocked_to_remove.append(edge_id)

            if blocked_to_add:
                pipe.sadd(self.BLOCKED_KEY, *blocked_to_add)
                pipe.expire(self.BLOCKED_KEY, ttl_seconds)
            if blocked_to_remove:
                pipe.srem(self.BLOCKED_KEY, *blocked_to_remove)

            pipe.execute()
        except redis.RedisError as e:
            logger.error(f"Lỗi pipeline Redis: {e}")

        return written

    # ── Đọc (dùng để debug hoặc route optimization đọc) ──────────────────────

    def get_edge_state(self, edge_id: str) -> Optional[Dict[str, Any]]:
        """Đọc trạng thái hiện tại của một edge."""
        if self._client is None:
            return None
        try:
            raw = self._client.get(f"{self.KEY_PREFIX}:{edge_id}")
            return json.loads(raw) if raw else None
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"Lỗi đọc Redis [{edge_id}]: {e}")
            return None

    def get_blocked_edges(self) -> List[str]:
        """Lấy danh sách tất cả edge đang bị tắc."""
        if self._client is None:
            return []
        try:
            return list(self._client.smembers(self.BLOCKED_KEY))
        except redis.RedisError as e:
            logger.error(f"Lỗi đọc blocked_edges: {e}")
            return []

    def get_all_edge_states(self) -> Dict[str, Dict[str, Any]]:
        """
        Lấy toàn bộ trạng thái edges (dùng cho debug / dashboard).
        Cẩn thận với graph lớn — dùng SCAN thay vì KEYS.
        """
        if self._client is None:
            return {}
        result = {}
        try:
            cursor = 0
            pattern = f"{self.KEY_PREFIX}:*"
            while True:
                cursor, keys = self._client.scan(
                    cursor=cursor, match=pattern, count=100
                )
                for k in keys:
                    raw = self._client.get(k)
                    if raw:
                        edge_id = k.replace(f"{self.KEY_PREFIX}:", "", 1)
                        result[edge_id] = json.loads(raw)
                if cursor == 0:
                    break
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"Lỗi get_all_edge_states: {e}")
        return result

    # ── Cleanup ───────────────────────────────────────────────────────────────

    def close(self):
        if self._client:
            self._client.close()
            logger.debug("Đã đóng kết nối Redis.")
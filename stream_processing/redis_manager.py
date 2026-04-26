import json
import logging
import os
from typing import Dict, Any, Optional, List
import redis

# Cấu hình logging chuyên nghiệp
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RedisWriter")

class RedisWriter:
    """
    Hệ thống ghi dữ liệu giao thông thời gian thực vào Redis.
    Tối ưu cho PySpark Structured Streaming.
    """
    
    # Prefix đồng bộ với Backend Node.js
    KEY_PREFIX = "edge"
    BLOCKED_KEY = "blocked_edges"

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: int = 0,
        password: Optional[str] = None
    ):
        # Ưu tiên lấy từ tham số truyền vào, nếu không thì lấy từ môi trường Docker
        self._host = host or os.getenv("REDIS_HOST", "redis")
        self._port = port or int(os.getenv("REDIS_PORT", 6379))
        self._db = db
        self._password = password
        self._client: Optional[redis.Redis] = None
        
        # Kết nối ngay khi khởi tạo
        self._connect()

    def _connect(self):
        """Khởi tạo kết nối với cơ chế Pool để tiết kiệm tài nguyên."""
        try:
            pool = redis.ConnectionPool(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password,
                decode_responses=True,
                socket_timeout=5.0,
                socket_keepalive=True,
                retry_on_timeout=True
            )
            self._client = redis.Redis(connection_pool=pool)
            self._client.ping()
            logger.info(f"✅ Redis Connected: {self._host}:{self._port}")
        except Exception as e:
            logger.error(f"❌ Redis Connection Failed: {e}")
            self._client = None

    def set_edge_state(self, edge_id: str, payload: Dict[str, Any], ttl: int = 120) -> bool:
        """Ghi trạng thái của 1 cung đường đơn lẻ."""
        if not self._client:
            return False
        
        key = f"{self.KEY_PREFIX}:{edge_id}"
        try:
            # Dùng pipeline để đảm bảo tính nguyên tử (Atomic)
            pipe = self._client.pipeline()
            pipe.set(key, json.dumps(payload), ex=ttl)
            
            # Cập nhật danh sách đường tắc
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

        BATCH_SIZE = 500
        total_written = 0

        try:
            for i in range(0, len(records), BATCH_SIZE):
                chunk = records[i:i + BATCH_SIZE]
                pipe = self._client.pipeline(transaction=False)

                blocked_add = []
                blocked_remove = []

                for rec in chunk:
                    eid = rec.get("edge_id")
                    if not eid: 
                        continue

                    key = f"{self.KEY_PREFIX}:{eid}"
                    value = json.dumps(rec, separators=(",", ":"))

                    pipe.set(key, value, ex=ttl)

                    if rec.get("is_congested"): 
                        blocked_add.append(eid)
                    else:
                        blocked_remove.append(eid)

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

    def get_all_edge_states(self) -> Dict[str, Any]:

        if not self._client: return {}
        
        result = {}
        try:
            cursor = 0
            while True:
                # Mỗi lần quét 500 keys để không làm nghẽn CPU
                cursor, keys = self._client.scan(cursor=cursor, match=f"{self.KEY_PREFIX}:*", count=500)
                if keys:
                    values = self._client.mget(keys)
                    for k, v in zip(keys, values):
                        if v:
                            eid = k.split(":")[1]
                            result[eid] = json.loads(v)
                if cursor == 0:
                    break
        except Exception as e:
            logger.error(f"Lỗi Scan: {e}")
        return result

    def close(self):
        """Đóng kết nối an toàn."""
        if self._client:
            self._client.close()
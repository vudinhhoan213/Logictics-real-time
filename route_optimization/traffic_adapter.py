import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from stream_processing.redis_manager import RedisWriter


class TrafficAdapter:
    def __init__(self, host=None, port=6379):
        self.client = RedisWriter(
            host=host or os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", port)),
        )

    def get_edge_state(self, edge_id):
        return self.client.get_edge_state(edge_id)

    def get_blocked_edges(self):
        return self.client.get_blocked_edges()

    def close(self):
        self.client.close()
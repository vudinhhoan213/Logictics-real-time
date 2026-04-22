import os
import sys


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
STREAM_DIR = os.path.join(PROJECT_ROOT, "stream_processing")

if STREAM_DIR not in sys.path:
    sys.path.append(STREAM_DIR)

from redis_manager import RedisWriter


class TrafficAdapter:
    def __init__(self, host="localhost", port=6379):
        self.client = RedisWriter(host=host, port=port)

    def get_edge_state(self, edge_id):
        return self.client.get_edge_state(edge_id)

    def get_blocked_edges(self):
        return self.client.get_blocked_edges()

    def close(self):
        self.client.close()
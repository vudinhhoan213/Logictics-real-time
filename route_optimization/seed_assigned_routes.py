#!/usr/bin/env python3
"""
Ghi bản ghi tối thiểu vào MongoDB `traffic_system.assigned_routes` để worker GA
có dữ liệu đầu vào. Sau khi seed, service route_optimization (vòng lặp mỗi 5s)
sẽ chạy GA lần đầu (force bootstrap) cho xe chưa có `new_assigned_route` và
ghi lại lộ trình — dashboard nhận qua Change Stream.

Chạy trong Docker (từ máy host). Dùng **tên service** `route_optimization`, không dùng container_name:

  cd infrastructure
  docker compose exec route_optimization python route_optimization/seed_assigned_routes.py --count 10

Tuỳ chọn:
  --count N   Số xe Truck_001..Truck_N (mặc định 10; GA xử lý tuần tự nên đừng đặt quá cao lúc thử).
  --reset     Xoá new_assigned_route / ETA để ép tối ưu lại (vẫn giữ assigned_route + khách).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

from pymongo import MongoClient


def load_edges(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        raise SystemExit(f"Không đọc được danh sách cạnh từ {path}")
    return data


def build_doc_for_truck(edges: List[Dict[str, Any]], index: int) -> Dict[str, Any]:
    """index: 1-based truck index (Truck_001, ...)."""
    n = len(edges)
    span = min(6, n)
    off = ((index - 1) * 13) % max(1, n - span)
    chunk = edges[off : off + span]
    eids = [e["edge_id"] for e in chunk]
    if len(eids) < 2:
        raise SystemExit("edges_schema quá ngắn")

    custs = []
    for j, e in enumerate(chunk[1:4]):
        sn = e.get("start_node") or {}
        lat, lon = sn.get("lat"), sn.get("lon")
        if lat is None or lon is None:
            continue
        custs.append(
            {
                "cust_id": f"Cust_T{index:03d}_{j+1}",
                "latitude": float(lat),
                "longitude": float(lon),
            }
        )
    if not custs:
        custs.append(
            {
                "cust_id": f"Cust_T{index:03d}_1",
                "latitude": float(chunk[0]["start_node"]["lat"]),
                "longitude": float(chunk[0]["start_node"]["lon"]),
            }
        )

    vid = f"Truck_{index:03d}"
    return {
        "vehicle_id": vid,
        "entity_id": vid,
        "entity_type": "Truck",
        "edge_id": eids[0],
        "current_edge_id": eids[0],
        "distance_on_edge": 0.0,
        "assigned_route": eids[: max(2, len(eids))],
        "remaining_customers": custs,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed assigned_routes trong MongoDB cho demo dashboard.")
    parser.add_argument("--count", type=int, default=10, help="Số xe (1..100)")
    parser.add_argument(
        "--edges",
        default=os.getenv("EDGES_JSON", "/app/data/edges_schema.json"),
        help="Đường dẫn edges_schema.json",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://mongodb:27017/"),
        help="Mongo URI (db traffic_system được dùng cố định giống route_optimizer)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Bỏ new_assigned_route / estimated_total_travel_time để GA chạy lại",
    )
    args = parser.parse_args()

    if args.count < 1 or args.count > 100:
        print("--count phải trong 1..100", file=sys.stderr)
        sys.exit(1)

    edges = load_edges(args.edges)
    client = MongoClient(args.mongo_uri)
    coll = client["traffic_system"]["assigned_routes"]

    for i in range(1, args.count + 1):
        doc = build_doc_for_truck(edges, i)
        vid = doc["vehicle_id"]
        update: Dict[str, Any] = {"$set": doc}
        if args.reset:
            update["$unset"] = {"new_assigned_route": "", "estimated_total_travel_time": ""}
        coll.update_one({"vehicle_id": vid}, update, upsert=True)
        print(f"Upsert OK: {vid}")

    print(f"Xong. Đã seed {args.count} xe. Đợi vài chu kỳ GA (xem log service_ga_optimizer), rồi F5 dashboard.")


if __name__ == "__main__":
    main()

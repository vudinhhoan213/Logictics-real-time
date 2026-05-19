/**
 * pathfinder.js - Dijkstra shortest-path trên graph edges_schema.json
 * Tính lộ trình liên tục từ vị trí xe → qua các điểm giao hàng
 */

const fs = require("fs");
const path = require("path");

class PathFinder {
  constructor() {
    this.nodes = {};     // node_id → {lat, lon}
    this.edges = {};     // edge_id → {start_node_id, end_node_id, length_meters, max_speed_kmh}
    this.adjacency = {}; // node_id → [{to_node, edge_id, cost}]
    this.loaded = false;
  }

  load(edgesPath) {
    const raw = JSON.parse(fs.readFileSync(edgesPath, "utf-8"));
    console.log(`📍 PathFinder: Loading ${raw.length} edges...`);

    for (const item of raw) {
      const edgeId = item.edge_id;
      const startId = item.start_node.node_id;
      const endId = item.end_node.node_id;

      this.nodes[startId] = { lat: item.start_node.lat, lon: item.start_node.lon };
      this.nodes[endId] = { lat: item.end_node.lat, lon: item.end_node.lon };

      this.edges[edgeId] = {
        start_node_id: startId,
        end_node_id: endId,
        length_meters: item.length_meters || 1,
        max_speed_kmh: item.max_speed_kmh || 40,
      };

      if (!this.adjacency[startId]) this.adjacency[startId] = [];
      this.adjacency[startId].push({
        to_node: endId,
        edge_id: edgeId,
        cost: (item.length_meters || 1) / ((item.max_speed_kmh || 40) * 1000 / 3600),
      });
      if (!this.adjacency[endId]) this.adjacency[endId] = [];
    }

    this.loaded = true;
    console.log(`✅ PathFinder: ${Object.keys(this.nodes).length} nodes, ${Object.keys(this.edges).length} edges`);
  }

  /**
   * Tìm node gần nhất với tọa độ (lat, lon)
   */
  nearestNode(lat, lon) {
    let bestNode = null;
    let bestDist = Infinity;
    for (const [nodeId, node] of Object.entries(this.nodes)) {
      const d = (node.lat - lat) ** 2 + (node.lon - lon) ** 2;
      if (d < bestDist) {
        bestDist = d;
        bestNode = nodeId;
      }
    }
    return bestNode;
  }

  /**
   * Min-heap đơn giản cho Dijkstra (nhanh hơn array.sort)
   */
  static _heapPush(heap, item) {
    heap.push(item);
    let i = heap.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (heap[parent].cost <= heap[i].cost) break;
      [heap[parent], heap[i]] = [heap[i], heap[parent]];
      i = parent;
    }
  }

  static _heapPop(heap) {
    const top = heap[0];
    const last = heap.pop();
    if (heap.length > 0) {
      heap[0] = last;
      let i = 0;
      while (true) {
        let smallest = i;
        const left = 2 * i + 1;
        const right = 2 * i + 2;
        if (left < heap.length && heap[left].cost < heap[smallest].cost) smallest = left;
        if (right < heap.length && heap[right].cost < heap[smallest].cost) smallest = right;
        if (smallest === i) break;
        [heap[smallest], heap[i]] = [heap[i], heap[smallest]];
        i = smallest;
      }
    }
    return top;
  }

  /**
   * Dijkstra: shortest path từ startNode → endNode
   * Trả về [edge_ids], cost
   */
  dijkstra(startNode, endNode) {
    if (!this.nodes[startNode] || !this.nodes[endNode]) return { path: [], cost: Infinity };
    if (startNode === endNode) return { path: [], cost: 0 };

    const dist = {};
    const prev = {};
    const visited = new Set();
    const queue = [];

    dist[startNode] = 0;
    PathFinder._heapPush(queue, { node: startNode, cost: 0 });

    while (queue.length > 0) {
      const { node: current, cost: currentCost } = PathFinder._heapPop(queue);

      if (visited.has(current)) continue;
      visited.add(current);

      if (current === endNode) break;

      for (const neighbor of (this.adjacency[current] || [])) {
        if (visited.has(neighbor.to_node)) continue;
        const newCost = currentCost + neighbor.cost;
        if (newCost < (dist[neighbor.to_node] ?? Infinity)) {
          dist[neighbor.to_node] = newCost;
          prev[neighbor.to_node] = { from: current, edge_id: neighbor.edge_id };
          PathFinder._heapPush(queue, { node: neighbor.to_node, cost: newCost });
        }
      }
    }

    if (dist[endNode] === undefined) return { path: [], cost: Infinity };

    // Trace back
    const pathEdges = [];
    let cur = endNode;
    while (cur !== startNode && prev[cur]) {
      pathEdges.push(prev[cur].edge_id);
      cur = prev[cur].from;
    }
    pathEdges.reverse();
    return { path: pathEdges, cost: dist[endNode] };
  }

  /**
   * Tính lộ trình liên tục từ vị trí xe → qua danh sách điểm giao hàng
   * @param {number} truckLat - vĩ độ xe hiện tại
   * @param {number} truckLon - kinh độ xe hiện tại
   * @param {Array} customers - [{cust_id, latitude, longitude}]
   * @returns {path: [edge_ids], totalCost: seconds}
   */
  buildRoute(truckLat, truckLon, customers) {
    if (!this.loaded || !customers || customers.length === 0) {
      return { path: [], totalCost: 0 };
    }

    // Tìm node gần nhất với xe
    let currentNode = this.nearestNode(truckLat, truckLon);
    if (!currentNode) return { path: [], totalCost: 0 };

    const fullPath = [];
    let totalCost = 0;

    // Greedy nearest-customer-first (đơn giản, hiệu quả)
    const unvisited = [...customers];

    while (unvisited.length > 0) {
      // Tìm customer gần nhất (theo Dijkstra cost)
      let bestIdx = 0;
      let bestCost = Infinity;
      let bestPath = [];
      let bestTargetNode = null;

      for (let i = 0; i < unvisited.length; i++) {
        const cust = unvisited[i];
        const targetNode = this.nearestNode(cust.latitude, cust.longitude);
        if (!targetNode) continue;

        const result = this.dijkstra(currentNode, targetNode);
        if (result.cost < bestCost) {
          bestCost = result.cost;
          bestPath = result.path;
          bestIdx = i;
          bestTargetNode = targetNode;
        }
      }

      if (bestCost === Infinity || !bestTargetNode) break;

      fullPath.push(...bestPath);
      totalCost += bestCost;
      currentNode = bestTargetNode;
      unvisited.splice(bestIdx, 1);
    }

    return { path: fullPath, totalCost };
  }
}

module.exports = PathFinder;

# Key-Value Store Load Generator (C, Multithreaded TCP Client)

A **multithreaded load generator** written in C for benchmarking the existing key-value storage server.  
It simulates concurrent clients performing random **CRUD (Create, Read, Update, Delete)** operations using the same **binary protocol** as the interactive/batch client, measuring performance metrics such as throughput and latency.

---

## Overview

This program stresses the key-value store server by generating synthetic load with configurable concurrency, key-space size, and test duration.  
Each client thread continuously connects to the server, issues randomized operations, and records success rates and latency statistics.

---

## Features

- Generates **multi-threaded load** against the key-value server  
- Supports **configurable test duration, concurrency, and key-space**  
- Uses the **same binary request/response protocol** as the standard client  
- Measures:
  - **Throughput (requests/sec)**
  - **Average response time (ms)**
  - **Success/failure counts**
  - **Keys created (approximation)**
- Implements robust retry/backoff logic for transient connection failures  
- Low-level socket I/O with `read_n` / `write_n` ensures reliable TCP framing  
- Independent from server logic — purely a performance testing client

---

## Architecture and Workload Model

Each worker thread:
1. Maintains a persistent TCP connection to the server (with reconnection logic).  
2. Randomly selects an operation type:
   - **Create** (`C`) – inserts a new key-value pair until key-space is filled.  
   - **Read** (`R`) – fetches existing keys.  
   - **Update** (`U`) – modifies an existing key’s value.  
   - **Delete** (`D`) – deletes an existing key.  
3. Generates random ASCII payloads (value lengths: 8–64 bytes).  
4. Records per-request latency and operation outcome.

Operation distribution dynamically adapts:
- During **early load**, higher create ratio (to populate keys).  
- Once key-space is filled, higher read/update/delete ratio (steady-state workload).

---

## Binary Protocol

### Request (Client → Server)

A fixed **9-byte header** followed by an optional value:

| Bytes | Field | Description |
|-------|--------|-------------|
| 0 | op | 'C', 'R', 'U', 'D' |
| 1–4 | key | int32_t (network byte order) |
| 5–8 | value_size | int32_t (network byte order; 0 if none) |
| 9.. | value | raw bytes (if applicable) |

### Response (Server → Client)

A fixed **5-byte header** followed by optional payload:

| Bytes | Field | Description |
|-------|--------|-------------|
| 0 | status | 'O' (OK), 'S' (Server error), 'C' (Client error) |
| 1–4 | payload_size | int32_t (network byte order) |
| 5.. | payload | value bytes or ASCII message |

---

## Build Instructions

Compile both server and load generator:

```bash
gcc -std=c11 -O2 -Wall -pthread -o kv-server kv-server.c
gcc -std=c11 -O2 -Wall -pthread -o kv-multi-client kv-multi-client.c
```

## Running the Server

```bash
./kv-server <bind-ip> <port>
```

### Example: 
```bash 
./kv-server 127.0.0.1 9000
```

--- 

## Running the Load Generator
```bash
./kv-multi-client <server-ip> <server-port> <num-threads> <duration-sec> [key-space]
```

### Arguments

| Argument         | Description                         | Default |
| ---------------- | ----------------------------------- | ------- |
| `<server-ip>`    | Server IP address                   | —       |
| `<server-port>`  | TCP port to connect to              | —       |
| `<num-threads>`  | Number of concurrent worker threads | —       |
| `<duration-sec>` | Total test duration                 | —       |
| `[key-space]`    | Maximum number of unique keys       | 10000   |

### Example

```bash
./kv-loadgen 127.0.0.1 9000 8 30 20000
```
This runs 8 client threads against the server for 30 seconds with a key-space of 20,000.

---

## Sample Output

```bash
Running load: server=127.0.0.1:9000 threads=8 duration=30 sec key-space=20000

---- Results ----
Duration (s): 30.002
Successful requests: 238912
Failed requests: 120
Throughput (req/s): 7963.52
Average response time (ms): 0.1263
Unique keys created (approx): 19875
------------------
```

---

## Implementation Details
- Each thread maintains local counters aggregated atomically at the end.
- Uses _Atomic operations for inter-thread safety.
- Adaptive exponential backoff for reconnect attempts (50ms → 1000ms cap).
- Nanosecond-precision timestamps for latency measurement.
- Graceful stop via stop_flag after duration expiry.
- Random seed per thread ensures distinct operation patterns.

---



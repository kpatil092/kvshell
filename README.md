# Key-Value Storage System (C, Socket Programming)

A simple **multi-threaded** client-server key-value store implemented in C using TCP sockets.  
The server maintains key-value pairs in memory, and the client provides both **interactive** and **batch** modes to manipulate stored data via a custom binary protocol.  

---

## Features
- Supports **Create, Read, Update, Delete (CRUD)** operations
- Keys are **integers**, values are **arbitrary-length byte strings**
- Each client connection is handled by a dedicated thread
- Custom binary protocol for reliable transmission over TCP
- Client supports:
  - **Interactive mode**: run commands with a prompt
  - **Batch mode**: execute commands from a file
- In-memory hash-table–based store with persistence across client sessions
- Handles TCP packet-splitting via exact-length `read_n/write_n` helpers
- Human-readable error responses
- Single-client server (simple to extend for concurrency)

---

## Protocol

### Client → Server (Request)
A fixed **9-byte header** followed by an optional value:
- byte 0 : op (ASCII) – 'C' = create, 'R' = read, 'U' = update, 'D' = delete
- bytes 1-4 : int32_t key (network byte order)
- bytes 5-8 : int32_t value_size (network byte order; 0 if no value follows)
- bytes 9.. : value (raw bytes, length = value_size)

### Server → Client (Response)
A fixed **5-byte header** followed by payload:
- byte 0 : status – 'O' (OK) or 'E' (Error)
- bytes 1-4 : int32_t payload_size (network byte order)
- bytes 5.. : payload
- On **successful READ**: payload = value bytes  
- On **other successes**: payload = `"OK"`  
- On **errors**: payload = ASCII error message  

---

## Build Instructions
Compile the server and client using `gcc`:
```bash
gcc -o kv-server kv-server.c
gcc -o kv-client kv-client.c
```

## Running the server
```bash
./kv-server <bind-ip> <port>
```

## Running the Client

### Interactive mode
```bash 
./kv-client interactive
```

### Batch Mode
```bash
./kv-client batch commands.txt
```
---

## Supported Client Commands

- `connect <server-ip> <server-port>`
- `disconnect`
- `create <key> <value-size> <value>`
- `read <key>`
- `update <key> <value-size> <value>`
- `delete <key>`

---

## Implementation Notes

- Server stores key-value pairs in a chained hash table
- Memory dynamically allocated for values and freed on deletion
- Server ignores SIGPIPE so broken pipes return errors instead of crashing
- Client enforces active connection before running operations
- Values may be multi-line in batch/interactive mode: if fewer bytes than <value-size> are provided on one line, the client reads additional input until the full value is collected
- No authentication, no concurrency — can be extended
- Errors returned as ASCII strings for easier debugging


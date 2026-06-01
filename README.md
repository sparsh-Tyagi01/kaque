# Kaque

> A lightweight, Kafka-inspired message broker built from scratch in Go.

Kaque is a minimal message broker that implements core pub/sub messaging concepts — topics, partitions, producers, consumers, and log-segment storage — using only the Go standard library and raw TCP.

---

## 🚀 Features

- **TCP-based broker** listening on port `9092` (Kafka-compatible port)
- **Topic management** with multi-partition support
- **FNV-based partition routing** — messages are deterministically routed to partitions via hash of the message value
- **Append-only log storage** — messages are persisted to disk as newline-delimited JSON
- **Segment rotation** — active segment is rotated when it exceeds 1 MB
- **Segment cleanup** — old segments can be purged to reclaim disk space
- **Offset-based message consumption** — consumers can read from any offset across all segments
- **Concurrent-safe** — partition writes are protected with `sync.Mutex`
- **Zero dependencies** — pure Go standard library, no external packages

---

## 📐 Architecture

```
                          ┌─────────────────────────────────────┐
                          │              Broker (TCP :9092)      │
                          │                                      │
  Producer ─── TCP ──►   │  Topic: "chat"                       │
                          │  ├── Partition 0  ──► Segment(s)    │
  Consumer ◄── TCP ───   │  ├── Partition 1  ──► Segment(s)    │
                          │  └── Partition 2  ──► Segment(s)    │
                          └─────────────────────────────────────┘
```

### Internal Packages

| Package | Responsibility |
|---|---|
| `internal/broker` | Manages topics; handles TCP connection logic; FNV hashing for partition routing |
| `internal/topic` | Topic struct holding a named collection of partitions |
| `internal/partition` | Append/read messages; segment rotation & cleanup; offset tracking |
| `internal/storage` | Low-level segment file management (open, append, size tracking) |
| `internal/protocol` | Wire format — `Message` and `Request` structs serialized as JSON |

---

## 📁 Project Structure

```
kaque/
├── cmd/
│   └── broker/
│       └── main.go          # Broker entry point
├── consumer/
│   └── consumer.go          # Sample consumer client
├── producer/
│   └── producer.go          # Sample producer client
├── internal/
│   ├── broker/
│   │   ├── broker.go        # Broker struct & topic registry
│   │   └── hash.go          # FNV-32a partition hash
│   ├── partition/
│   │   └── partition.go     # Partition: append, read, rotate, cleanup
│   ├── protocol/
│   │   ├── message.go       # Message wire format
│   │   └── request.go       # Request wire format
│   ├── storage/
│   │   └── segment.go       # Segment file I/O
│   └── topic/
│       └── topic.go         # Topic struct
├── data/                    # Log files (git-ignored)
├── go.mod
└── .gitignore
```

---

## 🛠️ Getting Started

### Prerequisites

- Go `1.21+`

### Run the Broker

```bash
go run ./cmd/broker/
```

The broker will start on **TCP port 9092** and create a `chat` topic with 3 partitions backed by log files in `data/`.

### Run the Producer

In a new terminal:

```bash
go run ./producer/producer.go
```

This sends a sample produce request to the broker:

```json
{ "action": "Produce", "topic": "chat", "value": "Hello there!, I am John Doe" }
```

Expected response: `STORED`

### Run the Consumer

In another terminal:

```bash
go run ./consumer/consumer.go
```

This reads all messages from partition `0` starting at offset `0`:

```json
{ "action": "Consume", "topic": "chat", "offset": 0, "partition": 0 }
```

---

## 📨 Wire Protocol

All communication is over a persistent TCP connection using newline-delimited JSON (`\n`).

### Produce Request

```json
{
  "action": "Produce",
  "topic": "chat",
  "value": "your message here"
}
```

**Response:** `STORED\n`

### Consume Request

```json
{
  "action": "Consume",
  "topic": "chat",
  "partition": 0,
  "offset": 0
}
```

**Response:** JSON array of `Message` objects:

```json
[
  {
    "offset": 0,
    "partition": 0,
    "key": null,
    "value": "SGVsbG8gdGhlcmUh...",
    "time": "2026-06-01T19:00:00Z"
  }
]
```

> **Note:** `value` is a base64-encoded byte slice (`[]byte` in Go's JSON encoding).

---

## 🗂️ Storage Format

Each partition maps to one or more **segment files** (`.log`) stored in the `data/` directory.

- Messages are appended as newline-delimited JSON objects.
- When the active segment exceeds **1 MB**, a new segment is automatically created (`data/chat-{partitionID}-{baseOffset}.log`).
- Older segments can be cleaned up via `partition.Cleanup()`.

---

## 🔀 Partition Routing

Producers do not select a partition — the broker routes messages automatically using **FNV-32a hashing** on the message value:

```
partitionIndex = FNV32a(message.value) % numPartitions
```

This ensures that identical values always land on the same partition.

---

## 🗺️ Roadmap

- [ ] Consumer groups with offset tracking
- [ ] Multi-topic support at runtime (dynamic topic creation via the wire protocol)
- [ ] Key-based routing in addition to value-based routing
- [ ] Replication support for fault tolerance
- [ ] Index files for O(1) offset lookups (instead of linear scan)
- [ ] CLI tool for producing/consuming from the terminal

---

## 📄 License

MIT

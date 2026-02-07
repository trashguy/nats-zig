# nats-zig

Pure Zig [NATS](https://nats.io) client library. Zero dependencies beyond `std`.

Requires Zig 0.15+.

## Features

### Core NATS
- Pub/sub messaging with subject validation
- Request/reply pattern with auto-generated inboxes
- Queue group subscriptions for load balancing
- NATS headers (HMSG/HPUB)
- Wire protocol parser/serializer
- Buffered TCP I/O (64KB read/write buffers)

### Connection Management
- Automatic reconnection with exponential backoff and jitter
- Server mesh discovery via `connect_urls`
- Ping/pong heartbeats with configurable intervals
- Token and username/password authentication
- URL-embedded credentials (`nats://user:pass@host`)
- Connection status tracking (disconnected, connected, reconnecting, draining)
- TLS via `std.crypto.tls` (auto-upgrade on `tls://` URLs or server requirement)
- Configurable certificate verification (`tls_verify` option)

### JetStream
- Stream CRUD (create, update, delete, info)
- Consumer management (create, delete, durable and ephemeral)
- Publish with acknowledgment (`PubAck` with stream, sequence, duplicate flag)
- Pull subscriptions with batch fetch
- Message ack/nak
- Configurable retention, storage, replication, and discard policies

### KeyValue Store
- Bucket create, open, and destroy
- Key operations: get, put, delete
- Revision tracking via JetStream sequence numbers
- Backed by JetStream streams (`KV_<bucket>`)
- Configurable history, TTL, max value size, storage, and replication
- Watch for key changes with `Watcher` (put, delete, purge operations)
- Keys-only watch mode (headers only, no values transferred)

## Usage

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .nats_zig = .{
        .url = "https://github.com/trashguy/nats-zig/archive/refs/heads/main.tar.gz",
        // .hash = "...",
    },
},
```

In `build.zig`:

```zig
const nats_dep = b.dependency("nats_zig", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("nats", nats_dep.module("nats"));
```

### Connect and Publish

```zig
const nats = @import("nats");

var client = try nats.Client.connect(allocator, .{
    .servers = &.{"nats://127.0.0.1:4222"},
    .name = "my-app",
});
defer client.close();

try client.publish("greet.joe", "hello!");
```

### Subscribe

```zig
const sub = try client.subscribe("greet.*", .{});

// Process incoming messages (non-blocking with short timeout)
try client.processIncoming();

if (sub.nextMsg()) |*msg| {
    defer msg.deinit();
    std.debug.print("received: {s}\n", .{msg.payload.?});
}
```

### Request/Reply

```zig
var reply = try client.request("service.add", "2+2", 5000);
defer reply.deinit();
std.debug.print("answer: {s}\n", .{reply.payload.?});
```

### Queue Groups

```zig
// Load-balanced across subscribers in the "workers" group
const sub = try client.subscribe("tasks.>", .{ .queue_group = "workers" });
```

### JetStream

```zig
var js = client.jetStream();

// Create a stream
try js.createStream(.{ .name = "ORDERS", .subjects = &.{"orders.>"} });

// Publish with ack
const ack = try js.publish("orders.new", "order-data");

// Create a consumer and fetch messages
try js.createConsumer("ORDERS", .{ .durable_name = "processor" });
var pull = try js.pullSubscribe("ORDERS", "processor");
defer pull.close();

const msgs = try pull.fetch(10, 5000);
for (msgs) |*msg| {
    defer msg.deinit();
    try pull.ack(msg);
}
```

### KeyValue Store

```zig
var js = client.jetStream();

// Create a bucket
var kv = try nats.KeyValue.create(js, "config", .{});
defer kv.destroy();

// Put and get values
const rev = try kv.put("app.setting", "value");
if (try kv.get("app.setting")) |entry| {
    defer entry.deinit();
    std.debug.print("value: {s}, revision: {d}\n", .{ entry.value, entry.revision });
}

// Delete a key
try kv.delete("app.setting");
```

## Building

```bash
zig build               # Compile
zig build test           # Run all tests
zig build test-unit      # Unit tests only
zig build test-integration # Integration tests (uses mock server)
zig build test-tls       # TLS tests (requires TLS NATS server, see below)
```

### TLS Testing

TLS tests require a NATS server with TLS enabled on port 4443:

```bash
# Generate self-signed certs for testing (or use your own)
nats-server --tls --tlscert=server-cert.pem --tlskey=server-key.pem -p 4443

# Run TLS tests
zig build test-tls
```

If no TLS server is available, the tests skip gracefully.

## Architecture

```
Client (public API)
├── publish(), subscribe(), request()
├── Subscription Manager (sid allocation, dispatch)
├── Protocol (wire parser + serializer)
├── Connection (TCP, buffered I/O, reconnect, TLS)
├── JetStream (streams, consumers, pull subscriptions)
│   └── PullSubscription (fetch, ack, nak)
├── KeyValue (buckets, get/put/delete, watch)
│   ├── backed by JetStream streams
│   └── Watcher (live key change notifications)
└── Headers (HMSG/HPUB support)
```

## License

MIT

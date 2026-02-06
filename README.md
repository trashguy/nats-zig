# nats-zig

Pure Zig [NATS](https://nats.io) client library. Zero dependencies beyond `std`.

Requires Zig 0.14+.

## Features

- Core pub/sub messaging
- Request/reply pattern
- Queue group subscriptions
- NATS headers (HMSG)
- Wire protocol parser/serializer
- Buffered TCP I/O

### Planned (future sessions)

- Reconnection with exponential backoff
- TLS via `std.crypto.tls`
- JetStream (streams, consumers, publish+ack)
- KeyValue store (get/put/delete/watch)

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

## Building

```bash
zig build          # Compile
zig build test     # Run all tests
```

## Architecture

```
Client (public API)
├── publish(), subscribe(), request()
├── Subscription Manager (sid allocation, dispatch)
├── Protocol (wire parser + serializer)
└── Connection (TCP, buffered I/O)
```

## License

MIT

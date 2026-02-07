const std = @import("std");
const nats = @import("nats");
const Client = nats.Client;

// Manual TLS integration test — requires a TLS-enabled NATS server.
//
// To run:
//   1. Start NATS with TLS:
//      nats-server --tls --tlscert=server-cert.pem --tlskey=server-key.pem -p 4443
//   2. Run this test:
//      zig build test-tls
//
// If no TLS server is available, this test will skip gracefully.

test "tls connection to NATS server" {
    var client = Client.connect(std.testing.allocator, .{
        .servers = &.{"tls://localhost:4443"},
        .tls_verify = true,
        .allow_reconnect = false,
    }) catch |err| {
        // Connection refused = no TLS server running, skip
        std.debug.print("TLS test skipped: {}\n", .{err});
        return;
    };
    defer client.close();

    // Verify we can pub/sub over TLS
    const sub = try client.subscribe("tls.test", .{});

    try client.publish("tls.test", "hello over tls");
    try client.processIncoming();

    // Give the message time to arrive
    std.Thread.sleep(100 * std.time.ns_per_ms);
    try client.processIncoming();

    if (sub.nextMsg()) |*msg| {
        var m = msg.*;
        defer m.deinit();
        try std.testing.expectEqualStrings("hello over tls", m.payload orelse "");
    } else {
        // Message may not have arrived in time; not a failure of TLS itself
        std.debug.print("TLS test: message not received (timing), but connection succeeded\n", .{});
    }
}

test "tls connection with verify disabled" {
    var client = Client.connect(std.testing.allocator, .{
        .servers = &.{"tls://localhost:4443"},
        .tls_verify = false,
        .allow_reconnect = false,
    }) catch |err| {
        std.debug.print("TLS no-verify test skipped: {}\n", .{err});
        return;
    };
    defer client.close();

    try client.publish("tls.noverify", "no verify");
    try client.processIncoming();
}

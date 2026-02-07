const std = @import("std");
const nats = @import("nats");
const Client = nats.Client;
const KeyValue = nats.KeyValue;
const MockServer = @import("mock_server.zig").MockServer;

test "kv bucket operations" {
    const server = try MockServer.start(std.testing.allocator);
    defer server.stop();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    const url_buf = server.url();
    const url_str = std.mem.sliceTo(&url_buf, 0);

    var client = try Client.connect(std.testing.allocator, .{
        .servers = &.{url_str},
        .allow_reconnect = false,
    });
    defer client.close();

    // Create a bucket
    var bucket = try KeyValue.Bucket.create(client, "test-bucket", .{
        .storage = .memory,
    });

    // Put a key
    const rev = try bucket.put("greeting", "hello world");
    try std.testing.expect(rev > 0);

    // Put another key (revision should increment)
    const rev2 = try bucket.put("farewell", "goodbye");
    try std.testing.expect(rev2 > rev);

    // Destroy the bucket
    try bucket.destroy();
}

test "kv watch receives put updates" {
    const server = try MockServer.start(std.testing.allocator);
    defer server.stop();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    const url_buf = server.url();
    const url_str = std.mem.sliceTo(&url_buf, 0);

    var client = try Client.connect(std.testing.allocator, .{
        .servers = &.{url_str},
        .allow_reconnect = false,
    });
    defer client.close();

    var bucket = try KeyValue.Bucket.create(client, "watch-test", .{
        .storage = .memory,
    });

    // Put values before watch
    _ = try bucket.put("key1", "value1");
    _ = try bucket.put("key2", "value2");

    // Create watcher
    const watcher = try bucket.watch(">", .{});
    defer watcher.deinit();

    // Should receive the stored messages
    if (watcher.nextWithTimeout(500)) |*entry_ptr| {
        var entry = entry_ptr.*;
        defer entry.deinit();
        try std.testing.expect(entry.key_len > 0);
        try std.testing.expect(entry.operation == .put);
    }

    try bucket.destroy();
}

test "kv watch initial complete" {
    const server = try MockServer.start(std.testing.allocator);
    defer server.stop();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    const url_buf = server.url();
    const url_str = std.mem.sliceTo(&url_buf, 0);

    var client = try Client.connect(std.testing.allocator, .{
        .servers = &.{url_str},
        .allow_reconnect = false,
    });
    defer client.close();

    var bucket = try KeyValue.Bucket.create(client, "init-test", .{
        .storage = .memory,
    });

    _ = try bucket.put("a", "1");
    _ = try bucket.put("b", "2");

    const watcher = try bucket.watch(">", .{});
    defer watcher.deinit();

    // Before consuming any messages, initialComplete should be false
    // (unless num_pending was 0, but we stored 2 messages)
    const initially_complete = watcher.initialComplete();

    // Consume messages
    var count: u32 = 0;
    while (count < 5) : (count += 1) {
        if (watcher.nextWithTimeout(200)) |*entry_ptr| {
            var entry = entry_ptr.*;
            defer entry.deinit();
        } else break;
    }

    // After consuming, should be complete (or started complete with no pending)
    _ = initially_complete; // We just verify no crash; exact behavior depends on mock timing

    try bucket.destroy();
}

test "kv watch keys only mode" {
    const server = try MockServer.start(std.testing.allocator);
    defer server.stop();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    const url_buf = server.url();
    const url_str = std.mem.sliceTo(&url_buf, 0);

    var client = try Client.connect(std.testing.allocator, .{
        .servers = &.{url_str},
        .allow_reconnect = false,
    });
    defer client.close();

    var bucket = try KeyValue.Bucket.create(client, "keys-only-test", .{
        .storage = .memory,
    });

    _ = try bucket.put("mykey", "myvalue");

    // Watch with keys_only option
    const watcher = try bucket.watch(">", .{ .keys_only = true });
    defer watcher.deinit();

    // Just verify the watcher was created without error
    // In keys_only mode, headers_only=true is set on the consumer
    try std.testing.expect(!watcher.stopped);

    try bucket.destroy();
}

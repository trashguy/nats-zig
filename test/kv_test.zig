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

const std = @import("std");
const nats = @import("nats");
const Client = nats.Client;
const MockServer = @import("mock_server.zig").MockServer;

test "connect to mock server" {
    const server = try MockServer.start(std.testing.allocator);
    defer server.stop();

    std.Thread.sleep(10 * std.time.ns_per_ms);

    const url_buf = server.url();
    const url_str = std.mem.sliceTo(&url_buf, 0);

    var client = try Client.connect(std.testing.allocator, .{
        .servers = &.{url_str},
        .name = "test-client",
        .allow_reconnect = false,
    });
    defer client.close();

    try std.testing.expect(client.status == .connected);
}

test "publish message" {
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

    try client.publish("test.subject", "hello world");
}

test "subscribe and receive message" {
    const server = try MockServer.start(std.testing.allocator);
    defer server.stop();

    std.Thread.sleep(10 * std.time.ns_per_ms);

    const url_buf = server.url();
    const url_str = std.mem.sliceTo(&url_buf, 0);

    var client = try Client.connect(std.testing.allocator, .{
        .servers = &.{url_str},
        .echo = true,
        .allow_reconnect = false,
    });
    defer client.close();

    const sub = try client.subscribe("test.echo", .{});
    _ = sub;

    try client.publish("test.echo", "echo test");
    std.Thread.sleep(50 * std.time.ns_per_ms);
    try client.processIncoming();
}

test "connect failure - no server" {
    const result = Client.connect(std.testing.allocator, .{
        .servers = &.{"nats://127.0.0.1:19999"},
        .allow_reconnect = false,
    });
    try std.testing.expectError(error.ConnectionFailed, result);
}

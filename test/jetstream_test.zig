const std = @import("std");
const nats = @import("nats");
const Client = nats.Client;
const MockServer = @import("mock_server.zig").MockServer;

test "jetstream operations" {
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

    var js = client.jetStream();
    js.timeout_ms = 2000;

    // Test: context properties
    try std.testing.expectEqualStrings("$JS.API", js.api_prefix);

    // Test: create stream
    const create_info = try js.createStream(.{
        .name = "ORDERS",
        .subjects = &.{"orders.>"},
        .storage = .memory,
    });
    try std.testing.expect(create_info.first_seq == 1);

    // Test: update stream
    const update_info = try js.updateStream(.{
        .name = "ORDERS",
        .subjects = &.{ "orders.>", "returns.>" },
    });
    try std.testing.expect(update_info.first_seq == 1);

    // Test: stream info
    const info = try js.streamInfo("ORDERS");
    try std.testing.expect(info.first_seq == 1);

    // Test: publish with ack (incrementing seq)
    const ack1 = try js.publish("orders.new", "{\"id\":1}");
    try std.testing.expect(ack1.seq > 0);
    try std.testing.expect(ack1.duplicate == false);

    const ack2 = try js.publish("orders.update", "{\"id\":2}");
    try std.testing.expect(ack2.seq > ack1.seq);

    // Test: create consumer
    const consumer_info = try js.createConsumer("ORDERS", .{
        .durable_name = "worker",
        .ack_policy = .explicit,
    });
    try std.testing.expect(consumer_info.num_pending == 0);

    // Test: delete consumer
    try js.deleteConsumer("ORDERS", "worker");

    // Test: delete stream
    try js.deleteStream("ORDERS");
}

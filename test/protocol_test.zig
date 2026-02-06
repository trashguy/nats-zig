const std = @import("std");
const nats = @import("nats");
const Protocol = nats.Protocol;
const Headers = nats.Headers;

test "parse sequence: INFO → MSG with payload" {
    var parser = Protocol.Parser.init(std.testing.allocator);
    defer parser.deinit();

    // Step 1: INFO
    const info_result = try parser.parse(
        \\INFO {"server_id":"test-server","version":"2.10.0","proto":1,"max_payload":1048576,"headers":true}
    );
    try std.testing.expect(info_result != null);
    try std.testing.expect(info_result.? == .info);

    // Step 2: MSG header
    const msg_header = try parser.parse("MSG foo.bar 1 13");
    try std.testing.expect(msg_header == null); // waiting for payload
    try std.testing.expect(parser.pendingBytes() == 13);

    // Step 3: MSG payload
    const msg_result = try parser.parse("hello, world!");
    try std.testing.expect(msg_result != null);
    switch (msg_result.?) {
        .msg => |m| {
            var msg = m;
            defer msg.deinit();
            try std.testing.expectEqualStrings("foo.bar", msg.subject);
            try std.testing.expectEqualStrings("1", msg.sid);
            try std.testing.expectEqualStrings("hello, world!", msg.payload.?);
        },
        else => return error.UnexpectedResult,
    }
}

test "parse multiple operations in sequence" {
    var parser = Protocol.Parser.init(std.testing.allocator);
    defer parser.deinit();

    // PING
    try std.testing.expect((try parser.parse("PING")).? == .ping);

    // +OK
    try std.testing.expect((try parser.parse("+OK")).? == .ok);

    // MSG
    _ = try parser.parse("MSG test 1 3");
    const result = try parser.parse("abc");
    try std.testing.expect(result != null);
    var msg = result.?.msg;
    defer msg.deinit();
    try std.testing.expectEqualStrings("abc", msg.payload.?);

    // PONG
    try std.testing.expect((try parser.parse("PONG")).? == .pong);
}

test "parse HMSG with headers" {
    var parser = Protocol.Parser.init(std.testing.allocator);
    defer parser.deinit();

    // HMSG header: subject sid hdr_len total_len
    const hdr_bytes = "NATS/1.0\r\nX-Test: value\r\n\r\n";
    const payload_bytes = "data";
    const hdr_len = hdr_bytes.len; // 27
    const total_len = hdr_len + payload_bytes.len; // 31

    var header_line_buf: [128]u8 = undefined;
    const header_line = std.fmt.bufPrint(&header_line_buf, "HMSG test.subject 5 {d} {d}", .{ hdr_len, total_len }) catch unreachable;

    const r1 = try parser.parse(header_line);
    try std.testing.expect(r1 == null); // need headers + payload
    try std.testing.expect(parser.pendingBytes() == total_len);

    // Combine headers + payload
    var full_data: [64]u8 = undefined;
    @memcpy(full_data[0..hdr_bytes.len], hdr_bytes);
    @memcpy(full_data[hdr_bytes.len .. hdr_bytes.len + payload_bytes.len], payload_bytes);

    const r2 = try parser.parse(full_data[0..total_len]);
    try std.testing.expect(r2 != null);
    switch (r2.?) {
        .msg => |m| {
            var msg = m;
            defer msg.deinit();
            try std.testing.expectEqualStrings("test.subject", msg.subject);
            try std.testing.expectEqualStrings("5", msg.sid);
            try std.testing.expect(msg.headers != null);
            try std.testing.expectEqualStrings("value", msg.headers.?.get("X-Test").?);
            try std.testing.expectEqualStrings("data", msg.payload.?);
        },
        else => return error.UnexpectedResult,
    }
}

test "serialize round-trip: PUB and SUB" {
    var buf: [512]u8 = undefined;

    // PUB with payload
    const pub_cmd = try Protocol.fmtPub(&buf, "events.click", null, "click data");
    try std.testing.expect(std.mem.startsWith(u8, pub_cmd, "PUB events.click 10\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, pub_cmd, "click data\r\n"));

    // SUB
    const sub_cmd = try Protocol.fmtSub(&buf, "events.>", null, 42);
    try std.testing.expectEqualStrings("SUB events.> 42\r\n", sub_cmd);

    // UNSUB with max
    const unsub_cmd = try Protocol.fmtUnsub(&buf, 42, 1);
    try std.testing.expectEqualStrings("UNSUB 42 1\r\n", unsub_cmd);
}

test "serialize CONNECT with auth" {
    var buf: [2048]u8 = undefined;
    const result = try Protocol.fmtConnect(&buf, .{
        .user = "admin",
        .pass = "secret",
        .name = "test-app",
        .verbose = false,
        .echo = true,
    });

    try std.testing.expect(std.mem.startsWith(u8, result, "CONNECT {"));
    try std.testing.expect(std.mem.indexOf(u8, result, "\"user\":\"admin\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"pass\":\"secret\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"test-app\"") != null);
}

test "error on invalid protocol line" {
    var parser = Protocol.Parser.init(std.testing.allocator);
    defer parser.deinit();

    const result = parser.parse("GARBAGE invalid");
    try std.testing.expectError(Protocol.ParseError.InvalidProtocol, result);
}

test "headers round-trip" {
    // Create headers
    var entries = [_]Headers.Entry{
        .{ .key = "Nats-Msg-Id", .value = "abc-123" },
        .{ .key = "Content-Type", .value = "application/json" },
    };
    const hdrs = Headers{ .entries = &entries };

    // Serialize
    var buf: [512]u8 = undefined;
    const serialized = try hdrs.serialize(&buf);

    // Parse back
    var parsed = try Headers.parse(std.testing.allocator, serialized);
    defer {
        for (parsed.entries) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(parsed.entries);
    }

    try std.testing.expect(parsed.entries.len == 2);
    try std.testing.expectEqualStrings("abc-123", parsed.get("Nats-Msg-Id").?);
    try std.testing.expectEqualStrings("application/json", parsed.get("Content-Type").?);
}

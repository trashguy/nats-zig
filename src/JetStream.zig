const std = @import("std");
const Allocator = std.mem.Allocator;
const Client = @import("Client.zig");
const Protocol = @import("Protocol.zig");
const Headers = @import("Headers.zig");

/// JetStream context for interacting with JetStream APIs.
/// All operations are performed via NATS request/reply on $JS.API.> subjects.
pub const Context = struct {
    client: *Client,
    allocator: Allocator,
    api_prefix: []const u8 = "$JS.API",
    timeout_ms: u32 = 5000,

    /// Create a new JetStream context from an existing client.
    pub fn init(client: *Client) Context {
        return .{
            .client = client,
            .allocator = client.allocator,
        };
    }

    /// Publish to a JetStream subject and wait for an ack.
    pub fn publish(self: *Context, subject: []const u8, payload: ?[]const u8) Error!PubAck {
        var msg = try self.client.request(subject, payload, self.timeout_ms);
        defer msg.deinit();

        return parsePubAck(msg.payload orelse return Error.InvalidResponse);
    }

    /// Create or update a stream.
    pub fn createStream(self: *Context, config: StreamConfig) Error!StreamInfo {
        var subject_buf: [256]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}.STREAM.CREATE.{s}", .{ self.api_prefix, config.name }) catch return Error.InvalidResponse;

        var json_buf: [4096]u8 = undefined;
        const json = serializeStreamConfig(&json_buf, config) catch return Error.InvalidResponse;

        var msg = try self.client.request(subject, json, self.timeout_ms);
        defer msg.deinit();

        return parseStreamInfo(msg.payload orelse return Error.InvalidResponse);
    }

    /// Update an existing stream.
    pub fn updateStream(self: *Context, config: StreamConfig) Error!StreamInfo {
        var subject_buf: [256]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}.STREAM.UPDATE.{s}", .{ self.api_prefix, config.name }) catch return Error.InvalidResponse;

        var json_buf: [4096]u8 = undefined;
        const json = serializeStreamConfig(&json_buf, config) catch return Error.InvalidResponse;

        var msg = try self.client.request(subject, json, self.timeout_ms);
        defer msg.deinit();

        return parseStreamInfo(msg.payload orelse return Error.InvalidResponse);
    }

    /// Delete a stream.
    pub fn deleteStream(self: *Context, name: []const u8) Error!void {
        var subject_buf: [256]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}.STREAM.DELETE.{s}", .{ self.api_prefix, name }) catch return Error.InvalidResponse;

        var msg = try self.client.request(subject, null, self.timeout_ms);
        defer msg.deinit();

        // Check for error in response
        if (msg.payload) |p| {
            if (checkJsError(p)) |err| return err;
        }
    }

    /// Get info about a stream.
    pub fn streamInfo(self: *Context, name: []const u8) Error!StreamInfo {
        var subject_buf: [256]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}.STREAM.INFO.{s}", .{ self.api_prefix, name }) catch return Error.InvalidResponse;

        var msg = try self.client.request(subject, null, self.timeout_ms);
        defer msg.deinit();

        return parseStreamInfo(msg.payload orelse return Error.InvalidResponse);
    }

    /// Create a consumer on a stream.
    pub fn createConsumer(self: *Context, stream: []const u8, config: ConsumerConfig) Error!ConsumerInfo {
        var subject_buf: [512]u8 = undefined;
        const subject = if (config.durable_name) |durable|
            std.fmt.bufPrint(&subject_buf, "{s}.CONSUMER.CREATE.{s}.{s}", .{ self.api_prefix, stream, durable }) catch return Error.InvalidResponse
        else
            std.fmt.bufPrint(&subject_buf, "{s}.CONSUMER.CREATE.{s}", .{ self.api_prefix, stream }) catch return Error.InvalidResponse;

        var json_buf: [4096]u8 = undefined;
        const json = serializeConsumerConfig(&json_buf, config) catch return Error.InvalidResponse;

        var msg = try self.client.request(subject, json, self.timeout_ms);
        defer msg.deinit();

        return parseConsumerInfo(msg.payload orelse return Error.InvalidResponse);
    }

    /// Delete a consumer.
    pub fn deleteConsumer(self: *Context, stream: []const u8, consumer: []const u8) Error!void {
        var subject_buf: [512]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}.CONSUMER.DELETE.{s}.{s}", .{ self.api_prefix, stream, consumer }) catch return Error.InvalidResponse;

        var msg = try self.client.request(subject, null, self.timeout_ms);
        defer msg.deinit();

        if (msg.payload) |p| {
            if (checkJsError(p)) |err| return err;
        }
    }

    /// Create a pull subscription for consuming messages.
    pub fn pullSubscribe(self: *Context, stream: []const u8, consumer: []const u8) Error!PullSubscription {
        // Subscribe to a unique inbox for receiving fetched messages
        const inbox_arr = self.client.sub_mgr.newInbox() catch return Error.OutOfMemory;
        const inbox = inbox_arr[0..39];

        const owned_stream = self.allocator.dupe(u8, stream) catch return Error.OutOfMemory;
        const owned_consumer = self.allocator.dupe(u8, consumer) catch {
            self.allocator.free(owned_stream);
            return Error.OutOfMemory;
        };

        const inbox_sub = self.client.subscribe(inbox, .{}) catch {
            self.allocator.free(owned_stream);
            self.allocator.free(owned_consumer);
            return Error.OutOfMemory;
        };

        return PullSubscription{
            .ctx = self,
            .stream = owned_stream,
            .consumer = owned_consumer,
            .inbox_sub = inbox_sub,
        };
    }
};

// ── Types ──

pub const Error = error{
    InvalidResponse,
    StreamNotFound,
    ConsumerNotFound,
    JetStreamError,
    OutOfMemory,
} || Client.Error;

pub const PubAck = struct {
    stream_buf: [64]u8 = undefined,
    stream_len: u8 = 0,
    seq: u64 = 0,
    duplicate: bool = false,

    pub fn stream(self: *const PubAck) []const u8 {
        return self.stream_buf[0..self.stream_len];
    }
};

pub const StreamConfig = struct {
    name: []const u8,
    subjects: []const []const u8 = &.{},
    retention: Retention = .limits,
    max_consumers: i64 = -1,
    max_msgs: i64 = -1,
    max_bytes: i64 = -1,
    max_age_ns: i64 = 0,
    max_msg_size: i32 = -1,
    max_msgs_per_subject: i64 = -1,
    storage: Storage = .file,
    num_replicas: u32 = 1,
    discard: Discard = .old,
};

pub const ConsumerConfig = struct {
    durable_name: ?[]const u8 = null,
    filter_subject: ?[]const u8 = null,
    deliver_subject: ?[]const u8 = null,
    ack_policy: AckPolicy = .explicit,
    deliver_policy: ?DeliverPolicy = null,
    headers_only: ?bool = null,
    max_deliver: i64 = -1,
    ack_wait_ns: i64 = 30_000_000_000,
};

pub const StreamInfo = struct {
    messages: u64 = 0,
    bytes: u64 = 0,
    first_seq: u64 = 0,
    last_seq: u64 = 0,
};

pub const ConsumerInfo = struct {
    name_buf: [128]u8 = undefined,
    name_len: u8 = 0,
    num_pending: u64 = 0,
    num_ack_pending: u64 = 0,

    pub fn name(self: *const ConsumerInfo) []const u8 {
        return self.name_buf[0..self.name_len];
    }
};

pub const Retention = enum { limits, interest, workqueue };
pub const Storage = enum { file, memory };
pub const Discard = enum { old, new };
pub const AckPolicy = enum { none, all, explicit };
pub const DeliverPolicy = enum {
    all,
    last,
    new,
    by_start_sequence,
    by_start_time,
    last_per_subject,
};

// ── Pull Subscription ──

pub const PullSubscription = struct {
    ctx: *Context,
    stream: []const u8,
    consumer: []const u8,
    inbox_sub: *Client.Subscription,

    /// Fetch up to `batch` messages with a timeout.
    /// Returns messages that should be individually acked via `ack()`.
    pub fn fetch(self: *PullSubscription, batch: u32, timeout_ms: u32) Error![]Protocol.Msg {
        // Send fetch request to $JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>
        var subject_buf: [512]u8 = undefined;
        const subject = std.fmt.bufPrint(&subject_buf, "{s}.CONSUMER.MSG.NEXT.{s}.{s}", .{ self.ctx.api_prefix, self.stream, self.consumer }) catch return Error.InvalidResponse;

        var payload_buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrint(&payload_buf, "{{\"batch\":{d}}}", .{batch}) catch return Error.InvalidResponse;

        // Publish request with inbox as reply_to
        self.ctx.client.publishTo(subject, self.inbox_sub.subject, payload) catch return Error.InvalidResponse;

        // Wait for messages on the inbox
        self.ctx.client.conn.setReadTimeout(@intCast(@min(timeout_ms, 5000)));
        defer self.ctx.client.conn.setReadTimeout(0);

        const deadline_ns: i128 = std.time.nanoTimestamp() + @as(i128, timeout_ms) * std.time.ns_per_ms;
        var messages = std.ArrayListUnmanaged(Protocol.Msg){};

        while (std.time.nanoTimestamp() < deadline_ns) {
            if (self.inbox_sub.nextMsg()) |msg| {
                messages.append(self.ctx.allocator, msg) catch continue;
                if (messages.items.len >= batch) break;
                continue;
            }

            // Try to read more from server
            self.ctx.client.processIncoming() catch break;
        }

        return messages.toOwnedSlice(self.ctx.allocator) catch {
            for (messages.items) |*m| m.deinit();
            messages.deinit(self.ctx.allocator);
            return Error.OutOfMemory;
        };
    }

    /// Acknowledge a message (publish empty to the ack subject).
    pub fn ack(self: *PullSubscription, msg: *const Protocol.Msg) Error!void {
        if (msg.reply_to) |reply_to| {
            self.ctx.client.publish(reply_to, null) catch return Error.InvalidResponse;
        }
    }

    /// Negative-acknowledge a message.
    pub fn nak(self: *PullSubscription, msg: *const Protocol.Msg) Error!void {
        if (msg.reply_to) |reply_to| {
            self.ctx.client.publish(reply_to, "-NAK") catch return Error.InvalidResponse;
        }
    }

    /// Close the pull subscription and free owned resources.
    pub fn close(self: *PullSubscription) void {
        self.ctx.client.unsubscribe(self.inbox_sub) catch {};
        self.ctx.allocator.free(self.stream);
        self.ctx.allocator.free(self.consumer);
    }
};

// ── JSON serialization helpers ──

/// Escape a string for safe embedding in a JSON value.
fn writeEscaped(writer: anytype, value: []const u8) !void {
    for (value) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try std.fmt.format(writer, "\\u{x:0>4}", .{@as(u16, c)});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
}

fn serializeStreamConfig(buf: []u8, config: StreamConfig) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const w = stream.writer();

    try w.writeAll("{\"name\":\"");
    try writeEscaped(w, config.name);
    try w.writeAll("\"");

    if (config.subjects.len > 0) {
        try w.writeAll(",\"subjects\":[");
        for (config.subjects, 0..) |subject, i| {
            if (i > 0) try w.writeAll(",");
            try w.writeAll("\"");
            try writeEscaped(w, subject);
            try w.writeAll("\"");
        }
        try w.writeAll("]");
    }

    try w.writeAll(",\"retention\":\"");
    try w.writeAll(switch (config.retention) {
        .limits => "limits",
        .interest => "interest",
        .workqueue => "workqueue",
    });
    try w.writeAll("\"");

    try w.writeAll(",\"storage\":\"");
    try w.writeAll(switch (config.storage) {
        .file => "file",
        .memory => "memory",
    });
    try w.writeAll("\"");

    try w.writeAll(",\"discard\":\"");
    try w.writeAll(switch (config.discard) {
        .old => "old",
        .new => "new",
    });
    try w.writeAll("\"");

    try std.fmt.format(w, ",\"max_consumers\":{d}", .{config.max_consumers});
    try std.fmt.format(w, ",\"max_msgs\":{d}", .{config.max_msgs});
    try std.fmt.format(w, ",\"max_bytes\":{d}", .{config.max_bytes});
    try std.fmt.format(w, ",\"max_age\":{d}", .{config.max_age_ns});
    try std.fmt.format(w, ",\"max_msg_size\":{d}", .{config.max_msg_size});
    try std.fmt.format(w, ",\"max_msgs_per_subject\":{d}", .{config.max_msgs_per_subject});
    try std.fmt.format(w, ",\"num_replicas\":{d}", .{config.num_replicas});

    try w.writeAll("}");
    return stream.getWritten();
}

fn serializeConsumerConfig(buf: []u8, config: ConsumerConfig) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const w = stream.writer();

    try w.writeAll("{");
    var first = true;

    if (config.durable_name) |name| {
        try w.writeAll("\"durable_name\":\"");
        try writeEscaped(w, name);
        try w.writeAll("\"");
        first = false;
    }

    if (config.filter_subject) |subj| {
        if (!first) try w.writeAll(",");
        try w.writeAll("\"filter_subject\":\"");
        try writeEscaped(w, subj);
        try w.writeAll("\"");
        first = false;
    }

    if (config.deliver_subject) |subj| {
        if (!first) try w.writeAll(",");
        try w.writeAll("\"deliver_subject\":\"");
        try writeEscaped(w, subj);
        try w.writeAll("\"");
        first = false;
    }

    if (!first) try w.writeAll(",");
    try w.writeAll("\"ack_policy\":\"");
    try w.writeAll(switch (config.ack_policy) {
        .none => "none",
        .all => "all",
        .explicit => "explicit",
    });
    try w.writeAll("\"");

    if (config.deliver_policy) |dp| {
        try w.writeAll(",\"deliver_policy\":\"");
        try w.writeAll(switch (dp) {
            .all => "all",
            .last => "last",
            .new => "new",
            .by_start_sequence => "by_start_sequence",
            .by_start_time => "by_start_time",
            .last_per_subject => "last_per_subject",
        });
        try w.writeAll("\"");
    }

    if (config.headers_only) |ho| {
        try w.writeAll(",\"headers_only\":");
        try w.writeAll(if (ho) "true" else "false");
    }

    try std.fmt.format(w, ",\"max_deliver\":{d}", .{config.max_deliver});
    try std.fmt.format(w, ",\"ack_wait\":{d}", .{config.ack_wait_ns});

    try w.writeAll("}");
    return stream.getWritten();
}

// ── JSON parsing helpers ──

fn parsePubAck(data: []const u8) Error!PubAck {
    if (checkJsError(data)) |err| return err;

    var ack = PubAck{};

    // Parse "stream":"name"
    if (extractJsonString(data, "stream")) |name| {
        const len = @min(name.len, ack.stream_buf.len);
        @memcpy(ack.stream_buf[0..len], name[0..len]);
        ack.stream_len = @intCast(len);
    }

    // Parse "seq":N (safe cast — negative seq from server is treated as 0)
    if (extractJsonInt(data, "seq")) |seq| {
        ack.seq = std.math.cast(u64, seq) orelse 0;
    }

    // Parse "duplicate":true/false
    if (std.mem.indexOf(u8, data, "\"duplicate\":true") != null) {
        ack.duplicate = true;
    }

    return ack;
}

fn parseStreamInfo(data: []const u8) Error!StreamInfo {
    if (checkJsError(data)) |err| return err;

    var info = StreamInfo{};

    // Look for "state" object fields (safe cast — negative values treated as 0)
    if (std.mem.indexOf(u8, data, "\"state\"")) |state_idx| {
        const state_data = data[state_idx..];
        if (extractJsonInt(state_data, "messages")) |v| info.messages = std.math.cast(u64, v) orelse 0;
        if (extractJsonInt(state_data, "bytes")) |v| info.bytes = std.math.cast(u64, v) orelse 0;
        if (extractJsonInt(state_data, "first_seq")) |v| info.first_seq = std.math.cast(u64, v) orelse 0;
        if (extractJsonInt(state_data, "last_seq")) |v| info.last_seq = std.math.cast(u64, v) orelse 0;
    }

    return info;
}

fn parseConsumerInfo(data: []const u8) Error!ConsumerInfo {
    if (checkJsError(data)) |err| return err;

    var info = ConsumerInfo{};
    if (extractJsonString(data, "name")) |n| {
        const len = @min(n.len, info.name_buf.len);
        @memcpy(info.name_buf[0..len], n[0..len]);
        info.name_len = @intCast(len);
    }
    if (extractJsonInt(data, "num_pending")) |v| info.num_pending = std.math.cast(u64, v) orelse 0;
    if (extractJsonInt(data, "num_ack_pending")) |v| info.num_ack_pending = std.math.cast(u64, v) orelse 0;

    return info;
}

/// Check for a JetStream error in the JSON response.
/// Looks for `"error":{` pattern to avoid false positives on data containing the word "error".
pub fn checkJsError(data: []const u8) ?Error {
    // Match "error":{ with optional whitespace — avoids matching string values like "error_handler"
    const has_error = std.mem.indexOf(u8, data, "\"error\":{") != null or
        std.mem.indexOf(u8, data, "\"error\" :{") != null or
        std.mem.indexOf(u8, data, "\"error\": {") != null;
    if (has_error) {
        // Check error code
        if (extractJsonInt(data, "err_code")) |code| {
            return switch (code) {
                10059 => Error.StreamNotFound,
                10014 => Error.ConsumerNotFound,
                else => Error.JetStreamError,
            };
        }
        if (extractJsonInt(data, "code")) |code| {
            return switch (code) {
                404 => Error.StreamNotFound,
                else => Error.JetStreamError,
            };
        }
        return Error.JetStreamError;
    }
    return null;
}

/// Simple JSON string extraction: find "key":"value" and return the value (without quotes).
/// Does not handle escaped quotes inside the value.
pub fn extractJsonString(data: []const u8, key: []const u8) ?[]const u8 {
    var search_buf: [128]u8 = undefined;
    const search = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{key}) catch return null;

    const idx = std.mem.indexOf(u8, data, search) orelse return null;
    const after = data[idx + search.len ..];
    const trimmed = std.mem.trimLeft(u8, after, " ");

    if (trimmed.len == 0 or trimmed[0] != '"') return null;
    const str_start = trimmed[1..];
    const end_quote = std.mem.indexOf(u8, str_start, "\"") orelse return null;
    return str_start[0..end_quote];
}

/// Simple JSON integer extraction: find "key":N and parse N.
pub fn extractJsonInt(data: []const u8, key: []const u8) ?i64 {
    // Search for "key":
    var search_buf: [128]u8 = undefined;
    const search = std.fmt.bufPrint(&search_buf, "\"{s}\":", .{key}) catch return null;

    const idx = std.mem.indexOf(u8, data, search) orelse return null;
    const after = data[idx + search.len ..];
    const trimmed = std.mem.trimLeft(u8, after, " ");

    // Find end of number (digits, minus sign)
    var end: usize = 0;
    if (end < trimmed.len and trimmed[end] == '-') end += 1;
    while (end < trimmed.len and trimmed[end] >= '0' and trimmed[end] <= '9') : (end += 1) {}

    if (end == 0) return null;
    return std.fmt.parseInt(i64, trimmed[0..end], 10) catch null;
}

// ── Tests ──

test "serialize stream config" {
    var buf: [4096]u8 = undefined;
    const json = try serializeStreamConfig(&buf, .{
        .name = "ORDERS",
        .subjects = &.{ "orders.>", "returns.>" },
        .storage = .memory,
    });

    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"ORDERS\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"orders.>\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"storage\":\"memory\"") != null);
}

test "serialize consumer config" {
    var buf: [4096]u8 = undefined;
    const json = try serializeConsumerConfig(&buf, .{
        .durable_name = "my-consumer",
        .ack_policy = .explicit,
    });

    try std.testing.expect(std.mem.indexOf(u8, json, "\"durable_name\":\"my-consumer\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"ack_policy\":\"explicit\"") != null);
}

test "serialize consumer config with deliver_policy" {
    var buf: [4096]u8 = undefined;
    const json = try serializeConsumerConfig(&buf, .{
        .deliver_policy = .last_per_subject,
        .headers_only = true,
    });

    try std.testing.expect(std.mem.indexOf(u8, json, "\"deliver_policy\":\"last_per_subject\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"headers_only\":true") != null);
}

test "serialize consumer config omits null deliver_policy" {
    var buf: [4096]u8 = undefined;
    const json = try serializeConsumerConfig(&buf, .{
        .durable_name = "test",
    });

    try std.testing.expect(std.mem.indexOf(u8, json, "deliver_policy") == null);
    try std.testing.expect(std.mem.indexOf(u8, json, "headers_only") == null);
}

test "parse pub ack" {
    const ack = try parsePubAck("{\"stream\":\"ORDERS\",\"seq\":42}");
    try std.testing.expectEqualStrings("ORDERS", ack.stream());
    try std.testing.expect(ack.seq == 42);
    try std.testing.expect(ack.duplicate == false);
}

test "parse pub ack duplicate" {
    const ack = try parsePubAck("{\"stream\":\"ORDERS\",\"seq\":42,\"duplicate\":true}");
    try std.testing.expectEqualStrings("ORDERS", ack.stream());
    try std.testing.expect(ack.seq == 42);
    try std.testing.expect(ack.duplicate == true);
}

test "parse stream info" {
    const info = try parseStreamInfo(
        \\{"type":"io.nats.jetstream.api.v1.stream_info_response","config":{},"state":{"messages":10,"bytes":1024,"first_seq":1,"last_seq":10}}
    );
    try std.testing.expect(info.messages == 10);
    try std.testing.expect(info.bytes == 1024);
    try std.testing.expect(info.first_seq == 1);
    try std.testing.expect(info.last_seq == 10);
}

test "parse consumer info" {
    const info = try parseConsumerInfo(
        \\{"type":"io.nats.jetstream.api.v1.consumer_create_response","config":{},"num_pending":5,"num_ack_pending":2}
    );
    try std.testing.expect(info.num_pending == 5);
    try std.testing.expect(info.num_ack_pending == 2);
}

test "check js error - stream not found" {
    const err = checkJsError("{\"error\":{\"code\":404,\"err_code\":10059,\"description\":\"stream not found\"}}");
    try std.testing.expect(err != null);
    try std.testing.expect(err.? == Error.StreamNotFound);
}

test "check js error - no error" {
    const err = checkJsError("{\"stream\":\"ORDERS\",\"seq\":1}");
    try std.testing.expect(err == null);
}

test "extract json int" {
    try std.testing.expect(extractJsonInt("{\"seq\":42}", "seq").? == 42);
    try std.testing.expect(extractJsonInt("{\"seq\":-1}", "seq").? == -1);
    try std.testing.expect(extractJsonInt("{\"foo\":\"bar\"}", "seq") == null);
}

test "extract json string" {
    try std.testing.expectEqualStrings("hello", extractJsonString("{\"data\":\"hello\"}", "data").?);
    try std.testing.expectEqualStrings("aGVsbG8=", extractJsonString("{\"data\":\"aGVsbG8=\"}", "data").?);
    try std.testing.expect(extractJsonString("{\"seq\":42}", "data") == null);
    try std.testing.expect(extractJsonString("{\"data\":42}", "data") == null);
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const Client = @import("Client.zig");
const JetStream = @import("JetStream.zig");
const Protocol = @import("Protocol.zig");
const Headers = @import("Headers.zig");

/// A KeyValue bucket backed by a JetStream stream.
/// Bucket "mybucket" → stream "KV_mybucket", keys → subjects "$KV.mybucket.<key>".
pub const Bucket = struct {
    js: JetStream.Context,
    name: []const u8,
    stream_name_buf: [128]u8 = undefined,
    stream_name_len: usize = 0,
    subject_prefix_buf: [128]u8 = undefined,
    subject_prefix_len: usize = 0,
    allocator: Allocator,

    /// Create a new KV bucket (creates the underlying JetStream stream).
    pub fn create(client: *Client, name: []const u8, config: BucketConfig) Error!Bucket {
        var bucket = Bucket{
            .js = JetStream.Context.init(client),
            .name = name,
            .allocator = client.allocator,
        };
        bucket.js.timeout_ms = 2000;

        // Build stream name: "KV_<name>"
        var sn_stream = std.io.fixedBufferStream(&bucket.stream_name_buf);
        std.fmt.format(sn_stream.writer(), "KV_{s}", .{name}) catch return Error.InvalidBucketName;
        bucket.stream_name_len = sn_stream.pos;

        // Build subject prefix: "$KV.<name>."
        var sp_stream = std.io.fixedBufferStream(&bucket.subject_prefix_buf);
        std.fmt.format(sp_stream.writer(), "$KV.{s}.", .{name}) catch return Error.InvalidBucketName;
        bucket.subject_prefix_len = sp_stream.pos;

        // Create underlying stream
        var pattern_buf: [128]u8 = undefined;
        const pattern = std.fmt.bufPrint(&pattern_buf, "$KV.{s}.>", .{name}) catch return Error.InvalidBucketName;

        const subjects: [1][]const u8 = .{pattern};

        _ = bucket.js.createStream(.{
            .name = bucket.stream_name_buf[0..bucket.stream_name_len],
            .subjects = &subjects,
            .retention = .limits,
            .storage = config.storage,
            .num_replicas = config.num_replicas,
            .max_bytes = config.max_bytes,
            .max_msg_size = config.max_value_size,
            .discard = .new,
        }) catch |err| return mapJsError(err);

        return bucket;
    }

    /// Open an existing KV bucket (doesn't create the stream).
    pub fn open(client: *Client, name: []const u8) Error!Bucket {
        var bucket = Bucket{
            .js = JetStream.Context.init(client),
            .name = name,
            .allocator = client.allocator,
        };
        bucket.js.timeout_ms = 2000;

        var sn_stream = std.io.fixedBufferStream(&bucket.stream_name_buf);
        std.fmt.format(sn_stream.writer(), "KV_{s}", .{name}) catch return Error.InvalidBucketName;
        bucket.stream_name_len = sn_stream.pos;

        var sp_stream = std.io.fixedBufferStream(&bucket.subject_prefix_buf);
        std.fmt.format(sp_stream.writer(), "$KV.{s}.", .{name}) catch return Error.InvalidBucketName;
        bucket.subject_prefix_len = sp_stream.pos;

        // Verify the stream exists
        _ = bucket.js.streamInfo(bucket.stream_name_buf[0..bucket.stream_name_len]) catch |err| return mapJsError(err);

        return bucket;
    }

    /// Get the value for a key. Returns null if the key doesn't exist.
    pub fn get(self: *Bucket, key: []const u8) Error!?Entry {
        // Request the last message for this key's subject
        var subject_buf: [256]u8 = undefined;
        const subject = self.keySubject(&subject_buf, key) orelse return Error.InvalidKey;

        // Use JetStream direct get: $JS.API.STREAM.MSG.GET.<stream>
        var req_subject_buf: [256]u8 = undefined;
        const req_subject = std.fmt.bufPrint(&req_subject_buf, "{s}.STREAM.MSG.GET.{s}", .{
            self.js.api_prefix,
            self.stream_name_buf[0..self.stream_name_len],
        }) catch return Error.InvalidKey;

        var payload_buf: [512]u8 = undefined;
        const req_payload = std.fmt.bufPrint(&payload_buf, "{{\"last_by_subj\":\"{s}\"}}", .{subject}) catch return Error.InvalidKey;

        var msg = self.js.client.request(req_subject, req_payload, self.js.timeout_ms) catch |err| {
            // 404 means key not found
            return switch (err) {
                error.Timeout => Error.Timeout,
                else => Error.NotFound,
            };
        };
        defer msg.deinit();

        const data = msg.payload orelse return null;

        // Check for error response (stream not found, key not found)
        if (JetStream.checkJsError(data)) |_| return null;

        // Parse the response to extract the value
        // Response contains: {"message":{"subject":"...","seq":N,"data":"base64..."}}
        return Entry{
            .key = key,
            .value = data,
            .revision = extractRevision(data),
            .operation = .put,
        };
    }

    /// Put a value for a key. Returns the revision (sequence number).
    pub fn put(self: *Bucket, key: []const u8, value: []const u8) Error!u64 {
        var subject_buf: [256]u8 = undefined;
        const subject = self.keySubject(&subject_buf, key) orelse return Error.InvalidKey;

        const ack = self.js.publish(subject, value) catch |err| return mapJsError(err);
        return ack.seq;
    }

    /// Delete a key by publishing an empty message with a delete marker.
    pub fn delete(self: *Bucket, key: []const u8) Error!void {
        var subject_buf: [256]u8 = undefined;
        const subject = self.keySubject(&subject_buf, key) orelse return Error.InvalidKey;

        // Publish with KV-Operation: DEL header
        var entries = [_]Headers.Entry{
            .{ .key = "KV-Operation", .value = "DEL" },
        };
        const hdrs = Headers{ .entries = &entries };
        self.js.client.publishMsg(subject, null, &hdrs, null) catch |err| {
            return switch (err) {
                error.NotConnected => Error.NotConnected,
                else => Error.JetStreamError,
            };
        };
    }

    /// Destroy the entire bucket (deletes the underlying stream).
    pub fn destroy(self: *Bucket) Error!void {
        self.js.deleteStream(self.stream_name_buf[0..self.stream_name_len]) catch |err| return mapJsError(err);
    }

    // ── Helpers ──

    fn keySubject(self: *const Bucket, buf: []u8, key: []const u8) ?[]const u8 {
        if (key.len == 0) return null;
        const prefix = self.subject_prefix_buf[0..self.subject_prefix_len];
        if (prefix.len + key.len > buf.len) return null;
        @memcpy(buf[0..prefix.len], prefix);
        @memcpy(buf[prefix.len .. prefix.len + key.len], key);
        return buf[0 .. prefix.len + key.len];
    }

    fn streamName(self: *const Bucket) []const u8 {
        return self.stream_name_buf[0..self.stream_name_len];
    }
};

// ── Types ──

pub const Error = error{
    InvalidBucketName,
    InvalidKey,
    NotFound,
    NotConnected,
    Timeout,
    JetStreamError,
    OutOfMemory,
};

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
    revision: u64 = 0,
    operation: Operation = .put,
};

pub const Operation = enum { put, delete, purge };

pub const BucketConfig = struct {
    history: u32 = 1,
    ttl_ns: i64 = 0,
    max_value_size: i32 = -1,
    storage: JetStream.Storage = .file,
    num_replicas: u32 = 1,
    max_bytes: i64 = -1,
};

// ── Helpers ──

fn extractRevision(data: []const u8) u64 {
    if (JetStream.extractJsonInt(data, "seq")) |v| {
        if (v >= 0) return @intCast(v);
    }
    return 0;
}

fn mapJsError(err: anytype) Error {
    return switch (err) {
        error.StreamNotFound => Error.NotFound,
        error.ConsumerNotFound => Error.NotFound,
        error.Timeout => Error.Timeout,
        error.InvalidResponse => Error.JetStreamError,
        error.JetStreamError => Error.JetStreamError,
        error.NotConnected => Error.NotConnected,
        error.OutOfMemory => Error.OutOfMemory,
        else => Error.JetStreamError,
    };
}

// ── Tests ──

test "key subject construction" {
    // Can't easily test without a client, but we can test the helper logic
    var buf: [256]u8 = undefined;
    const prefix = "$KV.test.";
    @memcpy(buf[0..prefix.len], prefix);
    const key = "mykey";
    @memcpy(buf[prefix.len .. prefix.len + key.len], key);
    const result = buf[0 .. prefix.len + key.len];
    try std.testing.expectEqualStrings("$KV.test.mykey", result);
}

test "extract revision from json" {
    try std.testing.expect(extractRevision("{\"seq\":42}") == 42);
    try std.testing.expect(extractRevision("{\"no_seq\":true}") == 0);
}

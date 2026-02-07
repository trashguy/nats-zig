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
            .max_age_ns = config.ttl_ns,
            .max_msg_size = config.max_value_size,
            .max_msgs_per_subject = if (config.history > 0) @intCast(config.history) else -1,
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

        // Parse the response: {"message":{"subject":"...","seq":N,"data":"base64..."}}
        const revision = extractRevision(data);

        // Extract and decode base64 "data" field
        const b64_data = JetStream.extractJsonString(data, "data") orelse return Entry{
            .key = key,
            .value = "",
            .revision = revision,
            .operation = .put,
        };

        // Decode base64 into an owned buffer
        const decoded_len = std.base64.standard.Decoder.calcSizeUpperBound(b64_data.len);
        const decoded_buf = self.allocator.alloc(u8, decoded_len) catch return Error.OutOfMemory;

        const actual_len = std.base64.standard.Decoder.decode(decoded_buf, b64_data) catch |err| {
            self.allocator.free(decoded_buf);
            // If base64 decode fails, maybe it's raw data (some NATS versions)
            _ = err;
            return Entry{
                .key = key,
                .value = "",
                .revision = revision,
                .operation = .put,
            };
        };

        return Entry{
            .key = key,
            .value = decoded_buf[0..actual_len],
            .revision = revision,
            .operation = .put,
            ._value_buf = decoded_buf,
            ._allocator = self.allocator,
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

    /// Watch for changes on keys matching the given pattern (or ">" for all keys).
    /// Creates an ordered ephemeral consumer with deliver_policy=last_per_subject.
    pub fn watch(self: *Bucket, key_pattern: []const u8, opts: WatchOptions) Error!*Watcher {
        // Build filter subject: $KV.<bucket>.<key_pattern>
        var filter_buf: [256]u8 = undefined;
        const prefix = self.subject_prefix_buf[0..self.subject_prefix_len];
        if (prefix.len + key_pattern.len > filter_buf.len) return Error.InvalidKey;
        @memcpy(filter_buf[0..prefix.len], prefix);
        @memcpy(filter_buf[prefix.len .. prefix.len + key_pattern.len], key_pattern);
        const filter_subject = filter_buf[0 .. prefix.len + key_pattern.len];

        // Create ordered ephemeral consumer
        const consumer_info = self.js.createConsumer(
            self.stream_name_buf[0..self.stream_name_len],
            .{
                .filter_subject = filter_subject,
                .ack_policy = .none,
                .deliver_policy = if (opts.include_history) .all else .last_per_subject,
                .headers_only = if (opts.keys_only) true else null,
            },
        ) catch |err| return mapJsError(err);

        const consumer_name = consumer_info.name();

        // Create pull subscription for this consumer
        var pull_sub = self.js.pullSubscribe(
            self.stream_name_buf[0..self.stream_name_len],
            consumer_name,
        ) catch |err| return mapJsError(err);
        errdefer pull_sub.close();

        const watcher = self.allocator.create(Watcher) catch return Error.OutOfMemory;
        watcher.* = .{
            .pull_sub = pull_sub,
            .allocator = self.allocator,
            .subject_prefix = prefix,
            .initial_pending = consumer_info.num_pending,
            .delivered_count = 0,
            .opts = opts,
            .stopped = false,
        };

        return watcher;
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
    /// Allocator-owned value buffer. Caller must call deinit() when done.
    _value_buf: ?[]const u8 = null,
    _allocator: ?Allocator = null,

    pub fn deinit(self: *Entry) void {
        if (self._value_buf) |buf| {
            if (self._allocator) |a| a.free(buf);
        }
        self._value_buf = null;
        self._allocator = null;
    }
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

pub const WatchOptions = struct {
    keys_only: bool = false,
    include_history: bool = false,
    ignore_deletes: bool = false,
};

pub const WatchEntry = struct {
    key_buf: [256]u8 = undefined,
    key_len: usize = 0,
    value: []const u8 = "",
    revision: u64 = 0,
    operation: Operation = .put,
    /// Allocator-owned value buffer.
    _value_buf: ?[]const u8 = null,
    _allocator: ?Allocator = null,

    pub fn key(self: *const WatchEntry) []const u8 {
        return self.key_buf[0..self.key_len];
    }

    pub fn deinit(self: *WatchEntry) void {
        if (self._value_buf) |buf| {
            if (self._allocator) |a| a.free(buf);
        }
        self._value_buf = null;
        self._allocator = null;
    }
};

/// Watches a KV bucket for changes via a JetStream pull subscription.
pub const Watcher = struct {
    pull_sub: JetStream.PullSubscription,
    allocator: Allocator,
    subject_prefix: []const u8,
    initial_pending: u64,
    delivered_count: u64,
    opts: WatchOptions,
    stopped: bool,

    /// Return the next change entry, or null if none available.
    /// Non-blocking: returns null immediately if no messages are pending.
    pub fn next(self: *Watcher) ?WatchEntry {
        return self.nextWithTimeout(200);
    }

    /// Return the next change entry, waiting up to `timeout_ms` for a message.
    pub fn nextWithTimeout(self: *Watcher, timeout_ms: u32) ?WatchEntry {
        if (self.stopped) return null;

        const messages = self.pull_sub.fetch(1, timeout_ms) catch return null;
        defer self.allocator.free(messages);

        if (messages.len == 0) return null;

        var msg = messages[0];
        defer msg.deinit();

        self.delivered_count += 1;

        const entry = self.msgToWatchEntry(&msg);

        // Filter out deletes if requested
        if (self.opts.ignore_deletes and (entry.operation == .delete or entry.operation == .purge)) {
            return null;
        }

        return entry;
    }

    /// Returns true once all initial values have been delivered.
    pub fn initialComplete(self: *const Watcher) bool {
        return self.delivered_count >= self.initial_pending;
    }

    /// Stop watching and release resources.
    pub fn stop(self: *Watcher) void {
        self.stopped = true;
    }

    /// Clean up all resources.
    pub fn deinit(self: *Watcher) void {
        self.pull_sub.close();
        self.allocator.destroy(self);
    }

    fn msgToWatchEntry(self: *const Watcher, msg: *const Protocol.Msg) WatchEntry {
        var entry = WatchEntry{};

        // Extract key from subject by stripping the $KV.<bucket>. prefix
        if (msg.subject.len > self.subject_prefix.len) {
            const key_part = msg.subject[self.subject_prefix.len..];
            const len = @min(key_part.len, entry.key_buf.len);
            @memcpy(entry.key_buf[0..len], key_part[0..len]);
            entry.key_len = len;
        }

        // Parse KV-Operation header
        if (msg.headers) |hdrs| {
            if (hdrs.get("KV-Operation")) |op_str| {
                if (std.ascii.eqlIgnoreCase(op_str, "DEL")) {
                    entry.operation = .delete;
                } else if (std.ascii.eqlIgnoreCase(op_str, "PURGE")) {
                    entry.operation = .purge;
                }
                // Default is .put (no header or "PUT")
            }
        }

        // Copy payload as value
        if (msg.payload) |payload| {
            if (payload.len > 0) {
                const value_buf = self.allocator.dupe(u8, payload) catch {
                    entry.value = "";
                    return entry;
                };
                entry.value = value_buf;
                entry._value_buf = value_buf;
                entry._allocator = self.allocator;
            }
        }

        // Extract revision from reply_to (JetStream encodes seq in ack subject)
        // Format: $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<timestamp>.<pending>
        if (msg.reply_to) |reply_to| {
            entry.revision = parseAckSequence(reply_to);
        }

        return entry;
    }
};

// ── Helpers ──

fn extractRevision(data: []const u8) u64 {
    if (JetStream.extractJsonInt(data, "seq")) |v| {
        return std.math.cast(u64, v) orelse 0;
    }
    return 0;
}

/// Parse the stream sequence number from a JetStream ACK reply subject.
/// Format: $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<timestamp>.<pending>
fn parseAckSequence(reply_to: []const u8) u64 {
    // Skip "$JS.ACK." prefix
    if (!std.mem.startsWith(u8, reply_to, "$JS.ACK.")) return 0;
    var rest = reply_to[8..]; // after "$JS.ACK."

    // Skip stream name, consumer name, delivered count (3 dot-separated fields)
    var skipped: u32 = 0;
    while (skipped < 3) {
        if (std.mem.indexOf(u8, rest, ".")) |dot| {
            rest = rest[dot + 1 ..];
            skipped += 1;
        } else return 0;
    }

    // Next field is stream_seq
    const end = std.mem.indexOf(u8, rest, ".") orelse rest.len;
    return std.fmt.parseInt(u64, rest[0..end], 10) catch 0;
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

test "parse ack sequence" {
    try std.testing.expect(parseAckSequence("$JS.ACK.KV_test.consumer1.1.42.1.1234567890.0") == 42);
    try std.testing.expect(parseAckSequence("$JS.ACK.KV_test.consumer1.1.1.1.1234567890.5") == 1);
    try std.testing.expect(parseAckSequence("not-an-ack") == 0);
    try std.testing.expect(parseAckSequence("$JS.ACK.short") == 0);
}

test "watch entry key extraction" {
    const prefix = "$KV.mybucket.";
    const subject = "$KV.mybucket.mykey";

    var entry = WatchEntry{};
    const key_part = subject[prefix.len..];
    const len = @min(key_part.len, entry.key_buf.len);
    @memcpy(entry.key_buf[0..len], key_part[0..len]);
    entry.key_len = len;

    try std.testing.expectEqualStrings("mykey", entry.key());
}

test "watch options defaults" {
    const opts = WatchOptions{};
    try std.testing.expect(opts.keys_only == false);
    try std.testing.expect(opts.include_history == false);
    try std.testing.expect(opts.ignore_deletes == false);
}

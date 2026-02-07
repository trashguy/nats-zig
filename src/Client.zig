const std = @import("std");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");
const Connection = @import("Connection.zig");
const SubscriptionManager = @import("Subscription.zig");
const Headers = @import("Headers.zig");
pub const JetStream = @import("JetStream.zig");

const Client = @This();

pub const ConnectOptions = struct {
    servers: []const []const u8 = &.{"nats://127.0.0.1:4222"},
    name: ?[]const u8 = null,
    token: ?[]const u8 = null,
    user: ?[]const u8 = null,
    pass: ?[]const u8 = null,
    verbose: bool = false,
    pedantic: bool = false,
    echo: bool = true,
    no_responders: bool = true,
    ping_interval_ms: u32 = 30_000,
    max_outstanding_pings: u32 = 2,
    max_reconnect_attempts: u32 = 60,
    reconnect_wait_ms: u32 = 2_000,
    max_payload: u32 = 1_048_576,
    tls_required: bool = false,
    tls_verify: bool = true,
    allow_reconnect: bool = true,
};

pub const Error = error{
    NotConnected,
    Timeout,
    NoResponders,
    ServerError,
    MaxPayloadExceeded,
    InvalidSubject,
    OutOfMemory,
    ConnectionFailed,
    ReconnectFailed,
} || Connection.ConnectError || Connection.ReadError || Connection.WriteError || Protocol.ParseError;

pub const Subscription = SubscriptionManager.Subscription;
pub const SubscribeOpts = SubscriptionManager.SubscribeOpts;
pub const Msg = Protocol.Msg;

allocator: Allocator,
conn: Connection,
parser: Protocol.Parser,
sub_mgr: SubscriptionManager,
server_info: ?Protocol.ServerInfo = null,
opts: ConnectOptions,
status: Status = .disconnected,

// Reconnection state
reconnect_attempts: u32 = 0,
last_ping_time: i128 = 0,
outstanding_pings: u32 = 0,
known_servers: std.ArrayListUnmanaged([]const u8) = .{},

pub const Status = enum {
    disconnected,
    connected,
    reconnecting,
    draining,
};

/// Connect to a NATS server.
pub fn connect(allocator: Allocator, opts: ConnectOptions) Error!*Client {
    const self = try allocator.create(Client);
    self.* = .{
        .allocator = allocator,
        .conn = Connection.init(),
        .parser = Protocol.Parser.init(allocator),
        .sub_mgr = SubscriptionManager.init(allocator),
        .opts = opts,
    };
    errdefer {
        self.parser.deinit();
        self.sub_mgr.deinit();
        allocator.destroy(self);
    }

    try self.doConnect();
    self.last_ping_time = std.time.nanoTimestamp();
    return self;
}

/// Close the connection and free resources.
pub fn close(self: *Client) void {
    self.conn.deinit();
    self.parser.deinit();
    self.sub_mgr.deinit();
    // Free known servers
    for (self.known_servers.items) |url| {
        self.allocator.free(url);
    }
    self.known_servers.deinit(self.allocator);
    // Free ServerInfo memory
    if (self.server_info) |*si| si.deinit();
    self.status = .disconnected;
    self.allocator.destroy(self);
}

/// Validate a NATS subject for protocol safety (no CRLF, spaces, or null bytes).
fn validateSubject(subject: []const u8) Error!void {
    if (subject.len == 0) return Error.InvalidSubject;
    for (subject) |c| {
        if (c == '\r' or c == '\n' or c == ' ' or c == '\t' or c == 0) return Error.InvalidSubject;
    }
}

/// Publish a message to a subject.
pub fn publish(self: *Client, subject: []const u8, payload: ?[]const u8) Error!void {
    try validateSubject(subject);
    try self.publishInner(subject, null, payload);
}

/// Publish a message with a reply-to subject.
pub fn publishTo(self: *Client, subject: []const u8, reply_to: []const u8, payload: ?[]const u8) Error!void {
    try validateSubject(subject);
    try self.publishInner(subject, reply_to, payload);
}

/// Publish a message with headers.
pub fn publishMsg(self: *Client, subject: []const u8, reply_to: ?[]const u8, hdrs: ?*const Headers, payload: ?[]const u8) Error!void {
    if (self.status != .connected) return Error.NotConnected;
    try validateSubject(subject);

    if (hdrs) |h| {
        // Serialize headers (small — 4KB is plenty for NATS headers)
        var hdr_buf: [4096]u8 = undefined;
        const hdr_bytes = h.serialize(&hdr_buf) catch return Error.MaxPayloadExceeded;
        const hdr_len = hdr_bytes.len;
        const payload_len = if (payload) |p| p.len else 0;
        const total_len = hdr_len + payload_len;

        const max_payload = if (self.server_info) |si| si.max_payload else self.opts.max_payload;
        if (total_len > max_payload) return Error.MaxPayloadExceeded;

        // Format just the HPUB header line
        var line_buf: [1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(&line_buf);
        const w = stream.writer();
        if (reply_to) |rt| {
            std.fmt.format(w, "HPUB {s} {s} {d} {d}\r\n", .{ subject, rt, hdr_len, total_len }) catch return Error.MaxPayloadExceeded;
        } else {
            std.fmt.format(w, "HPUB {s} {d} {d}\r\n", .{ subject, hdr_len, total_len }) catch return Error.MaxPayloadExceeded;
        }

        try self.writeOrReconnect(stream.getWritten());
        try self.writeOrReconnect(hdr_bytes);
        if (payload) |p| try self.writeOrReconnect(p);
        try self.writeOrReconnect("\r\n");
    } else {
        // No headers — delegate to publishInner
        const payload_len = if (payload) |p| p.len else 0;
        const max_payload = if (self.server_info) |si| si.max_payload else self.opts.max_payload;
        if (payload_len > max_payload) return Error.MaxPayloadExceeded;

        var line_buf: [1024]u8 = undefined;
        var stream = std.io.fixedBufferStream(&line_buf);
        const w = stream.writer();
        if (reply_to) |rt| {
            std.fmt.format(w, "PUB {s} {s} {d}\r\n", .{ subject, rt, payload_len }) catch return Error.MaxPayloadExceeded;
        } else {
            std.fmt.format(w, "PUB {s} {d}\r\n", .{ subject, payload_len }) catch return Error.MaxPayloadExceeded;
        }

        try self.writeOrReconnect(stream.getWritten());
        if (payload) |p| try self.writeOrReconnect(p);
        try self.writeOrReconnect("\r\n");
    }
    self.conn.flush() catch return Error.NotConnected;
}

/// Subscribe to a subject, returning a Subscription handle.
pub fn subscribe(self: *Client, subject: []const u8, opts: SubscribeOpts) Error!*Subscription {
    if (self.status != .connected) return Error.NotConnected;
    try validateSubject(subject);

    const sub = try self.sub_mgr.addSubscription(subject, opts);
    errdefer self.sub_mgr.removeSubscription(sub.sid);

    // Send SUB to server
    var buf: [1024]u8 = undefined;
    const formatted = Protocol.fmtSub(&buf, subject, opts.queue_group, sub.sid) catch return Error.MaxPayloadExceeded;
    self.conn.write(formatted) catch return Error.NotConnected;
    self.conn.flush() catch return Error.NotConnected;

    return sub;
}

/// Unsubscribe from a subscription.
pub fn unsubscribe(self: *Client, sub: *Subscription) Error!void {
    var buf: [256]u8 = undefined;
    const formatted = Protocol.fmtUnsub(&buf, sub.sid, null) catch return Error.MaxPayloadExceeded;
    self.conn.write(formatted) catch return Error.NotConnected;
    self.conn.flush() catch return Error.NotConnected;
    self.sub_mgr.removeSubscription(sub.sid);
}

/// Request/Reply: publish to subject and wait for a single reply.
pub fn request(self: *Client, subject: []const u8, payload: ?[]const u8, timeout_ms: u32) Error!Msg {
    if (self.status != .connected) return Error.NotConnected;

    // Create unique inbox
    const inbox_arr = self.sub_mgr.newInbox() catch return Error.OutOfMemory;
    const inbox = inbox_arr[0..39];

    // Subscribe to inbox with auto-unsub after 1 message
    const sub = try self.sub_mgr.addSubscription(inbox, .{});
    self.sub_mgr.autoUnsubscribe(sub.sid, 1);
    const sid = sub.sid;

    // Send SUB + UNSUB(1)
    {
        var buf: [1024]u8 = undefined;
        const sub_cmd = Protocol.fmtSub(&buf, inbox, null, sid) catch return Error.MaxPayloadExceeded;
        self.conn.write(sub_cmd) catch return Error.NotConnected;
    }
    {
        var buf: [256]u8 = undefined;
        const unsub_cmd = Protocol.fmtUnsub(&buf, sid, 1) catch return Error.MaxPayloadExceeded;
        self.conn.write(unsub_cmd) catch return Error.NotConnected;
    }

    // Publish with reply-to set to inbox
    try self.publishInner(subject, inbox, payload);

    // Wait for reply with timeout
    self.conn.setReadTimeout(100);
    defer self.conn.setReadTimeout(0);

    const deadline_ns: i128 = std.time.nanoTimestamp() + @as(i128, timeout_ms) * std.time.ns_per_ms;

    while (std.time.nanoTimestamp() < deadline_ns) {
        // Check if we already have a reply
        if (self.sub_mgr.getSub(sid)) |s| {
            if (s.nextMsg()) |msg| {
                self.sub_mgr.removeSubscription(sid);
                return msg;
            }
        } else {
            return Error.Timeout;
        }

        // Try to read more data from the server
        _ = self.processOneOp() catch |err| switch (err) {
            error.WouldBlock => continue,
            error.ConnectionClosed => return Error.NotConnected,
            else => return Error.NotConnected,
        };
    }

    // Timed out — clean up
    self.sub_mgr.removeSubscription(sid);
    return Error.Timeout;
}

/// Process one incoming server operation (non-blocking if no data buffered).
/// Returns the operation if one was processed, null if no complete message available.
pub fn poll(self: *Client) Error!?Protocol.ServerOp {
    if (self.status != .connected) return Error.NotConnected;

    // Only try to read if we have buffered data or the socket has data
    if (!self.conn.hasPendingData()) return null;

    return self.processOneOp() catch |err| switch (err) {
        error.ConnectionClosed => {
            self.handleConnectionLoss();
            return Error.NotConnected;
        },
        else => return Error.NotConnected,
    };
}

/// Process all pending incoming data with a short timeout.
/// Will read from socket with a 100ms timeout to avoid blocking forever.
pub fn processIncoming(self: *Client) !void {
    // Set a short read timeout so we don't block forever
    self.conn.setReadTimeout(100);
    defer self.conn.setReadTimeout(0);

    while (true) {
        const op = self.processOneOp() catch |err| switch (err) {
            error.ConnectionClosed => {
                self.handleConnectionLoss();
                return;
            },
            error.WouldBlock => return,
            else => return,
        };
        if (op == null) return;
    }
}

/// Check if a ping should be sent based on the configured interval.
/// Call this periodically (e.g., from your event loop).
pub fn maybeSendPing(self: *Client) Error!void {
    if (self.status != .connected) return;
    if (self.opts.ping_interval_ms == 0) return;

    const now = std.time.nanoTimestamp();
    const diff = @max(now - self.last_ping_time, 0);
    const elapsed_ms: u64 = @intCast(@divFloor(diff, std.time.ns_per_ms));

    if (elapsed_ms >= self.opts.ping_interval_ms) {
        if (self.outstanding_pings >= self.opts.max_outstanding_pings) {
            // Too many outstanding pings — server is unresponsive
            self.handleConnectionLoss();
            return;
        }

        var buf: [8]u8 = undefined;
        const ping_cmd = Protocol.fmtPing(&buf) catch unreachable;
        self.conn.write(ping_cmd) catch {
            self.handleConnectionLoss();
            return;
        };
        self.conn.flush() catch {
            self.handleConnectionLoss();
            return;
        };

        self.outstanding_pings += 1;
        self.last_ping_time = now;
    }
}

/// Create a JetStream context for this client.
pub fn jetStream(self: *Client) JetStream.Context {
    return JetStream.Context.init(self);
}

// ── Internal ──

fn doConnect(self: *Client) Error!void {
    // Build server list: configured servers + discovered servers
    const configured = self.opts.servers;
    const discovered = self.known_servers.items;
    const total = configured.len + discovered.len;

    // Try each server
    for (0..total) |i| {
        const server_url = if (i < configured.len)
            configured[i]
        else
            discovered[i - configured.len];

        const url = Connection.parseUrl(server_url) catch continue;

        self.conn.connect(url.host, url.port) catch continue;

        // Read INFO
        const info_line = self.conn.readLine() catch {
            self.conn.disconnect();
            continue;
        };
        const info_op = self.parser.parse(info_line) catch {
            self.conn.disconnect();
            continue;
        };
        if (info_op) |op| {
            switch (op) {
                .info => |info| {
                    if (self.server_info) |*old| old.deinit();
                    self.server_info = info;
                    // Merge connect_urls
                    self.mergeConnectUrls(info);
                },
                else => {
                    self.conn.disconnect();
                    continue;
                },
            }
        } else {
            self.conn.disconnect();
            continue;
        }

        // Check if TLS upgrade is required
        const need_tls = url.tls or self.opts.tls_required or
            (if (self.server_info) |si| si.tls_required else false);

        if (need_tls) {
            self.conn.upgradeTls(self.allocator, url.host, self.opts.tls_verify) catch {
                self.conn.disconnect();
                continue;
            };
        }

        // Send CONNECT + PING
        var connect_buf: [4096]u8 = undefined;
        const connect_cmd = Protocol.fmtConnect(&connect_buf, .{
            .verbose = self.opts.verbose,
            .pedantic = self.opts.pedantic,
            .echo = self.opts.echo,
            .no_responders = self.opts.no_responders,
            .name = self.opts.name,
            .token = self.opts.token orelse if (url.user != null and url.pass == null) url.user else null,
            .user = if (url.pass != null) url.user orelse self.opts.user else self.opts.user,
            .pass = if (url.pass != null) url.pass orelse self.opts.pass else self.opts.pass,
        }) catch {
            self.conn.disconnect();
            continue;
        };
        self.conn.write(connect_cmd) catch {
            self.conn.disconnect();
            continue;
        };

        var ping_buf: [8]u8 = undefined;
        const ping_cmd = Protocol.fmtPing(&ping_buf) catch unreachable;
        self.conn.write(ping_cmd) catch {
            self.conn.disconnect();
            continue;
        };
        self.conn.flush() catch {
            self.conn.disconnect();
            continue;
        };

        // Wait for PONG (or +OK then PONG if verbose)
        var got_pong = false;
        for (0..10) |_| {
            const reply_line = self.conn.readLine() catch break;
            const reply_op = self.parser.parse(reply_line) catch break;
            if (reply_op) |rop| {
                switch (rop) {
                    .pong => {
                        got_pong = true;
                        break;
                    },
                    .ok => continue, // verbose mode
                    .err => {
                        self.conn.disconnect();
                        break;
                    },
                    else => continue,
                }
            }
        }

        if (got_pong) {
            self.status = .connected;
            return;
        }

        self.conn.disconnect();
    }

    return Error.ConnectionFailed;
}

fn publishInner(self: *Client, subject: []const u8, reply_to: ?[]const u8, payload: ?[]const u8) Error!void {
    if (self.status != .connected) return Error.NotConnected;

    // Enforce max_payload from server INFO or client config
    const payload_len = if (payload) |p| p.len else 0;
    const max_payload = if (self.server_info) |si| si.max_payload else self.opts.max_payload;
    if (payload_len > max_payload) return Error.MaxPayloadExceeded;

    // Format just the header line — subject + numbers, always small
    var hdr_buf: [1024]u8 = undefined;
    var stream = std.io.fixedBufferStream(&hdr_buf);
    const w = stream.writer();
    if (reply_to) |rt| {
        std.fmt.format(w, "PUB {s} {s} {d}\r\n", .{ subject, rt, payload_len }) catch return Error.MaxPayloadExceeded;
    } else {
        std.fmt.format(w, "PUB {s} {d}\r\n", .{ subject, payload_len }) catch return Error.MaxPayloadExceeded;
    }

    try self.writeOrReconnect(stream.getWritten());
    if (payload) |p| try self.writeOrReconnect(p);
    try self.writeOrReconnect("\r\n");
    self.conn.flush() catch return Error.NotConnected;
}

fn processOneOp(self: *Client) !?Protocol.ServerOp {
    switch (self.parser.state) {
        .awaiting_op => {
            const line = try self.conn.readLine();
            const op = try self.parser.parse(line);

            if (op) |server_op| {
                try self.handleOp(server_op);
                return server_op;
            }
            // Parser needs payload — will be in awaiting_msg_payload or awaiting_hmsg_headers_and_payload
            return null;
        },
        .awaiting_msg_payload, .awaiting_hmsg_headers_and_payload => {
            const needed = self.parser.pendingBytes();
            const result = try self.conn.readExactAlloc(self.allocator, needed);
            defer result.free(self.allocator);
            const op = try self.parser.parse(result.data);

            if (op) |server_op| {
                try self.handleOp(server_op);
                return server_op;
            }
            return null;
        },
    }
}

fn handleOp(self: *Client, op: Protocol.ServerOp) !void {
    switch (op) {
        .ping => {
            // Auto-reply with PONG
            var buf: [8]u8 = undefined;
            const pong = Protocol.fmtPong(&buf) catch unreachable;
            self.conn.write(pong) catch return;
            self.conn.flush() catch return;
        },
        .pong => {
            // Reset outstanding ping counter
            if (self.outstanding_pings > 0) {
                self.outstanding_pings -= 1;
            }
        },
        .msg => |msg| {
            // Dispatch to subscription manager
            try self.sub_mgr.dispatch(msg);
        },
        .info => |info| {
            if (self.server_info) |*old| old.deinit();
            self.server_info = info;
            self.mergeConnectUrls(info);
        },
        .ok, .err => {
            // Handled by caller or ignored
        },
    }
}

fn mergeConnectUrls(self: *Client, info: Protocol.ServerInfo) void {
    if (info.connect_urls) |urls| {
        for (urls) |url| {
            // Check for duplicates
            var found = false;
            for (self.known_servers.items) |existing| {
                if (std.mem.eql(u8, existing, url)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                // Also check against configured servers
                for (self.opts.servers) |configured| {
                    // Simple comparison — strip nats:// prefix for comparison
                    var config_host = configured;
                    if (std.mem.startsWith(u8, config_host, "nats://")) {
                        config_host = config_host[7..];
                    } else if (std.mem.startsWith(u8, config_host, "tls://")) {
                        config_host = config_host[6..];
                    }
                    if (std.mem.eql(u8, config_host, url)) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                const duped = self.allocator.dupe(u8, url) catch continue;
                self.known_servers.append(self.allocator, duped) catch {
                    self.allocator.free(duped);
                };
            }
        }
    }
}

fn handleConnectionLoss(self: *Client) void {
    if (!self.opts.allow_reconnect) {
        self.status = .disconnected;
        return;
    }
    self.tryReconnect() catch {
        self.status = .disconnected;
    };
}

fn writeOrReconnect(self: *Client, data: []const u8) Error!void {
    self.conn.write(data) catch {
        try self.handleWriteFailure();
        self.conn.write(data) catch return Error.NotConnected;
    };
}

fn handleWriteFailure(self: *Client) Error!void {
    if (!self.opts.allow_reconnect) return Error.NotConnected;
    self.tryReconnect() catch return Error.NotConnected;
}

fn tryReconnect(self: *Client) Error!void {
    self.status = .reconnecting;
    self.conn.disconnect();

    // Reset parser state
    self.parser.state = .awaiting_op;

    var attempt: u32 = 0;
    const max_attempts = self.opts.max_reconnect_attempts;

    while (attempt < max_attempts) : (attempt += 1) {
        // Exponential backoff with jitter
        if (attempt > 0) {
            const base_wait: u64 = self.opts.reconnect_wait_ms;
            const shift: u6 = @intCast(@min(attempt - 1, 10));
            const backoff = @min(base_wait * (@as(u64, 1) << shift), 60_000);

            // Add ±20% jitter
            var rand_bytes: [4]u8 = undefined;
            std.crypto.random.bytes(&rand_bytes);
            const rand_val: u32 = std.mem.readInt(u32, &rand_bytes, .little);
            const jitter_range = backoff * 2 / 5; // 40% range for ±20%
            const jitter = if (jitter_range > 0) rand_val % @as(u32, @intCast(jitter_range)) else 0;
            const wait = backoff - jitter_range / 2 + jitter;

            std.Thread.sleep(wait * std.time.ns_per_ms);
        }

        // Try to connect
        self.doConnect() catch continue;

        // Reconnected — re-subscribe all active subscriptions
        self.resubscribeAll() catch {
            self.conn.disconnect();
            continue;
        };

        // Success
        self.reconnect_attempts = attempt + 1;
        self.outstanding_pings = 0;
        self.last_ping_time = std.time.nanoTimestamp();
        return;
    }

    self.status = .disconnected;
    return Error.ReconnectFailed;
}

fn resubscribeAll(self: *Client) Error!void {
    const subs = self.sub_mgr.allSubscriptions() catch return Error.OutOfMemory;
    defer self.allocator.free(subs);

    for (subs) |sub| {
        var buf: [1024]u8 = undefined;
        const formatted = Protocol.fmtSub(&buf, sub.subject, sub.queue_group, sub.sid) catch continue;
        self.conn.write(formatted) catch return Error.NotConnected;
    }
    self.conn.flush() catch return Error.NotConnected;
}

// ── Tests ──

test "ConnectOptions defaults" {
    const opts = ConnectOptions{};
    try std.testing.expect(opts.servers.len == 1);
    try std.testing.expectEqualStrings("nats://127.0.0.1:4222", opts.servers[0]);
    try std.testing.expect(opts.echo == true);
    try std.testing.expect(opts.no_responders == true);
    try std.testing.expect(opts.ping_interval_ms == 30_000);
    try std.testing.expect(opts.allow_reconnect == true);
    try std.testing.expect(opts.max_outstanding_pings == 2);
}

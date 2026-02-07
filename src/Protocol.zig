const std = @import("std");
const Allocator = std.mem.Allocator;
const Headers = @import("Headers.zig");

/// NATS wire protocol parser and serializer.
///
/// The parser is a state machine that processes bytes incrementally.
/// It handles: INFO, MSG, HMSG, PING, PONG, +OK, -ERR
///
/// The serializer produces: CONNECT, PUB, HPUB, SUB, UNSUB, PING, PONG

// ── Parsed message types ──

pub const ServerInfo = struct {
    server_id: []const u8 = "",
    server_name: []const u8 = "",
    version: []const u8 = "",
    proto: i64 = 0,
    host: []const u8 = "",
    port: u16 = 4222,
    max_payload: u32 = 1_048_576,
    headers: bool = false,
    jetstream: bool = false,
    client_id: u64 = 0,
    tls_required: bool = false,
    connect_urls: ?[]const []const u8 = null,

    /// Internal: arena that owns the memory for string fields and connect_urls.
    _arena: ?*std.heap.ArenaAllocator = null,

    /// Free memory owned by this ServerInfo. Safe to call multiple times.
    pub fn deinit(self: *ServerInfo) void {
        if (self._arena) |arena| {
            const child = arena.child_allocator;
            arena.deinit();
            child.destroy(arena);
        }
        self._arena = null;
        self.connect_urls = null;
    }
};

pub const Msg = struct {
    subject: []const u8,
    sid: []const u8,
    reply_to: ?[]const u8 = null,
    headers: ?Headers = null,
    payload: ?[]const u8 = null,

    /// Arena that owns all the memory for this message's fields.
    arena: ?std.heap.ArenaAllocator = null,

    pub fn deinit(self: *Msg) void {
        if (self.arena) |*a| {
            a.deinit();
            self.arena = null;
        }
    }
};

pub const ServerOp = union(enum) {
    info: ServerInfo,
    msg: Msg,
    ping,
    pong,
    ok,
    err: []const u8,
};

// ── Parser ──

pub const ParseError = error{
    InvalidProtocol,
    PayloadTooLarge,
    OutOfMemory,
    InvalidHeaderLength,
};

const ParserState = enum {
    awaiting_op,
    awaiting_msg_payload,
    awaiting_hmsg_headers_and_payload,
};

const MAX_PENDING_FIELD = 512;

pub const Parser = struct {
    state: ParserState = .awaiting_op,
    allocator: Allocator,

    // Pending MSG/HMSG metadata stored in fixed buffers
    pending_subject_buf: [MAX_PENDING_FIELD]u8 = undefined,
    pending_subject_len: usize = 0,
    pending_sid_buf: [64]u8 = undefined,
    pending_sid_len: usize = 0,
    pending_reply_buf: [MAX_PENDING_FIELD]u8 = undefined,
    pending_reply_len: usize = 0,
    pending_has_reply: bool = false,
    pending_payload_len: usize = 0,
    pending_header_len: usize = 0,
    pending_total_len: usize = 0,

    pub fn init(allocator: Allocator) Parser {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Parser) void {
        _ = self;
    }

    pub fn parse(self: *Parser, data: []const u8) ParseError!?ServerOp {
        switch (self.state) {
            .awaiting_op => return try self.parseOp(data),
            .awaiting_msg_payload => return try self.parseMsgPayload(data),
            .awaiting_hmsg_headers_and_payload => return try self.parseHmsgPayload(data),
        }
    }

    fn parseOp(self: *Parser, line: []const u8) ParseError!?ServerOp {
        if (line.len == 0) return null;

        if (std.ascii.startsWithIgnoreCase(line, "PING")) return .ping;
        if (std.ascii.startsWithIgnoreCase(line, "PONG")) return .pong;

        if (line.len >= 3 and line[0] == '+' and (line[1] == 'O' or line[1] == 'o') and (line[2] == 'K' or line[2] == 'k')) {
            return .ok;
        }

        if (line.len >= 4 and line[0] == '-' and std.ascii.startsWithIgnoreCase(line[1..], "ERR")) {
            const err_msg = if (line.len > 5) std.mem.trim(u8, line[5..], " '") else "";
            return .{ .err = err_msg };
        }

        if (std.ascii.startsWithIgnoreCase(line, "INFO ") or std.ascii.startsWithIgnoreCase(line, "INFO\t")) {
            const json_str = std.mem.trimLeft(u8, line[4..], " \t");
            return .{ .info = parseServerInfo(self.allocator, json_str) };
        }

        if (std.ascii.startsWithIgnoreCase(line, "MSG ")) {
            try self.parseMsgHeader(line[4..]);
            return null;
        }

        if (std.ascii.startsWithIgnoreCase(line, "HMSG ")) {
            try self.parseHmsgHeader(line[5..]);
            return null;
        }

        return ParseError.InvalidProtocol;
    }

    fn parseMsgHeader(self: *Parser, args: []const u8) ParseError!void {
        var it = std.mem.tokenizeScalar(u8, args, ' ');
        const subject = it.next() orelse return ParseError.InvalidProtocol;
        const sid = it.next() orelse return ParseError.InvalidProtocol;
        const third = it.next() orelse return ParseError.InvalidProtocol;

        if (subject.len > MAX_PENDING_FIELD or sid.len > 64) return ParseError.PayloadTooLarge;

        if (it.next()) |fourth| {
            // 4 tokens: subject sid reply_to payload_len
            if (third.len > MAX_PENDING_FIELD) return ParseError.PayloadTooLarge;
            @memcpy(self.pending_reply_buf[0..third.len], third);
            self.pending_reply_len = third.len;
            self.pending_has_reply = true;
            self.pending_payload_len = std.fmt.parseInt(usize, fourth, 10) catch return ParseError.InvalidProtocol;
        } else {
            // 3 tokens: subject sid payload_len
            self.pending_has_reply = false;
            self.pending_payload_len = std.fmt.parseInt(usize, third, 10) catch return ParseError.InvalidProtocol;
        }

        @memcpy(self.pending_subject_buf[0..subject.len], subject);
        self.pending_subject_len = subject.len;
        @memcpy(self.pending_sid_buf[0..sid.len], sid);
        self.pending_sid_len = sid.len;
        self.state = .awaiting_msg_payload;
    }

    fn parseHmsgHeader(self: *Parser, args: []const u8) ParseError!void {
        var it = std.mem.tokenizeScalar(u8, args, ' ');
        const subject = it.next() orelse return ParseError.InvalidProtocol;
        const sid = it.next() orelse return ParseError.InvalidProtocol;
        const third = it.next() orelse return ParseError.InvalidProtocol;
        const fourth = it.next() orelse return ParseError.InvalidProtocol;

        if (subject.len > MAX_PENDING_FIELD or sid.len > 64) return ParseError.PayloadTooLarge;

        if (it.next()) |fifth| {
            // 5 tokens: subject sid reply_to header_len total_len
            if (third.len > MAX_PENDING_FIELD) return ParseError.PayloadTooLarge;
            @memcpy(self.pending_reply_buf[0..third.len], third);
            self.pending_reply_len = third.len;
            self.pending_has_reply = true;
            self.pending_header_len = std.fmt.parseInt(usize, fourth, 10) catch return ParseError.InvalidProtocol;
            self.pending_total_len = std.fmt.parseInt(usize, fifth, 10) catch return ParseError.InvalidProtocol;
        } else {
            // 4 tokens: subject sid header_len total_len
            self.pending_has_reply = false;
            self.pending_header_len = std.fmt.parseInt(usize, third, 10) catch return ParseError.InvalidProtocol;
            self.pending_total_len = std.fmt.parseInt(usize, fourth, 10) catch return ParseError.InvalidProtocol;
        }

        if (self.pending_header_len > self.pending_total_len) return ParseError.InvalidHeaderLength;

        @memcpy(self.pending_subject_buf[0..subject.len], subject);
        self.pending_subject_len = subject.len;
        @memcpy(self.pending_sid_buf[0..sid.len], sid);
        self.pending_sid_len = sid.len;
        self.state = .awaiting_hmsg_headers_and_payload;
    }

    fn parseMsgPayload(self: *Parser, payload_data: []const u8) ParseError!?ServerOp {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const subject = try aa.dupe(u8, self.pending_subject_buf[0..self.pending_subject_len]);
        const sid = try aa.dupe(u8, self.pending_sid_buf[0..self.pending_sid_len]);
        const reply_to = if (self.pending_has_reply)
            try aa.dupe(u8, self.pending_reply_buf[0..self.pending_reply_len])
        else
            null;

        const payload = if (payload_data.len > 0)
            try aa.dupe(u8, payload_data)
        else
            null;

        self.state = .awaiting_op;

        return .{ .msg = .{
            .subject = subject,
            .sid = sid,
            .reply_to = reply_to,
            .payload = payload,
            .arena = arena,
        } };
    }

    fn parseHmsgPayload(self: *Parser, full_data: []const u8) ParseError!?ServerOp {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const aa = arena.allocator();

        const hdr_len = self.pending_header_len;
        const total_len = self.pending_total_len;

        if (full_data.len < total_len) return ParseError.InvalidProtocol;

        const header_bytes = full_data[0..hdr_len];
        const payload_bytes = full_data[hdr_len..total_len];

        const subject = try aa.dupe(u8, self.pending_subject_buf[0..self.pending_subject_len]);
        const sid = try aa.dupe(u8, self.pending_sid_buf[0..self.pending_sid_len]);
        const reply_to = if (self.pending_has_reply)
            try aa.dupe(u8, self.pending_reply_buf[0..self.pending_reply_len])
        else
            null;

        const headers = Headers.parse(aa, header_bytes) catch null;
        const payload = if (payload_bytes.len > 0)
            try aa.dupe(u8, payload_bytes)
        else
            null;

        self.state = .awaiting_op;

        return .{ .msg = .{
            .subject = subject,
            .sid = sid,
            .reply_to = reply_to,
            .headers = headers,
            .payload = payload,
            .arena = arena,
        } };
    }

    pub fn pendingBytes(self: *const Parser) usize {
        return switch (self.state) {
            .awaiting_op => 0,
            .awaiting_msg_payload => self.pending_payload_len,
            .awaiting_hmsg_headers_and_payload => self.pending_total_len,
        };
    }
};

fn parseServerInfo(allocator: Allocator, json_str: []const u8) ServerInfo {
    var info = ServerInfo{};

    // Create a dedicated arena for ServerInfo string data
    const arena_ptr = allocator.create(std.heap.ArenaAllocator) catch return info;
    arena_ptr.* = std.heap.ArenaAllocator.init(allocator);
    info._arena = arena_ptr;
    const aa = arena_ptr.allocator();

    // Parse JSON temporarily — we'll dupe strings into our arena
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_str, .{}) catch return info;
    defer parsed.deinit();

    const obj = switch (parsed.value) {
        .object => |o| o,
        else => return info,
    };

    if (obj.get("server_id")) |v| {
        if (v == .string) info.server_id = aa.dupe(u8, v.string) catch "";
    }
    if (obj.get("server_name")) |v| {
        if (v == .string) info.server_name = aa.dupe(u8, v.string) catch "";
    }
    if (obj.get("version")) |v| {
        if (v == .string) info.version = aa.dupe(u8, v.string) catch "";
    }
    if (obj.get("proto")) |v| {
        if (v == .integer) info.proto = v.integer;
    }
    if (obj.get("host")) |v| {
        if (v == .string) info.host = aa.dupe(u8, v.string) catch "";
    }
    if (obj.get("port")) |v| {
        if (v == .integer) info.port = std.math.cast(u16, v.integer) orelse 4222;
    }
    if (obj.get("max_payload")) |v| {
        if (v == .integer) info.max_payload = std.math.cast(u32, v.integer) orelse 1_048_576;
    }
    if (obj.get("headers")) |v| {
        if (v == .bool) info.headers = v.bool;
    }
    if (obj.get("jetstream")) |v| {
        if (v == .bool) info.jetstream = v.bool;
    }
    if (obj.get("client_id")) |v| {
        if (v == .integer) info.client_id = std.math.cast(u64, v.integer) orelse 0;
    }
    if (obj.get("tls_required")) |v| {
        if (v == .bool) info.tls_required = v.bool;
    }

    // Parse connect_urls array — dupe everything into our arena
    if (obj.get("connect_urls")) |v| {
        if (v == .array) {
            const arr = v.array;
            if (arr.items.len > 0) {
                const urls = aa.alloc([]const u8, arr.items.len) catch return info;
                var count: usize = 0;
                for (arr.items) |item| {
                    if (item == .string) {
                        urls[count] = aa.dupe(u8, item.string) catch continue;
                        count += 1;
                    }
                }
                if (count > 0) {
                    info.connect_urls = urls[0..count];
                }
            }
        }
    }

    return info;
}

// ── Serializer ──

pub const SerializeError = error{
    BufferTooSmall,
};

pub fn fmtConnect(buf: []u8, opts: ConnectOptions) SerializeError![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    writer.writeAll("CONNECT ") catch return SerializeError.BufferTooSmall;

    writer.writeAll("{") catch return SerializeError.BufferTooSmall;
    writeJsonBool(writer, "verbose", opts.verbose, true) catch return SerializeError.BufferTooSmall;
    writeJsonBool(writer, "pedantic", opts.pedantic, false) catch return SerializeError.BufferTooSmall;
    writeJsonBool(writer, "echo", opts.echo, false) catch return SerializeError.BufferTooSmall;
    writeJsonBool(writer, "no_responders", opts.no_responders, false) catch return SerializeError.BufferTooSmall;
    writeJsonBool(writer, "headers", true, false) catch return SerializeError.BufferTooSmall;
    writeJsonInt(writer, "protocol", 1, false) catch return SerializeError.BufferTooSmall;

    if (opts.name) |name| {
        writeJsonStr(writer, "name", name, false) catch return SerializeError.BufferTooSmall;
    }
    if (opts.token) |token| {
        writeJsonStr(writer, "auth_token", token, false) catch return SerializeError.BufferTooSmall;
    }
    if (opts.user) |user| {
        writeJsonStr(writer, "user", user, false) catch return SerializeError.BufferTooSmall;
    }
    if (opts.pass) |pass| {
        writeJsonStr(writer, "pass", pass, false) catch return SerializeError.BufferTooSmall;
    }

    writer.writeAll("}") catch return SerializeError.BufferTooSmall;
    writer.writeAll("\r\n") catch return SerializeError.BufferTooSmall;

    return stream.getWritten();
}

pub fn fmtPub(buf: []u8, subject: []const u8, reply_to: ?[]const u8, payload: ?[]const u8) SerializeError![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    const payload_len = if (payload) |p| p.len else 0;

    if (reply_to) |rt| {
        std.fmt.format(writer, "PUB {s} {s} {d}\r\n", .{ subject, rt, payload_len }) catch return SerializeError.BufferTooSmall;
    } else {
        std.fmt.format(writer, "PUB {s} {d}\r\n", .{ subject, payload_len }) catch return SerializeError.BufferTooSmall;
    }

    if (payload) |p| {
        writer.writeAll(p) catch return SerializeError.BufferTooSmall;
    }
    writer.writeAll("\r\n") catch return SerializeError.BufferTooSmall;

    return stream.getWritten();
}

pub fn fmtHpub(buf: []u8, subject: []const u8, reply_to: ?[]const u8, hdrs: *const Headers, payload: ?[]const u8) SerializeError![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    var hdr_buf: [4096]u8 = undefined;
    const hdr_bytes = hdrs.serialize(&hdr_buf) catch return SerializeError.BufferTooSmall;
    const hdr_len = hdr_bytes.len;
    const payload_len = if (payload) |p| p.len else 0;
    const total_len = hdr_len + payload_len;

    if (reply_to) |rt| {
        std.fmt.format(writer, "HPUB {s} {s} {d} {d}\r\n", .{ subject, rt, hdr_len, total_len }) catch return SerializeError.BufferTooSmall;
    } else {
        std.fmt.format(writer, "HPUB {s} {d} {d}\r\n", .{ subject, hdr_len, total_len }) catch return SerializeError.BufferTooSmall;
    }

    writer.writeAll(hdr_bytes) catch return SerializeError.BufferTooSmall;
    if (payload) |p| {
        writer.writeAll(p) catch return SerializeError.BufferTooSmall;
    }
    writer.writeAll("\r\n") catch return SerializeError.BufferTooSmall;

    return stream.getWritten();
}

pub fn fmtSub(buf: []u8, subject: []const u8, queue_group: ?[]const u8, sid: u64) SerializeError![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    if (queue_group) |qg| {
        std.fmt.format(writer, "SUB {s} {s} {d}\r\n", .{ subject, qg, sid }) catch return SerializeError.BufferTooSmall;
    } else {
        std.fmt.format(writer, "SUB {s} {d}\r\n", .{ subject, sid }) catch return SerializeError.BufferTooSmall;
    }

    return stream.getWritten();
}

pub fn fmtUnsub(buf: []u8, sid: u64, max_msgs: ?u32) SerializeError![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    if (max_msgs) |mm| {
        std.fmt.format(writer, "UNSUB {d} {d}\r\n", .{ sid, mm }) catch return SerializeError.BufferTooSmall;
    } else {
        std.fmt.format(writer, "UNSUB {d}\r\n", .{sid}) catch return SerializeError.BufferTooSmall;
    }

    return stream.getWritten();
}

pub fn fmtPing(buf: []u8) SerializeError![]const u8 {
    if (buf.len < 6) return SerializeError.BufferTooSmall;
    @memcpy(buf[0..6], "PING\r\n");
    return buf[0..6];
}

pub fn fmtPong(buf: []u8) SerializeError![]const u8 {
    if (buf.len < 6) return SerializeError.BufferTooSmall;
    @memcpy(buf[0..6], "PONG\r\n");
    return buf[0..6];
}

// ── Connect options (serialization) ──

pub const ConnectOptions = struct {
    verbose: bool = false,
    pedantic: bool = false,
    echo: bool = true,
    no_responders: bool = true,
    name: ?[]const u8 = null,
    token: ?[]const u8 = null,
    user: ?[]const u8 = null,
    pass: ?[]const u8 = null,
};

// ── JSON helpers ──

fn writeJsonBool(writer: anytype, key: []const u8, value: bool, first: bool) !void {
    if (!first) try writer.writeAll(",");
    try writer.writeAll("\"");
    try writer.writeAll(key);
    try writer.writeAll("\":");
    if (value) {
        try writer.writeAll("true");
    } else {
        try writer.writeAll("false");
    }
}

fn writeJsonStr(writer: anytype, key: []const u8, value: []const u8, first: bool) !void {
    if (!first) try writer.writeAll(",");
    try writer.writeAll("\"");
    try writer.writeAll(key);
    try writer.writeAll("\":\"");
    try writeEscapedJson(writer, value);
    try writer.writeAll("\"");
}

/// Escape a string for safe embedding in a JSON value.
fn writeEscapedJson(writer: anytype, value: []const u8) !void {
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

fn writeJsonInt(writer: anytype, key: []const u8, value: i64, first: bool) !void {
    if (!first) try writer.writeAll(",");
    try writer.writeAll("\"");
    try writer.writeAll(key);
    try writer.writeAll("\":");
    try std.fmt.format(writer, "{d}", .{value});
}

// ── Tests ──

test "parse PING" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const result = try parser.parse("PING");
    try std.testing.expect(result != null);
    try std.testing.expect(result.? == .ping);
}

test "parse PONG" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const result = try parser.parse("PONG");
    try std.testing.expect(result != null);
    try std.testing.expect(result.? == .pong);
}

test "parse +OK" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const result = try parser.parse("+OK");
    try std.testing.expect(result != null);
    try std.testing.expect(result.? == .ok);
}

test "parse -ERR" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const result = try parser.parse("-ERR 'Authorization Violation'");
    try std.testing.expect(result != null);
    switch (result.?) {
        .err => |msg| try std.testing.expectEqualStrings("Authorization Violation", msg),
        else => return error.UnexpectedResult,
    }
}

test "parse INFO" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const result = try parser.parse(
        \\INFO {"server_id":"test","version":"2.10.0","proto":1,"max_payload":1048576,"headers":true,"jetstream":true}
    );
    try std.testing.expect(result != null);
    var info: ServerInfo = switch (result.?) {
        .info => |i| i,
        else => return error.UnexpectedResult,
    };
    defer info.deinit();
    try std.testing.expect(info.proto == 1);
    try std.testing.expect(info.max_payload == 1_048_576);
    try std.testing.expect(info.headers == true);
    try std.testing.expect(info.jetstream == true);
}

test "parse MSG without reply-to" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const r1 = try parser.parse("MSG test.subject 1 5");
    try std.testing.expect(r1 == null);
    try std.testing.expect(parser.pendingBytes() == 5);

    var op = (try parser.parse("hello")).?;
    switch (op) {
        .msg => |*m| {
            defer m.deinit();
            try std.testing.expectEqualStrings("test.subject", m.subject);
            try std.testing.expectEqualStrings("1", m.sid);
            try std.testing.expect(m.reply_to == null);
            try std.testing.expectEqualStrings("hello", m.payload.?);
        },
        else => return error.UnexpectedResult,
    }
}

test "parse MSG with reply-to" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const r1 = try parser.parse("MSG test.subject 1 _INBOX.abc 5");
    try std.testing.expect(r1 == null);

    var op = (try parser.parse("hello")).?;
    switch (op) {
        .msg => |*m| {
            defer m.deinit();
            try std.testing.expectEqualStrings("test.subject", m.subject);
            try std.testing.expectEqualStrings("_INBOX.abc", m.reply_to.?);
            try std.testing.expectEqualStrings("hello", m.payload.?);
        },
        else => return error.UnexpectedResult,
    }
}

test "parse MSG with empty payload" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const r1 = try parser.parse("MSG test.subject 1 0");
    try std.testing.expect(r1 == null);
    try std.testing.expect(parser.pendingBytes() == 0);

    var op = (try parser.parse("")).?;
    switch (op) {
        .msg => |*m| {
            defer m.deinit();
            try std.testing.expectEqualStrings("test.subject", m.subject);
            try std.testing.expect(m.payload == null);
        },
        else => return error.UnexpectedResult,
    }
}

test "serialize PUB" {
    var buf: [256]u8 = undefined;
    const result = try fmtPub(&buf, "test.subject", null, "hello");
    try std.testing.expectEqualStrings("PUB test.subject 5\r\nhello\r\n", result);
}

test "serialize PUB with reply-to" {
    var buf: [256]u8 = undefined;
    const result = try fmtPub(&buf, "test.subject", "_INBOX.abc", "hello");
    try std.testing.expectEqualStrings("PUB test.subject _INBOX.abc 5\r\nhello\r\n", result);
}

test "serialize PUB empty payload" {
    var buf: [256]u8 = undefined;
    const result = try fmtPub(&buf, "test.subject", null, null);
    try std.testing.expectEqualStrings("PUB test.subject 0\r\n\r\n", result);
}

test "serialize SUB" {
    var buf: [256]u8 = undefined;
    const result = try fmtSub(&buf, "test.subject", null, 1);
    try std.testing.expectEqualStrings("SUB test.subject 1\r\n", result);
}

test "serialize SUB with queue group" {
    var buf: [256]u8 = undefined;
    const result = try fmtSub(&buf, "test.subject", "workers", 1);
    try std.testing.expectEqualStrings("SUB test.subject workers 1\r\n", result);
}

test "serialize UNSUB" {
    var buf: [256]u8 = undefined;
    const result = try fmtUnsub(&buf, 1, null);
    try std.testing.expectEqualStrings("UNSUB 1\r\n", result);
}

test "serialize UNSUB with max" {
    var buf: [256]u8 = undefined;
    const result = try fmtUnsub(&buf, 1, 5);
    try std.testing.expectEqualStrings("UNSUB 1 5\r\n", result);
}

test "serialize PING/PONG" {
    var buf: [16]u8 = undefined;
    try std.testing.expectEqualStrings("PING\r\n", try fmtPing(&buf));
    try std.testing.expectEqualStrings("PONG\r\n", try fmtPong(&buf));
}

test "serialize CONNECT" {
    var buf: [1024]u8 = undefined;
    const result = try fmtConnect(&buf, .{
        .name = "test-client",
    });
    try std.testing.expect(std.mem.startsWith(u8, result, "CONNECT {"));
    try std.testing.expect(std.mem.endsWith(u8, result, "}\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "\"verbose\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"echo\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"headers\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"protocol\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"test-client\"") != null);
}

test "parse INFO with connect_urls" {
    var parser = Parser.init(std.testing.allocator);
    defer parser.deinit();

    const result = try parser.parse(
        \\INFO {"server_id":"test","version":"2.10.0","proto":1,"connect_urls":["10.0.0.1:4222","10.0.0.2:4222"]}
    );
    try std.testing.expect(result != null);
    var info: ServerInfo = switch (result.?) {
        .info => |i| i,
        else => return error.UnexpectedResult,
    };
    defer info.deinit();
    try std.testing.expect(info.connect_urls != null);
    try std.testing.expect(info.connect_urls.?.len == 2);
}

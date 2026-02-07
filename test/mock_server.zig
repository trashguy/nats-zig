const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

/// In-process mock NATS server for testing.
pub const MockServer = struct {
    const Self = @This();

    allocator: Allocator,
    listener: posix.socket_t,
    port: u16,
    thread: ?std.Thread = null,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn start(allocator: Allocator) !*Self {
        return startOnPort(allocator, 0);
    }

    pub fn startOnPort(allocator: Allocator, requested_port: u16) !*Self {
        const self = try allocator.create(Self);

        const listener = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, posix.IPPROTO.TCP);
        errdefer posix.close(listener);

        const one: u32 = 1;
        posix.setsockopt(listener, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&one)) catch {};

        // Set accept timeout so serverLoop can check running flag periodically
        const SO_RCVTIMEO = if (@hasDecl(posix.SO, "RCVTIMEO")) posix.SO.RCVTIMEO else 20;
        const timeval = extern struct { tv_sec: i64, tv_usec: i64 };
        const tv = timeval{ .tv_sec = 0, .tv_usec = 200_000 }; // 200ms
        posix.setsockopt(listener, posix.SOL.SOCKET, SO_RCVTIMEO, std.mem.asBytes(&tv)) catch {};

        const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, requested_port);
        try posix.bind(listener, &addr.any, addr.getOsSockLen());
        try posix.listen(listener, 1);

        var bound_addr: posix.sockaddr.storage = std.mem.zeroes(posix.sockaddr.storage);
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
        try posix.getsockname(listener, @ptrCast(&bound_addr), &addr_len);
        const inet_addr: *const posix.sockaddr.in = @ptrCast(&bound_addr);
        const port = std.mem.bigToNative(u16, inet_addr.port);

        self.* = .{
            .allocator = allocator,
            .listener = listener,
            .port = port,
        };
        self.running.store(true, .release);

        self.thread = try std.Thread.spawn(.{}, serverLoop, .{self});

        return self;
    }

    pub fn stop(self: *Self) void {
        self.running.store(false, .release);
        posix.close(self.listener);
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
        self.allocator.destroy(self);
    }

    pub fn url(self: *const Self) [32]u8 {
        var buf: [32]u8 = undefined;
        const written = std.fmt.bufPrint(&buf, "nats://127.0.0.1:{d}", .{self.port}) catch unreachable;
        @memset(buf[written.len..], 0);
        return buf;
    }

    fn serverLoop(self: *Self) void {
        while (self.running.load(.acquire)) {
            const client_sock = posix.accept(self.listener, null, null, posix.SOCK.CLOEXEC) catch |err| switch (err) {
                error.WouldBlock => continue, // timeout, check running flag
                else => break,
            };

            self.handleClient(client_sock) catch {};
            posix.close(client_sock);
        }
    }

    fn handleClient(self: *Self, sock: posix.socket_t) !void {
        const SO_RCVTIMEO = 20;
        const timeval = extern struct { tv_sec: i64, tv_usec: i64 };
        const tv = timeval{ .tv_sec = 0, .tv_usec = 200_000 };
        posix.setsockopt(sock, posix.SOL.SOCKET, SO_RCVTIMEO, std.mem.asBytes(&tv)) catch {};

        var subs = std.StringHashMapUnmanaged([]const u8){};
        defer {
            var it = subs.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            subs.deinit(self.allocator);
        }

        var js_state = JetStreamState{};

        const info_msg = "INFO {\"server_id\":\"mock\",\"version\":\"0.0.1\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":0,\"max_payload\":1048576,\"headers\":true,\"jetstream\":true}\r\n";
        sendAll(sock, info_msg) catch return;

        var read_buf: [65536]u8 = undefined;
        var buf_len: usize = 0;

        while (self.running.load(.acquire)) {
            const n = posix.recv(sock, read_buf[buf_len..], 0) catch |err| switch (err) {
                error.WouldBlock => continue,
                else => break,
            };
            if (n == 0) break;
            buf_len += n;

            while (true) {
                const data = read_buf[0..buf_len];
                const line_end = std.mem.indexOf(u8, data, "\r\n") orelse break;
                const line = data[0..line_end];

                if (std.ascii.startsWithIgnoreCase(line, "CONNECT ")) {
                    consumeLine(&read_buf, &buf_len, line_end + 2);
                    continue;
                }

                if (std.ascii.startsWithIgnoreCase(line, "PING")) {
                    sendAll(sock, "PONG\r\n") catch return;
                    consumeLine(&read_buf, &buf_len, line_end + 2);
                    continue;
                }

                if (std.ascii.startsWithIgnoreCase(line, "PONG")) {
                    consumeLine(&read_buf, &buf_len, line_end + 2);
                    continue;
                }

                if (std.ascii.startsWithIgnoreCase(line, "SUB ")) {
                    const args = line[4..];
                    var it = std.mem.tokenizeScalar(u8, args, ' ');
                    const subject = it.next() orelse {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    const second = it.next() orelse {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    const sid = it.next() orelse second;

                    const duped_subject = self.allocator.dupe(u8, subject) catch {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    const duped_sid = self.allocator.dupe(u8, sid) catch {
                        self.allocator.free(duped_subject);
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    subs.put(self.allocator, duped_subject, duped_sid) catch {
                        self.allocator.free(duped_subject);
                        self.allocator.free(duped_sid);
                    };

                    consumeLine(&read_buf, &buf_len, line_end + 2);
                    continue;
                }

                if (std.ascii.startsWithIgnoreCase(line, "UNSUB ")) {
                    consumeLine(&read_buf, &buf_len, line_end + 2);
                    continue;
                }

                if (std.ascii.startsWithIgnoreCase(line, "HPUB ")) {
                    // HPUB subject [reply_to] hdr_len total_len
                    const args = line[5..];
                    var it = std.mem.tokenizeScalar(u8, args, ' ');
                    const hpub_subject_raw = it.next() orelse {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    const hpub_second = it.next() orelse {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    const hpub_third = it.next() orelse {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };

                    var hpub_reply_to_raw: ?[]const u8 = null;
                    var hpub_total_len: usize = 0;

                    if (it.next()) |fourth| {
                        hpub_reply_to_raw = hpub_second;
                        hpub_total_len = std.fmt.parseInt(usize, fourth, 10) catch 0;
                    } else {
                        hpub_total_len = std.fmt.parseInt(usize, hpub_third, 10) catch 0;
                    }

                    // Copy subject/reply_to before consumeLine invalidates them
                    var hpub_subj_buf: [256]u8 = undefined;
                    @memcpy(hpub_subj_buf[0..hpub_subject_raw.len], hpub_subject_raw);
                    const hpub_subject = hpub_subj_buf[0..hpub_subject_raw.len];

                    var hpub_rt_buf: [256]u8 = undefined;
                    var hpub_reply_to: ?[]const u8 = null;
                    if (hpub_reply_to_raw) |rt| {
                        @memcpy(hpub_rt_buf[0..rt.len], rt);
                        hpub_reply_to = hpub_rt_buf[0..rt.len];
                    }

                    consumeLine(&read_buf, &buf_len, line_end + 2);

                    const hpub_needed = hpub_total_len + 2;
                    while (buf_len < hpub_needed) {
                        const pn = posix.recv(sock, read_buf[buf_len..], 0) catch break;
                        if (pn == 0) break;
                        buf_len += pn;
                    }
                    if (buf_len < hpub_needed) break;

                    routeMessage(sock, &subs, hpub_subject, hpub_reply_to, null, &js_state);
                    consumeLine(&read_buf, &buf_len, hpub_needed);
                    continue;
                }

                if (std.ascii.startsWithIgnoreCase(line, "PUB ")) {
                    const args = line[4..];
                    var it = std.mem.tokenizeScalar(u8, args, ' ');
                    const subject_raw = it.next() orelse {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    const second = it.next() orelse {
                        consumeLine(&read_buf, &buf_len, line_end + 2);
                        continue;
                    };
                    var reply_to_raw: ?[]const u8 = null;
                    var payload_len: usize = 0;

                    if (it.next()) |third| {
                        reply_to_raw = second;
                        payload_len = std.fmt.parseInt(usize, third, 10) catch 0;
                    } else {
                        payload_len = std.fmt.parseInt(usize, second, 10) catch 0;
                    }

                    // Copy subject/reply_to to stack buffers before consumeLine invalidates them
                    var subject_buf: [256]u8 = undefined;
                    @memcpy(subject_buf[0..subject_raw.len], subject_raw);
                    const subject_copy = subject_buf[0..subject_raw.len];

                    var reply_to_buf: [256]u8 = undefined;
                    var reply_to_copy: ?[]const u8 = null;
                    if (reply_to_raw) |rt| {
                        @memcpy(reply_to_buf[0..rt.len], rt);
                        reply_to_copy = reply_to_buf[0..rt.len];
                    }

                    consumeLine(&read_buf, &buf_len, line_end + 2);

                    const total_needed = payload_len + 2;
                    while (buf_len < total_needed) {
                        const pn = posix.recv(sock, read_buf[buf_len..], 0) catch break;
                        if (pn == 0) break;
                        buf_len += pn;
                    }

                    if (buf_len < total_needed) break;

                    const payload = if (payload_len > 0) read_buf[0..payload_len] else null;
                    routeMessage(sock, &subs, subject_copy, reply_to_copy, payload, &js_state);
                    consumeLine(&read_buf, &buf_len, total_needed);
                    continue;
                }

                consumeLine(&read_buf, &buf_len, line_end + 2);
            }
        }
    }

    fn routeMessage(sock: posix.socket_t, subs: *std.StringHashMapUnmanaged([]const u8), subject: []const u8, reply_to: ?[]const u8, payload: ?[]const u8, js_state: *JetStreamState) void {
        if (reply_to) |rt| {
            // Handle JetStream API requests ($JS.API.>): reply with API response
            if (std.mem.startsWith(u8, subject, "$JS.API.")) {
                const response = js_state.handleRequest(subject, payload);
                sendToInbox(sock, subs, rt, response);
                return;
            }

            // Handle JetStream publish (any PUB with reply_to to an _INBOX):
            // Real NATS sends a PubAck if subject matches a stream
            if (std.mem.startsWith(u8, rt, "_INBOX.")) {
                const response = js_state.pubAckResponse();
                sendToInbox(sock, subs, rt, response);
                return;
            }
        }

        // Regular message routing (echo/fan-out)
        if (subs.get(subject)) |sid| {
            sendMsgToClient(sock, subject, sid, reply_to, payload);
            return;
        }

        var it = subs.iterator();
        while (it.next()) |entry| {
            if (subjectMatch(entry.key_ptr.*, subject)) {
                sendMsgToClient(sock, subject, entry.value_ptr.*, reply_to, payload);
            }
        }
    }

    fn sendToInbox(sock: posix.socket_t, subs: *std.StringHashMapUnmanaged([]const u8), inbox: []const u8, response: []const u8) void {
        if (subs.get(inbox)) |sid| {
            sendMsgToClient(sock, inbox, sid, null, response);
            return;
        }
        var it = subs.iterator();
        while (it.next()) |entry| {
            if (subjectMatch(entry.key_ptr.*, inbox)) {
                sendMsgToClient(sock, inbox, entry.value_ptr.*, null, response);
                return;
            }
        }
    }

    fn sendMsgToClient(sock: posix.socket_t, subject: []const u8, sid: []const u8, reply_to: ?[]const u8, payload: ?[]const u8) void {
        var buf: [65536]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buf);
        const writer = stream.writer();

        const payload_len = if (payload) |p| p.len else 0;

        if (reply_to) |rt| {
            std.fmt.format(writer, "MSG {s} {s} {s} {d}\r\n", .{ subject, sid, rt, payload_len }) catch return;
        } else {
            std.fmt.format(writer, "MSG {s} {s} {d}\r\n", .{ subject, sid, payload_len }) catch return;
        }

        if (payload) |p| writer.writeAll(p) catch return;
        writer.writeAll("\r\n") catch return;

        sendAll(sock, stream.getWritten()) catch return;
    }

    fn consumeLine(buf: *[65536]u8, buf_len: *usize, consumed: usize) void {
        const remaining = buf_len.* - consumed;
        if (remaining > 0) {
            std.mem.copyForwards(u8, buf, buf[consumed..buf_len.*]);
        }
        buf_len.* = remaining;
    }

    fn sendAll(sock: posix.socket_t, data: []const u8) !void {
        var sent: usize = 0;
        while (sent < data.len) {
            sent += posix.send(sock, data[sent..], 0) catch return error.ConnectionClosed;
        }
    }
};

/// Minimal JetStream state for mock testing.
const JetStreamState = struct {
    next_seq: u64 = 1,
    response_buf: [4096]u8 = undefined,

    fn handleRequest(self: *JetStreamState, subject: []const u8, payload: ?[]const u8) []const u8 {
        _ = payload;
        // Extract the API path after "$JS.API."
        const api_path = subject[8..]; // skip "$JS.API."

        if (std.mem.startsWith(u8, api_path, "STREAM.CREATE.") or
            std.mem.startsWith(u8, api_path, "STREAM.UPDATE."))
        {
            return self.streamInfoResponse();
        }

        if (std.mem.startsWith(u8, api_path, "STREAM.INFO.")) {
            return self.streamInfoResponse();
        }

        if (std.mem.startsWith(u8, api_path, "STREAM.DELETE.")) {
            return self.simpleOkResponse();
        }

        if (std.mem.startsWith(u8, api_path, "CONSUMER.CREATE.")) {
            return self.consumerInfoResponse();
        }

        if (std.mem.startsWith(u8, api_path, "CONSUMER.DELETE.")) {
            return self.simpleOkResponse();
        }

        // Default: pub ack (for JetStream publish)
        return self.pubAckResponse();
    }

    fn pubAckResponse(self: *JetStreamState) []const u8 {
        const seq = self.next_seq;
        self.next_seq += 1;
        var stream = std.io.fixedBufferStream(&self.response_buf);
        std.fmt.format(stream.writer(), "{{\"stream\":\"TEST\",\"seq\":{d}}}", .{seq}) catch return "{}";
        return stream.getWritten();
    }

    fn streamInfoResponse(self: *JetStreamState) []const u8 {
        var stream = std.io.fixedBufferStream(&self.response_buf);
        std.fmt.format(stream.writer(),
            \\{{"type":"io.nats.jetstream.api.v1.stream_create_response","config":{{"name":"TEST"}},"state":{{"messages":{d},"bytes":0,"first_seq":1,"last_seq":{d}}}}}
        , .{ self.next_seq - 1, self.next_seq - 1 }) catch return "{}";
        return stream.getWritten();
    }

    fn consumerInfoResponse(self: *JetStreamState) []const u8 {
        var stream = std.io.fixedBufferStream(&self.response_buf);
        stream.writer().writeAll(
            \\{"type":"io.nats.jetstream.api.v1.consumer_create_response","config":{},"num_pending":0,"num_ack_pending":0}
        ) catch return "{}";
        return stream.getWritten();
    }

    fn simpleOkResponse(self: *JetStreamState) []const u8 {
        var stream = std.io.fixedBufferStream(&self.response_buf);
        stream.writer().writeAll(
            \\{"success":true}
        ) catch return "{}";
        return stream.getWritten();
    }
};

pub fn subjectMatch(pattern: []const u8, subject: []const u8) bool {
    var pat_it = std.mem.tokenizeScalar(u8, pattern, '.');
    var sub_it = std.mem.tokenizeScalar(u8, subject, '.');

    while (true) {
        const pat_tok = pat_it.next();
        const sub_tok = sub_it.next();

        if (pat_tok == null and sub_tok == null) return true;
        if (pat_tok == null) return false;
        if (std.mem.eql(u8, pat_tok.?, ">")) return true;
        if (sub_tok == null) return false;
        if (std.mem.eql(u8, pat_tok.?, "*")) continue;
        if (!std.mem.eql(u8, pat_tok.?, sub_tok.?)) return false;
    }
}

test "subject matching - exact" {
    try std.testing.expect(subjectMatch("foo.bar", "foo.bar"));
    try std.testing.expect(!subjectMatch("foo.bar", "foo.baz"));
    try std.testing.expect(!subjectMatch("foo.bar", "foo.bar.baz"));
}

test "subject matching - wildcard *" {
    try std.testing.expect(subjectMatch("foo.*", "foo.bar"));
    try std.testing.expect(subjectMatch("foo.*", "foo.baz"));
    try std.testing.expect(!subjectMatch("foo.*", "foo.bar.baz"));
    try std.testing.expect(subjectMatch("*.bar", "foo.bar"));
}

test "subject matching - wildcard >" {
    try std.testing.expect(subjectMatch("foo.>", "foo.bar"));
    try std.testing.expect(subjectMatch("foo.>", "foo.bar.baz"));
    try std.testing.expect(!subjectMatch("foo.>", "bar.baz"));
    try std.testing.expect(subjectMatch(">", "anything.at.all"));
}

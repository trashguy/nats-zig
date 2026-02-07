const std = @import("std");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");
const posix = std.posix;

/// Manages a TCP connection to a NATS server with buffered I/O.
const Connection = @This();

pub const ConnectError = error{
    InvalidUrl,
    ConnectionRefused,
    HandshakeFailed,
    Unexpected,
    OutOfMemory,
} || posix.ConnectError || posix.SocketError || std.net.TcpConnectToAddressError;

pub const ReadError = error{
    ConnectionClosed,
    Overflow,
    WouldBlock,
} || posix.RecvFromError;

pub const WriteError = error{
    ConnectionClosed,
} || posix.SendError;

/// Parsed NATS URL components.
pub const ServerUrl = struct {
    host: []const u8,
    port: u16,
    user: ?[]const u8 = null,
    pass: ?[]const u8 = null,
    tls: bool = false,
};

const READ_BUF_SIZE = 64 * 1024;
const WRITE_BUF_SIZE = 64 * 1024;

socket: posix.socket_t = undefined,
connected: bool = false,
is_tls: bool = false,

read_buf: [READ_BUF_SIZE]u8 = undefined,
read_start: usize = 0,
read_end: usize = 0,

write_buf: [WRITE_BUF_SIZE]u8 = undefined,
write_pos: usize = 0,

pub fn init() Connection {
    return .{};
}

pub fn deinit(self: *Connection) void {
    self.disconnect();
}

/// Parse a NATS URL: nats://[user:pass@]host[:port]
pub fn parseUrl(url: []const u8) ConnectError!ServerUrl {
    var result = ServerUrl{ .host = "127.0.0.1", .port = 4222 };
    var rest = url;

    if (std.mem.startsWith(u8, rest, "nats://")) {
        rest = rest[7..];
    } else if (std.mem.startsWith(u8, rest, "tls://")) {
        rest = rest[6..];
        result.tls = true;
    }

    if (std.mem.indexOf(u8, rest, "@")) |at_idx| {
        const auth = rest[0..at_idx];
        rest = rest[at_idx + 1 ..];
        if (std.mem.indexOf(u8, auth, ":")) |colon_idx| {
            result.user = auth[0..colon_idx];
            result.pass = auth[colon_idx + 1 ..];
        } else {
            result.user = auth;
        }
    }

    if (std.mem.indexOf(u8, rest, ":")) |colon_idx| {
        result.host = rest[0..colon_idx];
        result.port = std.fmt.parseInt(u16, rest[colon_idx + 1 ..], 10) catch return ConnectError.InvalidUrl;
    } else if (rest.len > 0) {
        result.host = rest;
    }

    return result;
}

/// Connect to a NATS server via TCP.
pub fn connect(self: *Connection, host: []const u8, port: u16) ConnectError!void {
    self.disconnect();

    const addr = std.net.Address.resolveIp(host, port) catch return ConnectError.ConnectionRefused;

    const sock = posix.socket(addr.any.family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, posix.IPPROTO.TCP) catch {
        return ConnectError.ConnectionRefused;
    };
    errdefer posix.close(sock);

    posix.connect(sock, &addr.any, addr.getOsSockLen()) catch return ConnectError.ConnectionRefused;

    // Disable Nagle's algorithm
    const one: u32 = 1;
    const TCP_NODELAY = 1;
    posix.setsockopt(sock, posix.IPPROTO.TCP, TCP_NODELAY, std.mem.asBytes(&one)) catch {};

    self.socket = sock;
    self.connected = true;
    self.read_start = 0;
    self.read_end = 0;
    self.write_pos = 0;
}

pub fn disconnect(self: *Connection) void {
    if (self.connected) {
        posix.close(self.socket);
        self.connected = false;
        self.is_tls = false;
        self.read_start = 0;
        self.read_end = 0;
        self.write_pos = 0;
    }
}

/// Upgrade the current TCP connection to TLS.
///
/// This uses Zig's std.crypto.tls.Client which requires std.Io.Reader/Writer
/// abstractions. Currently stubbed — returns an error. For production TLS use,
/// this needs to wrap the socket in std.Io.Reader/Writer and perform the
/// TLS handshake.
///
/// To use TLS with NATS, connect to a NATS server that has TLS configured:
///   nats-server --tls --tlscert=server-cert.pem --tlskey=server-key.pem
///
/// The client will detect TLS is required either from:
///   1. URL scheme: tls://host:port
///   2. Server INFO: tls_required=true
///   3. Client option: tls_required=true
pub fn upgradeTls(self: *Connection, host: []const u8) ConnectError!void {
    _ = host;
    if (!self.connected) return ConnectError.HandshakeFailed;

    // TODO: Implement TLS upgrade using std.crypto.tls.Client
    // The Zig 0.15 TLS client requires std.Io.Reader/Writer wrappers
    // around the raw socket, and a certificate bundle for verification.
    //
    // Rough implementation:
    //   1. Create std.Io.Reader wrapping self.socket recv
    //   2. Create std.Io.Writer wrapping self.socket send
    //   3. Call std.crypto.tls.Client.init(reader, writer, .{
    //        .host = .{ .explicit = host },
    //        .ca = .{ .no_verification },  // or bundle from system
    //   })
    //   4. Replace recv/send paths to use TLS client reader/writer
    //
    // For now, return error to indicate TLS is not yet fully implemented.
    return ConnectError.HandshakeFailed;
}

/// Set a receive timeout on the socket (milliseconds, 0 = no timeout).
pub fn setReadTimeout(self: *Connection, timeout_ms: u32) void {
    if (!self.connected) return;
    // Use platform-correct SO_RCVTIMEO (Linux=20, macOS=4102, etc.)
    const SO_RCVTIMEO = if (@hasDecl(posix.SO, "RCVTIMEO"))
        posix.SO.RCVTIMEO
    else
        20; // Linux fallback
    const timeval = extern struct {
        tv_sec: i64,
        tv_usec: i64,
    };
    const tv = timeval{
        .tv_sec = @intCast(timeout_ms / 1000),
        .tv_usec = @intCast(@as(u64, timeout_ms % 1000) * 1000),
    };
    posix.setsockopt(self.socket, posix.SOL.SOCKET, SO_RCVTIMEO, std.mem.asBytes(&tv)) catch {};
}

/// Read a line from the connection (terminated by \r\n).
/// Returns the line without the \r\n terminator.
/// The returned slice is valid until the next readLine() or readExact() call.
pub fn readLine(self: *Connection) ReadError![]const u8 {
    while (true) {
        if (self.read_start < self.read_end) {
            const buffered = self.read_buf[self.read_start..self.read_end];
            if (std.mem.indexOf(u8, buffered, "\r\n")) |idx| {
                const line = buffered[0..idx];
                self.read_start += idx + 2;
                return line;
            }
        }

        if (self.read_start > 0) {
            const remaining = self.read_end - self.read_start;
            if (remaining > 0) {
                std.mem.copyForwards(u8, &self.read_buf, self.read_buf[self.read_start..self.read_end]);
            }
            self.read_end = remaining;
            self.read_start = 0;
        }

        if (self.read_end >= READ_BUF_SIZE) {
            return ReadError.Overflow;
        }

        const n = posix.recv(self.socket, self.read_buf[self.read_end..], 0) catch |err| switch (err) {
            error.WouldBlock => return ReadError.WouldBlock,
            else => return ReadError.ConnectionClosed,
        };
        if (n == 0) return ReadError.ConnectionClosed;
        self.read_end += n;
    }
}

/// Read exactly `len` bytes from the connection.
/// Returns a slice valid until the next read call.
pub fn readExact(self: *Connection, len: usize) ReadError![]const u8 {
    const total_needed = len + 2; // payload + \r\n

    while ((self.read_end - self.read_start) < total_needed) {
        if (self.read_start > 0) {
            const remaining = self.read_end - self.read_start;
            if (remaining > 0) {
                std.mem.copyForwards(u8, &self.read_buf, self.read_buf[self.read_start..self.read_end]);
            }
            self.read_end = remaining;
            self.read_start = 0;
        }

        if (self.read_end >= READ_BUF_SIZE) {
            return ReadError.Overflow;
        }

        const n = posix.recv(self.socket, self.read_buf[self.read_end..], 0) catch |err| switch (err) {
            error.WouldBlock => return ReadError.WouldBlock,
            else => return ReadError.ConnectionClosed,
        };
        if (n == 0) return ReadError.ConnectionClosed;
        self.read_end += n;
    }

    const data = self.read_buf[self.read_start .. self.read_start + len];
    self.read_start += total_needed;
    return data;
}

/// Write data to the write buffer. Call flush() to send.
pub fn write(self: *Connection, data: []const u8) WriteError!void {
    if (data.len > WRITE_BUF_SIZE - self.write_pos) {
        try self.flush();
        if (data.len > WRITE_BUF_SIZE) {
            try self.sendAll(data);
            return;
        }
    }
    @memcpy(self.write_buf[self.write_pos .. self.write_pos + data.len], data);
    self.write_pos += data.len;
}

/// Flush the write buffer to the socket.
pub fn flush(self: *Connection) WriteError!void {
    if (self.write_pos == 0) return;
    try self.sendAll(self.write_buf[0..self.write_pos]);
    self.write_pos = 0;
}

fn sendAll(self: *Connection, data: []const u8) WriteError!void {
    if (!self.connected) return WriteError.ConnectionClosed;
    var sent: usize = 0;
    while (sent < data.len) {
        const n = posix.send(self.socket, data[sent..], 0) catch return WriteError.ConnectionClosed;
        if (n == 0) return WriteError.ConnectionClosed;
        sent += n;
    }
}

/// Result of readExactAlloc — either a zero-copy slice into read_buf
/// or an allocated buffer for payloads larger than READ_BUF_SIZE.
pub const ReadResult = struct {
    data: []const u8,
    allocated: ?[]u8 = null,

    pub fn free(self: ReadResult, allocator: Allocator) void {
        if (self.allocated) |buf| allocator.free(buf);
    }
};

/// Read exactly `len` bytes plus trailing \r\n, supporting payloads larger than READ_BUF_SIZE.
/// For small payloads, returns a zero-copy slice into the internal read buffer.
/// For large payloads, allocates a buffer and reads directly into it.
pub fn readExactAlloc(self: *Connection, allocator: Allocator, len: usize) (ReadError || Allocator.Error)!ReadResult {
    const total_needed = len + 2; // payload + \r\n

    // Small payload: use existing zero-copy path
    if (total_needed <= READ_BUF_SIZE) {
        const data = try self.readExact(len);
        return .{ .data = data };
    }

    // Large payload: allocate buffer, copy buffered data, recv rest
    const buf = try allocator.alloc(u8, len);
    errdefer allocator.free(buf);

    var filled: usize = 0;

    // Copy any already-buffered data
    const buffered = self.read_end - self.read_start;
    if (buffered > 0) {
        const to_copy = @min(buffered, len);
        @memcpy(buf[0..to_copy], self.read_buf[self.read_start .. self.read_start + to_copy]);
        self.read_start += to_copy;
        filled = to_copy;
    }

    // Recv remaining directly into allocated buffer
    while (filled < len) {
        const n = posix.recv(self.socket, buf[filled..len], 0) catch |err| switch (err) {
            error.WouldBlock => return ReadError.WouldBlock,
            else => return ReadError.ConnectionClosed,
        };
        if (n == 0) return ReadError.ConnectionClosed;
        filled += n;
    }

    // Consume trailing \r\n
    var crlf_consumed: usize = 0;
    // Check if some of the trailing \r\n is already buffered
    const remaining_buffered = self.read_end - self.read_start;
    if (remaining_buffered > 0) {
        const skip = @min(remaining_buffered, 2);
        self.read_start += skip;
        crlf_consumed = skip;
    }
    // Recv any remaining \r\n bytes
    while (crlf_consumed < 2) {
        var tmp: [2]u8 = undefined;
        const n = posix.recv(self.socket, tmp[0 .. 2 - crlf_consumed], 0) catch |err| switch (err) {
            error.WouldBlock => return ReadError.WouldBlock,
            else => return ReadError.ConnectionClosed,
        };
        if (n == 0) return ReadError.ConnectionClosed;
        crlf_consumed += n;
    }

    return .{ .data = buf, .allocated = buf };
}

/// Check if there is buffered data available to read without blocking.
pub fn hasPendingData(self: *const Connection) bool {
    return self.read_start < self.read_end;
}

// ── Tests ──

test "parse nats URL" {
    const url = try parseUrl("nats://localhost:4222");
    try std.testing.expectEqualStrings("localhost", url.host);
    try std.testing.expect(url.port == 4222);
    try std.testing.expect(url.user == null);
}

test "parse URL with auth" {
    const url = try parseUrl("nats://user:pass@myhost:5222");
    try std.testing.expectEqualStrings("myhost", url.host);
    try std.testing.expect(url.port == 5222);
    try std.testing.expectEqualStrings("user", url.user.?);
    try std.testing.expectEqualStrings("pass", url.pass.?);
}

test "parse URL defaults" {
    const url = try parseUrl("nats://127.0.0.1");
    try std.testing.expectEqualStrings("127.0.0.1", url.host);
    try std.testing.expect(url.port == 4222);
}

test "parse bare host:port" {
    const url = try parseUrl("localhost:4222");
    try std.testing.expectEqualStrings("localhost", url.host);
    try std.testing.expect(url.port == 4222);
}

test "parse tls URL" {
    const url = try parseUrl("tls://secure.nats.io:4443");
    try std.testing.expectEqualStrings("secure.nats.io", url.host);
    try std.testing.expect(url.port == 4443);
    try std.testing.expect(url.tls == true);
}

test "nats URL is not tls" {
    const url = try parseUrl("nats://localhost:4222");
    try std.testing.expect(url.tls == false);
}

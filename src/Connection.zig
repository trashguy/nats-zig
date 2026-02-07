const std = @import("std");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");
const posix = std.posix;
const tls = std.crypto.tls;
const Certificate = std.crypto.Certificate;
const Io = std.Io;

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

/// TLS state stored on the heap (TLS client is non-movable due to internal pointers).
pub const TlsState = struct {
    tls_client: tls.Client,
    ca_bundle: Certificate.Bundle,
    /// Socket wrapped as an std.net.Stream for the TLS client's encrypted I/O.
    net_stream: std.net.Stream,
    net_reader: std.net.Stream.Reader,
    net_writer: std.net.Stream.Writer,
    /// Buffers owned by TlsState for the TLS read/write operations.
    tls_read_buf: []u8,
    tls_write_buf: []u8,
    io_read_buf: []u8,
    io_write_buf: []u8,
    allocator: Allocator,

    pub fn deinit(self: *TlsState) void {
        self.ca_bundle.deinit(self.allocator);
        self.allocator.free(self.tls_read_buf);
        self.allocator.free(self.tls_write_buf);
        self.allocator.free(self.io_read_buf);
        self.allocator.free(self.io_write_buf);
        self.allocator.destroy(self);
    }
};

socket: posix.socket_t = undefined,
connected: bool = false,
is_tls: bool = false,
tls_state: ?*TlsState = null,

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
        if (self.tls_state) |ts| {
            ts.deinit();
            self.tls_state = null;
        }
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
/// Creates std.net.Stream Reader/Writer from the raw socket, loads system CA
/// certificates, performs the TLS handshake, and installs the TLS state. All
/// subsequent read/write operations are transparently encrypted.
///
/// If `verify` is false, certificate verification is skipped (dev/testing only).
pub fn upgradeTls(self: *Connection, allocator: Allocator, host: []const u8, verify: bool) ConnectError!void {
    if (!self.connected) return ConnectError.HandshakeFailed;

    // Allocate TLS state on the heap (TLS client is non-movable)
    const ts = allocator.create(TlsState) catch return ConnectError.OutOfMemory;
    errdefer allocator.destroy(ts);

    // Allocate buffers for TLS I/O
    const io_read_buf = allocator.alloc(u8, tls.Client.min_buffer_len) catch {
        allocator.destroy(ts);
        return ConnectError.OutOfMemory;
    };
    errdefer allocator.free(io_read_buf);

    const io_write_buf = allocator.alloc(u8, 16384) catch {
        allocator.free(io_read_buf);
        allocator.destroy(ts);
        return ConnectError.OutOfMemory;
    };
    errdefer allocator.free(io_write_buf);

    const tls_read_buf = allocator.alloc(u8, tls.Client.min_buffer_len) catch {
        allocator.free(io_write_buf);
        allocator.free(io_read_buf);
        allocator.destroy(ts);
        return ConnectError.OutOfMemory;
    };
    errdefer allocator.free(tls_read_buf);

    const tls_write_buf = allocator.alloc(u8, 16384) catch {
        allocator.free(tls_read_buf);
        allocator.free(io_write_buf);
        allocator.free(io_read_buf);
        allocator.destroy(ts);
        return ConnectError.OutOfMemory;
    };
    errdefer allocator.free(tls_write_buf);

    // Load CA certificates for verification
    ts.ca_bundle = .{};
    ts.allocator = allocator;
    ts.tls_read_buf = tls_read_buf;
    ts.tls_write_buf = tls_write_buf;
    ts.io_read_buf = io_read_buf;
    ts.io_write_buf = io_write_buf;

    if (verify) {
        ts.ca_bundle.rescan(allocator) catch {
            allocator.free(tls_write_buf);
            allocator.free(tls_read_buf);
            allocator.free(io_write_buf);
            allocator.free(io_read_buf);
            allocator.destroy(ts);
            return ConnectError.HandshakeFailed;
        };
    }

    // Wrap socket in std.net.Stream reader/writer
    ts.net_stream = .{ .handle = self.socket };
    ts.net_reader = ts.net_stream.reader(io_read_buf);
    ts.net_writer = ts.net_stream.writer(io_write_buf);

    // Perform TLS handshake
    ts.tls_client = tls.Client.init(
        ts.net_reader.interface(),
        &ts.net_writer.interface,
        .{
            .host = if (verify) .{ .explicit = host } else .no_verification,
            .ca = if (verify) .{ .bundle = ts.ca_bundle } else .no_verification,
            .read_buffer = tls_read_buf,
            .write_buffer = tls_write_buf,
        },
    ) catch {
        if (verify) ts.ca_bundle.deinit(allocator);
        allocator.free(tls_write_buf);
        allocator.free(tls_read_buf);
        allocator.free(io_write_buf);
        allocator.free(io_read_buf);
        allocator.destroy(ts);
        return ConnectError.HandshakeFailed;
    };

    self.tls_state = ts;
    self.is_tls = true;

    // Clear any buffered data from pre-TLS reads
    self.read_start = 0;
    self.read_end = 0;
    self.write_pos = 0;
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

// ── Abstracted read/write paths ──

/// Read bytes from the connection (TLS-aware).
fn recvBytes(self: *Connection, buf: []u8) ReadError!usize {
    if (self.tls_state) |ts| {
        // Read decrypted data through TLS client
        const n = ts.tls_client.reader.readSliceShort(buf) catch return ReadError.ConnectionClosed;
        return n;
    } else {
        const n = posix.recv(self.socket, buf, 0) catch |err| switch (err) {
            error.WouldBlock => return ReadError.WouldBlock,
            else => return ReadError.ConnectionClosed,
        };
        if (n == 0) return ReadError.ConnectionClosed;
        return n;
    }
}

/// Write bytes to the connection (TLS-aware).
fn sendBytes(self: *Connection, data: []const u8) WriteError!void {
    if (self.tls_state) |ts| {
        ts.tls_client.writer.writeAll(data) catch return WriteError.ConnectionClosed;
        ts.tls_client.writer.flush() catch return WriteError.ConnectionClosed;
    } else {
        var sent: usize = 0;
        while (sent < data.len) {
            const n = posix.send(self.socket, data[sent..], 0) catch return WriteError.ConnectionClosed;
            if (n == 0) return WriteError.ConnectionClosed;
            sent += n;
        }
    }
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

        const n = try self.recvBytes(self.read_buf[self.read_end..]);
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

        const n = try self.recvBytes(self.read_buf[self.read_end..]);
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
            try self.sendBytes(data);
            return;
        }
    }
    @memcpy(self.write_buf[self.write_pos .. self.write_pos + data.len], data);
    self.write_pos += data.len;
}

/// Flush the write buffer to the socket.
pub fn flush(self: *Connection) WriteError!void {
    if (self.write_pos == 0) return;
    try self.sendBytes(self.write_buf[0..self.write_pos]);
    self.write_pos = 0;
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
        const n = try self.recvBytes(buf[filled..len]);
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
        const n = try self.recvBytes(tmp[0 .. 2 - crlf_consumed]);
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

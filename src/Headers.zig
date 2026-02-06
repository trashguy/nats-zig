const std = @import("std");
const Allocator = std.mem.Allocator;

/// NATS message headers.
///
/// Format:
/// ```
/// NATS/1.0\r\n
/// Key: Value\r\n
/// Key: Value\r\n
/// \r\n
/// ```
const Headers = @This();

/// Stored entries as slices of key-value pairs.
entries: []Entry = &.{},
status: ?[]const u8 = null,
description: ?[]const u8 = null,

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

/// Get the first value for a key (case-insensitive).
pub fn get(self: *const Headers, key: []const u8) ?[]const u8 {
    for (self.entries) |entry| {
        if (std.ascii.eqlIgnoreCase(entry.key, key)) return entry.value;
    }
    return null;
}

/// Get all values for a key (case-insensitive).
pub fn getAll(self: *const Headers, allocator: Allocator, key: []const u8) ![]const []const u8 {
    var list: std.ArrayListUnmanaged([]const u8) = .{};
    for (self.entries) |entry| {
        if (std.ascii.eqlIgnoreCase(entry.key, key)) {
            try list.append(allocator, entry.value);
        }
    }
    return list.toOwnedSlice(allocator);
}

/// Parse NATS headers from raw bytes.
/// Expects format: "NATS/1.0[ status description]\r\nKey: Value\r\n...\r\n"
pub fn parse(allocator: Allocator, data: []const u8) !Headers {
    var headers = Headers{};
    var entries: std.ArrayListUnmanaged(Entry) = .{};

    // Find first \r\n — the status line
    const first_line_end = std.mem.indexOf(u8, data, "\r\n") orelse return error.InvalidHeaders;
    const status_line = data[0..first_line_end];

    // Parse status line: "NATS/1.0" or "NATS/1.0 503" or "NATS/1.0 503 No Responders"
    if (!std.mem.startsWith(u8, status_line, "NATS/1.0")) return error.InvalidHeaders;

    if (status_line.len > 9) { // "NATS/1.0 " + at least 1 char
        const rest = std.mem.trimLeft(u8, status_line[8..], " ");
        if (rest.len > 0) {
            if (std.mem.indexOf(u8, rest, " ")) |space_idx| {
                headers.status = try allocator.dupe(u8, rest[0..space_idx]);
                headers.description = try allocator.dupe(u8, rest[space_idx + 1 ..]);
            } else {
                headers.status = try allocator.dupe(u8, rest);
            }
        }
    }

    // Parse key-value pairs
    var pos = first_line_end + 2; // skip past first \r\n
    while (pos < data.len) {
        // Check for empty line (end of headers)
        if (pos + 1 < data.len and data[pos] == '\r' and data[pos + 1] == '\n') break;

        const line_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse break;
        const line = data[pos..line_end];
        pos = line_end + 2;

        // Parse "Key: Value"
        const colon_idx = std.mem.indexOf(u8, line, ":") orelse continue;
        const key = std.mem.trim(u8, line[0..colon_idx], " ");
        const value = std.mem.trim(u8, line[colon_idx + 1 ..], " ");

        try entries.append(allocator, .{
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, value),
        });
    }

    headers.entries = try entries.toOwnedSlice(allocator);
    return headers;
}

/// Serialize headers to wire format.
/// Returns the slice of bytes written.
pub fn serialize(self: *const Headers, buf: []u8) ![]const u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    // Status line
    try writer.writeAll("NATS/1.0");
    if (self.status) |status| {
        try writer.writeAll(" ");
        try writer.writeAll(status);
        if (self.description) |desc| {
            try writer.writeAll(" ");
            try writer.writeAll(desc);
        }
    }
    try writer.writeAll("\r\n");

    // Key-value pairs
    for (self.entries) |entry| {
        try writer.writeAll(entry.key);
        try writer.writeAll(": ");
        try writer.writeAll(entry.value);
        try writer.writeAll("\r\n");
    }

    // Terminating blank line
    try writer.writeAll("\r\n");

    return stream.getWritten();
}

/// Create headers from a slice of entries. Does not copy — caller owns memory.
pub fn fromEntries(entries: []Entry) Headers {
    return .{ .entries = entries };
}

// ── Tests ──

test "parse simple headers" {
    const data = "NATS/1.0\r\nX-Custom: hello\r\nContent-Type: text/plain\r\n\r\n";
    const hdrs = try parse(std.testing.allocator, data);
    defer {
        for (hdrs.entries) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        std.testing.allocator.free(hdrs.entries);
    }

    try std.testing.expect(hdrs.status == null);
    try std.testing.expect(hdrs.entries.len == 2);
    try std.testing.expectEqualStrings("hello", hdrs.get("X-Custom").?);
    try std.testing.expectEqualStrings("text/plain", hdrs.get("Content-Type").?);
}

test "parse headers with status" {
    const data = "NATS/1.0 503 No Responders\r\n\r\n";
    const hdrs = try parse(std.testing.allocator, data);
    defer {
        if (hdrs.status) |s| std.testing.allocator.free(s);
        if (hdrs.description) |d| std.testing.allocator.free(d);
        std.testing.allocator.free(hdrs.entries);
    }

    try std.testing.expectEqualStrings("503", hdrs.status.?);
    try std.testing.expectEqualStrings("No Responders", hdrs.description.?);
    try std.testing.expect(hdrs.entries.len == 0);
}

test "serialize headers" {
    var entries = [_]Entry{
        .{ .key = "X-Custom", .value = "hello" },
        .{ .key = "Content-Type", .value = "text/plain" },
    };
    const hdrs = Headers{ .entries = &entries };

    var buf: [256]u8 = undefined;
    const result = try hdrs.serialize(&buf);
    try std.testing.expectEqualStrings("NATS/1.0\r\nX-Custom: hello\r\nContent-Type: text/plain\r\n\r\n", result);
}

test "serialize headers with status" {
    const hdrs = Headers{
        .status = "503",
        .description = "No Responders",
        .entries = &.{},
    };

    var buf: [256]u8 = undefined;
    const result = try hdrs.serialize(&buf);
    try std.testing.expectEqualStrings("NATS/1.0 503 No Responders\r\n\r\n", result);
}

test "case-insensitive get" {
    var entries = [_]Entry{
        .{ .key = "Content-Type", .value = "text/plain" },
    };
    const hdrs = Headers{ .entries = &entries };
    try std.testing.expectEqualStrings("text/plain", hdrs.get("content-type").?);
    try std.testing.expectEqualStrings("text/plain", hdrs.get("CONTENT-TYPE").?);
    try std.testing.expect(hdrs.get("Missing") == null);
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const Protocol = @import("Protocol.zig");

/// Manages subscriptions: sid allocation, subject matching, message queuing.
const SubscriptionManager = @This();

pub const Subscription = struct {
    sid: u64,
    subject: []const u8,
    queue_group: ?[]const u8 = null,
    max_msgs: ?u32 = null,
    received: u32 = 0,
    pending: std.ArrayListUnmanaged(Protocol.Msg) = .{},

    /// Get next available message, or null if none queued.
    pub fn nextMsg(self: *Subscription) ?Protocol.Msg {
        if (self.pending.items.len == 0) return null;
        return self.pending.orderedRemove(0);
    }

    pub fn deinit(self: *Subscription, allocator: Allocator) void {
        // Free any pending messages
        for (self.pending.items) |*msg| {
            msg.deinit();
        }
        self.pending.deinit(allocator);
        allocator.free(self.subject);
        if (self.queue_group) |qg| allocator.free(qg);
    }
};

pub const SubscribeOpts = struct {
    queue_group: ?[]const u8 = null,
};

allocator: Allocator,
next_sid: u64 = 1,
subs: std.AutoHashMapUnmanaged(u64, *Subscription) = .{},

pub fn init(allocator: Allocator) SubscriptionManager {
    return .{ .allocator = allocator };
}

pub fn deinit(self: *SubscriptionManager) void {
    var it = self.subs.valueIterator();
    while (it.next()) |sub_ptr| {
        const sub = sub_ptr.*;
        sub.deinit(self.allocator);
        self.allocator.destroy(sub);
    }
    self.subs.deinit(self.allocator);
}

/// Create a new subscription, returning the subscription and its assigned SID.
pub fn addSubscription(self: *SubscriptionManager, subject: []const u8, opts: SubscribeOpts) !*Subscription {
    const sid = self.next_sid;
    self.next_sid += 1;

    const sub = try self.allocator.create(Subscription);
    sub.* = .{
        .sid = sid,
        .subject = try self.allocator.dupe(u8, subject),
        .queue_group = if (opts.queue_group) |qg| try self.allocator.dupe(u8, qg) else null,
    };

    try self.subs.put(self.allocator, sid, sub);
    return sub;
}

/// Remove a subscription by SID.
pub fn removeSubscription(self: *SubscriptionManager, sid: u64) void {
    if (self.subs.fetchRemove(sid)) |kv| {
        const sub = kv.value;
        sub.deinit(self.allocator);
        self.allocator.destroy(sub);
    }
}

/// Dispatch an incoming MSG to the matching subscription.
pub fn dispatch(self: *SubscriptionManager, msg: Protocol.Msg) !void {
    const sid_int = std.fmt.parseInt(u64, msg.sid, 10) catch {
        var m = msg;
        m.deinit();
        return;
    };

    if (self.subs.get(sid_int)) |sub| {
        sub.received += 1;

        // Check auto-unsub limit
        if (sub.max_msgs) |max| {
            if (sub.received > max) {
                // Already past limit, drop message and clean up
                var m = msg;
                m.deinit();
                self.removeSubscription(sid_int);
                return;
            }
        }

        try sub.pending.append(self.allocator, msg);

        // Remove subscription if we've hit the limit
        if (sub.max_msgs) |max| {
            if (sub.received >= max) {
                // Don't remove yet — let the consumer drain pending messages
            }
        }
    } else {
        // No matching subscription, drop the message
        var m = msg;
        m.deinit();
    }
}

/// Set auto-unsubscribe after N messages on a subscription.
pub fn autoUnsubscribe(self: *SubscriptionManager, sid: u64, max_msgs: u32) void {
    if (self.subs.get(sid)) |sub| {
        sub.max_msgs = max_msgs;
    }
}

/// Look up a subscription by SID.
pub fn getSub(self: *SubscriptionManager, sid: u64) ?*Subscription {
    return self.subs.get(sid);
}

/// Generate a unique inbox subject for request/reply.
pub fn newInbox(self: *SubscriptionManager) ![44]u8 {
    _ = self;
    var buf: [44]u8 = undefined;
    @memcpy(buf[0..7], "_INBOX.");

    // Generate a random suffix
    var rand_buf: [24]u8 = undefined;
    std.crypto.random.bytes(&rand_buf);
    _ = std.base64.url_safe_no_pad.Encoder.encode(buf[7..39], rand_buf[0..24]);

    return buf;
}

/// Get all active subscriptions (for re-subscribe on reconnect).
pub fn allSubscriptions(self: *SubscriptionManager) ![]*Subscription {
    var list = std.ArrayListUnmanaged(*Subscription){};
    var it = self.subs.valueIterator();
    while (it.next()) |sub_ptr| {
        try list.append(self.allocator, sub_ptr.*);
    }
    return list.toOwnedSlice(self.allocator);
}

// ── Tests ──

test "subscription add and remove" {
    var mgr = SubscriptionManager.init(std.testing.allocator);
    defer mgr.deinit();

    const sub = try mgr.addSubscription("test.subject", .{});
    try std.testing.expect(sub.sid == 1);
    try std.testing.expectEqualStrings("test.subject", sub.subject);

    const sub2 = try mgr.addSubscription("test.other", .{});
    try std.testing.expect(sub2.sid == 2);

    mgr.removeSubscription(1);
    try std.testing.expect(mgr.getSub(1) == null);
    try std.testing.expect(mgr.getSub(2) != null);
}

test "subscription with queue group" {
    var mgr = SubscriptionManager.init(std.testing.allocator);
    defer mgr.deinit();

    const sub = try mgr.addSubscription("test.subject", .{ .queue_group = "workers" });
    try std.testing.expectEqualStrings("workers", sub.queue_group.?);
}

test "dispatch message to subscription" {
    var mgr = SubscriptionManager.init(std.testing.allocator);
    defer mgr.deinit();

    const sub = try mgr.addSubscription("test.subject", .{});

    // Create a message with arena
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const aa = arena.allocator();
    const msg = Protocol.Msg{
        .subject = try aa.dupe(u8, "test.subject"),
        .sid = try aa.dupe(u8, "1"),
        .payload = try aa.dupe(u8, "hello"),
        .arena = arena,
    };
    try mgr.dispatch(msg);

    try std.testing.expect(sub.pending.items.len == 1);
    var received = sub.nextMsg().?;
    defer received.deinit();
    try std.testing.expectEqualStrings("hello", received.payload.?);
}

test "auto-unsubscribe" {
    var mgr = SubscriptionManager.init(std.testing.allocator);
    defer mgr.deinit();

    const sub = try mgr.addSubscription("test.subject", .{});
    mgr.autoUnsubscribe(sub.sid, 2);

    // Dispatch 2 messages
    for (0..2) |i| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        const aa = arena.allocator();
        var sid_buf: [8]u8 = undefined;
        const sid_str = std.fmt.bufPrint(&sid_buf, "{d}", .{sub.sid}) catch unreachable;
        const msg = Protocol.Msg{
            .subject = try aa.dupe(u8, "test.subject"),
            .sid = try aa.dupe(u8, sid_str),
            .payload = try aa.dupe(u8, if (i == 0) "first" else "second"),
            .arena = arena,
        };
        try mgr.dispatch(msg);
    }

    try std.testing.expect(sub.pending.items.len == 2);
    try std.testing.expect(sub.received == 2);
}

test "inbox generation" {
    var mgr = SubscriptionManager.init(std.testing.allocator);
    defer mgr.deinit();

    const inbox1 = try mgr.newInbox();
    const inbox2 = try mgr.newInbox();

    try std.testing.expect(std.mem.startsWith(u8, &inbox1, "_INBOX."));
    try std.testing.expect(std.mem.startsWith(u8, &inbox2, "_INBOX."));
    // Should be different (random)
    try std.testing.expect(!std.mem.eql(u8, &inbox1, &inbox2));
}

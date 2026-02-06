//! nats-zig: Pure Zig NATS client library.
//!
//! Provides a client for the NATS messaging system, supporting core pub/sub,
//! request/reply, JetStream, and KeyValue operations.
//!
//! ## Quick Start
//! ```zig
//! const nats = @import("nats");
//!
//! var client = try nats.Client.connect(allocator, .{
//!     .servers = &.{"nats://127.0.0.1:4222"},
//!     .name = "my-app",
//! });
//! defer client.close();
//!
//! // Publish
//! try client.publish("greet.joe", "hello");
//!
//! // Subscribe
//! var sub = try client.subscribe("greet.*", .{});
//! // ... poll for messages ...
//!
//! // JetStream
//! var js = client.jetStream();
//! _ = try js.createStream(.{ .name = "ORDERS", .subjects = &.{"orders.>"} });
//! _ = try js.publish("orders.new", "{\"id\":1}");
//! ```

pub const Client = @import("Client.zig");
pub const Protocol = @import("Protocol.zig");
pub const Connection = @import("Connection.zig");
pub const Headers = @import("Headers.zig");
pub const SubscriptionManager = @import("Subscription.zig");
pub const JetStream = @import("JetStream.zig");
pub const KeyValue = @import("KeyValue.zig");

// Re-export commonly used types
pub const Msg = Protocol.Msg;
pub const ServerInfo = Protocol.ServerInfo;
pub const ConnectOptions = Client.ConnectOptions;
pub const Subscription = SubscriptionManager.Subscription;
pub const SubscribeOpts = SubscriptionManager.SubscribeOpts;

// Pull in all tests from submodules
test {
    _ = Protocol;
    _ = Headers;
    _ = Connection;
    _ = SubscriptionManager;
    _ = Client;
    _ = JetStream;
    _ = KeyValue;
}

const std = @import("std");

test {
    _ = @import("mock_server.zig");
    _ = @import("protocol_test.zig");
    _ = @import("client_test.zig");
    _ = @import("jetstream_test.zig");
    _ = @import("kv_test.zig");
}

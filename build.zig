const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library module (for consumers)
    _ = b.addModule("nats", .{
        .root_source_file = b.path("src/nats.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Unit tests (src/ — inline tests in the library)
    const unit_test_module = b.createModule(.{
        .root_source_file = b.path("src/nats.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{
        .root_module = unit_test_module,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    // Nats module for integration tests to import
    const nats_module = b.createModule(.{
        .root_source_file = b.path("src/nats.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Integration tests (test/)
    const integration_test_module = b.createModule(.{
        .root_source_file = b.path("test/all_tests.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_test_module.addImport("nats", nats_module);
    const integration_tests = b.addTest(.{
        .root_module = integration_test_module,
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    // Test step — run integration after unit tests to avoid port conflicts
    run_integration_tests.step.dependOn(&run_unit_tests.step);
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_integration_tests.step);

    // Unit test only step
    const unit_test_step = b.step("test-unit", "Run unit tests only");
    unit_test_step.dependOn(&run_unit_tests.step);

    // Integration test only step
    const integration_test_step = b.step("test-integration", "Run integration tests only");
    integration_test_step.dependOn(&run_integration_tests.step);

}

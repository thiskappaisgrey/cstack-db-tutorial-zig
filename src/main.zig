const std = @import("std");

// Allocator..?
const Allocator = std.mem.Allocator;

const MetaCommandError = error{MetaCommandUnrecognizedCommand};
const PrepareError = error{PrepareUnRecognizedStatement};
const StatementTypes = enum { Insert, Select };

fn do_meta_command(c: []const u8) MetaCommandError!void {
    if (std.mem.eql(u8, c, ".exit")) {
        std.os.exit(0);
    } else {
        return MetaCommandError.MetaCommandUnrecognizedCommand;
    }
}

fn prepare_statement(c: []const u8) PrepareError!StatementTypes {
    if (std.mem.eql(u8, c[0..6], "insert")) {
        return StatementTypes.Insert;
    } else if (std.mem.eql(u8, c, "select")) {
        return StatementTypes.Select;
    } else {
        return PrepareError.PrepareUnRecognizedStatement;
    }
}
fn execute_statement(st: StatementTypes, logger: anytype) !void {
    switch (st) {
        StatementTypes.Select => {
            try logger.print("Statement Select Executed\n", .{});
        },
        StatementTypes.Insert => {
            try logger.print("Statement Insert Executed\n", .{});
        },
        // else => unreachable,
    }
}
pub fn main() !void {
    // Prints to stderr (it's a shortcut based on `std.io.getStdErr()`)
    // std.debug.print("Debuging..", .{});

    // stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.
    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    var stdout = bw.writer();

    const in = std.io.getStdIn().reader();
    var buf = std.io.bufferedReader(in);
    var stdin = buf.reader();

    // _ = try stdin.read
    while (true) {
        try stdout.print("sqlite>", .{});
        try bw.flush();
        var command_buf: [4096]u8 = undefined;
        var command = try stdin.readUntilDelimiterOrEof(&command_buf, '\n');
        if (command) |c| {
            if (c[0] == '.') {
                const e = do_meta_command(c);
                if (e == MetaCommandError.MetaCommandUnrecognizedCommand) {
                    try stdout.print("Unrecognized Command: {s}\n", .{c});
                }
            }
            var st = prepare_statement(c);
            if (st) |s| {
                try execute_statement(s, stdout);
            } else |err| switch (err) {
                PrepareError.PrepareUnRecognizedStatement => {
                    try stdout.print("Unrecognized Prepare Statement: {s}\n", .{c});
                },
            }
        }
        try bw.flush();
    }

    // try stdout.print("Run `zig build test` to run the tests.\n", .{});

    try bw.flush(); // don't forget to flush!
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

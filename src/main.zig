const std = @import("std");

// Allocator..?
const Allocator = std.mem.Allocator;

const MetaCommandError = error{MetaCommandUnrecognizedCommand};
const PrepareError = error{ PrepareUnRecognizedStatement, PrepareMissingArgs, PrepareParseIntErr, PrepareStringTooLong };
const ExecuteError = error{TableFull};
const username_size = 32 + 1;
const email_size = 255 + 1;

const Row = struct {
    id: u32,
    username: [username_size]u8,
    email: [email_size]u8,
};
const StatementTypes = union(enum) { Insert: Row, Select };
// Tagged Union
//n
// TODO: Represent a row - get the size by using comptime..
// Figure out how to get the size of a variable at comptime
const row_size = @sizeOf(Row);
// 4 Kilobytes pages
const page_size: u32 = 4096;
// 100 page arbritary limit..
const table_max_pages: u32 = 100;
const rows_per_page: u32 = page_size / row_size;
const table_max_rows = rows_per_page * table_max_pages;

// A "page" is essentially a block of memory on the heap of size page_size
const Page = []u8;
const page_allocator = std.heap.page_allocator;

/// The table holds pointers that we get from the os
const Table = struct {
    num_rows: u32,
    pages: [table_max_pages]?Page,
    allocator: Allocator,
    /// Create a table and initialize the memory with null on the heap
    fn new_table(alloc: Allocator) Allocator.Error!*Table {
        var table: *Table = try alloc.create(Table);
        table.num_rows = 0;
        table.allocator = alloc;
        table.pages = [_]?Page{null} ** table_max_pages;
        return table;
    }
    fn destroy(table: *Table) void {
        table.allocator.destroy(table);
    }
    // Allocate new memory
    fn row_slot(table: *Table, row_num: u32) ![]u8 {
        var page_num: u32 = row_num / rows_per_page;

        std.debug.print("page num: {d}\n", .{page_num});
        std.debug.print("row num: {d}\n", .{row_num});
        var page = table.pages[page_num];
        // page is a "nullable" type
        if (page == null) {
            var mem = try page_allocator.alloc(u8, page_size);
            table.pages[page_num] = mem;
            // not sure if I have to do this b/c they should point to the same chunk of memory..?
            page = table.pages[page_num];
            std.debug.print("alloc\n", .{});
        }

        std.debug.print("page len: {d}\n", .{page.?.len});
        var row_offset: u32 = row_num % rows_per_page;
        var byte_offset: u32 = row_offset * row_size;
        var page1 = page.?[byte_offset..(byte_offset + row_size)];
        std.debug.print("byte_offset is: {d}\n", .{byte_offset});
        std.debug.print("slot len: {d}\n", .{page1.len});
        return page1;
    }
    fn write_row(table: *Table, row_num: u32, row: Row) !void {
        var slot = try table.row_slot(row_num);
        var row_bytes = std.mem.asBytes(&row);
        @memcpy(slot, row_bytes);
    }
    fn read_row(table: *Table, row_num: u32) !*align(1) Row {
        var slot = try table.row_slot(row_num);
        var s: *[row_size]u8 = @as(*[row_size]u8, @ptrCast(slot.ptr));
        // var row_bytes: [292]u8 = slot[row_num..row_size];
        var my_row_ptr = std.mem.bytesAsValue(Row, s);
        // @compileLog(@TypeOf(my_row_ptr));
        return my_row_ptr;
    }
};

// The article compacts the struct to bytes (without wasted space & getting rid of alignment)
// but in this case - you only save 1 byte per "page", so idk if it's worth serializing the row..?
// maybe for learning purposes..?
//
//
fn do_meta_command(c: []const u8) MetaCommandError!void {
    if (std.mem.eql(u8, c, ".exit")) {
        std.os.exit(0);
    } else {
        return MetaCommandError.MetaCommandUnrecognizedCommand;
    }
}

fn prepare_statement(c: []const u8) !StatementTypes {
    if (c.len < 6) {
        return PrepareError.PrepareUnRecognizedStatement;
    }
    // This is a segfault when a command is under..
    if (std.mem.eql(u8, c[0..6], "insert")) {
        var s = std.mem.tokenizeAny(u8, c[6..], " ");
        // _ = s;
        // if(s)
        var id: u32 = undefined;
        if (s.next()) |u| {
            if (std.fmt.parseInt(u32, u, 10)) |i| {
                id = i;
            } else |_| {
                // _ = e;
                return PrepareError.PrepareParseIntErr;
            }
        }
        var username: [username_size]u8 = undefined;
        if (s.next()) |u| {
            // username = me;
            if (u.len > (username_size - 1)) {
                return PrepareError.PrepareStringTooLong;
            }
            std.mem.copy(u8, &username, u);
            // manually write the null terminator..?
            username[u.len] = 0;
        } else {
            return PrepareError.PrepareMissingArgs;
        }
        var email: [email_size]u8 = undefined;

        if (s.next()) |u| {
            // username = me;
            if (u.len > (email_size - 1)) {
                return PrepareError.PrepareStringTooLong;
            }

            std.mem.copy(u8, &email, u);
            email[u.len] = 0;
        } else {
            return PrepareError.PrepareMissingArgs;
        }

        return StatementTypes{ .Insert = .{ .id = id, .username = username, .email = email } };
    } else if (std.mem.eql(u8, c, "select")) {
        return StatementTypes.Select;
    } else {
        return PrepareError.PrepareUnRecognizedStatement;
    }
}
fn execute_statement(st: StatementTypes, logger: anytype, table: *Table) !void {
    switch (st) {
        StatementTypes.Select => {
            try execute_select(table, logger);
        },
        StatementTypes.Insert => |row| {
            if (execute_insert(row, table, logger)) {} else |err| {
                switch (err) {
                    ExecuteError.TableFull => try logger.print("Could not insert into table\n", .{}),
                    else => try logger.print("Some other error with insertion\n", .{}),
                }
            }
        },
        // else => unreachable,
    }
}

fn execute_insert(row: Row, table: *Table, logger: anytype) !void {
    if (table.num_rows >= table_max_rows) {
        return ExecuteError.TableFull;
    }
    try logger.print("Executed.\n", .{});
    // try print_row(row, logger);
    try table.write_row(table.num_rows, row);
    // var bytes: [*]u8 = std.mem.asBytes(&row);

    table.num_rows += 1;
}
fn print_row(r: anytype, logger: anytype) !void {
    // Need to cast into a null-terminated string first before
    // printing b/c by defautl - this would print the entire buffer..
    var username: [*:0]const u8 = @ptrCast(&r.username);
    var email: [*:0]const u8 = @ptrCast(&r.email);
    try logger.print("({d}, {s}, {s})\n", .{ r.id, username, email });
}
fn execute_select(table: *Table, logger: anytype) !void {
    for (0..table.num_rows) |i| {
        var row = try table.read_row(@intCast(i));
        try print_row(row, logger);
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
    var table = try Table.new_table(std.heap.page_allocator);
    defer table.destroy();

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
                try execute_statement(s, stdout, table);
            } else |err| switch (err) {
                PrepareError.PrepareUnRecognizedStatement => {
                    try stdout.print("Unrecognized Prepare Statement: {s}\n", .{c});
                },
                PrepareError.PrepareMissingArgs => {
                    try stdout.print("Insert is missing arguments\n", .{});
                },
                PrepareError.PrepareParseIntErr => {
                    try stdout.print("Could not parse int argument\n", .{});
                },

                PrepareError.PrepareStringTooLong => {
                    try stdout.print("String is too long\n", .{});
                },
                // else => {
                //     try stdout.print("Could not parse prepare statement\n", .{});
                // },
            }
        }
        try bw.flush();
    }

    // try stdout.print("Run `zig build test` to run the tests.\n", .{});

    try bw.flush(); // don't forget to flush!
}

test "simple test" {
    var table = try Table.new_table(std.testing.allocator);
    defer table.destroy(); // try commenting this out and see if zig detects the memory leak!
    for (table.pages) |page| {
        try std.testing.expectEqual(page, @as(?Page, null));
    }

    // try indexing into row 0..?
    // var page = try table.row_slot(0);
    // @compileLog("type of page is: ", @TypeOf(page));
    // TODO: Make a row constructor that takes a string literal and copies it to the array
    var usrname_buffer: [32]u8 = [_]u8{0} ** 32;
    var email_buffer: [email_size]u8 = [_]u8{0} ** email_size;
    var username = "hello";
    usrname_buffer[0..username.len].* = username.*;

    var email = "world@email.com";
    email_buffer[0..email.len].* = email.*;

    var my_row: Row = Row{ .id = 1, .username = usrname_buffer, .email = email_buffer };
    try table.write_row(0, my_row);
    var my_row_ptr = try table.read_row(0);
    // var my_row_bytes: []u8 = std.mem.asBytes(&my_row);
    // @memcpy(page, my_row_bytes);
    // var pg_0 = table.pages[0].?[0..row_size];
    // var my_row_ptr = std.mem.bytesAsValue(Row, pg_0);
    try std.testing.expectEqual(my_row_ptr.username, my_row.username);
    try std.testing.expectEqual(my_row_ptr.id, my_row.id);
    try std.testing.expectEqual(my_row_ptr.email, my_row.email);
    // std.debug.print("Email is: {s}", .{my_row_ptr.email});
}

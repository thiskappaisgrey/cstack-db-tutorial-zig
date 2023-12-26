const std = @import("std");

// Allocator..?
const Allocator = std.mem.Allocator;

const MetaCommandError = error{MetaCommandUnrecognizedCommand};
const PrepareError = error{ PrepareUnRecognizedStatement, InsertMissingArgs };
const ExecuteError = error{TableFull};
const username_size = 32;
const email_size = 255;
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
        var page = table.pages[page_num];
        // page is a "nullable" type
        if (page == null) {
            var mem = try page_allocator.alloc(u8, page_size);
            table.pages[page_num] = mem;
            // not sure if I have to do this b/c they should point to the same chunk of memory..?
            page = table.pages[page_num];
        }

        var row_offset: u32 = row_num % rows_per_page;
        var byte_offset: u32 = row_offset % row_size;
        var page1 = page.?;

        return page1[byte_offset..row_size];
    }
    fn write_row(table: *Table, row_num: u32, row: Row) !void {
        var slot = try table.row_slot(row_num);
        var row_bytes = std.mem.asBytes(&row);
        @memcpy(slot, row_bytes);
    }
    fn read_row(table: *Table, row_num: u32) !*align(1) Row {
        var slot = try table.row_slot(row_num);
        var s: *[292]u8 = @as(*[292]u8, @ptrCast(slot.ptr));
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
    if (std.mem.eql(u8, c[0..6], "insert")) {
        var s = std.mem.tokenizeAny(u8, c[6..], " ");
        // _ = s;
        // if(s)
        var id: u32 = undefined;
        if (s.next()) |u| {
            var i = try std.fmt.parseInt(u32, u, 10);
            id = i;
            // std.mem.copy(u8, )
        }
        var username: [username_size]u8 = undefined;
        if (s.next()) |u| {
            // username = me;
            std.mem.copy(u8, &username, u);
        } else {
            return PrepareError.InsertMissingArgs;
        }
        var email: [email_size]u8 = undefined;

        if (s.next()) |u| {
            // username = me;
            std.mem.copy(u8, &email, u);
        } else {
            return PrepareError.InsertMissingArgs;
        }
        // if (email == null) {
        //     return PrepareError.InsertMissingArgs;
        // }

        // TODO: Intialize memory!
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
            try logger.print("Statement Select Executed\n", .{});
        },
        StatementTypes.Insert => |row| {
            try execute_insert(row, table, logger);
            try logger.print("Statement Insert Executed\n", .{});
        },
        // else => unreachable,
    }
}

fn execute_insert(row: Row, table: *Table, logger: anytype) !void {
    if (table.num_rows >= table_max_rows) {
        return ExecuteError.TableFull;
    }
    try logger.print("Writing row: ", .{});
    try print_row(row, logger);
    try table.write_row(table.num_rows, row);
    // var bytes: [*]u8 = std.mem.asBytes(&row);

    table.num_rows += 1;
}
fn print_row(r: anytype, logger: anytype) !void {
    try logger.print("Row - {d}, {s}, {s}\n", .{ r.id, r.username, r.email });
}
fn execute_select(table: *Table, logger: anytype) !void {
    for (0..table.num_rows) |i| {
        var row = try table.read_row(@intCast(i));
        try logger.print("Read row: ", .{});
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
                PrepareError.InsertMissingArgs => {
                    try stdout.print("Insert is missing arguments\n", .{});
                },
                else => {
                    try stdout.print("Could not parse prepare statement", .{});
                },
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

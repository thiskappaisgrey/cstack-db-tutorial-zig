const std = @import("std");
// const util = @import("util.zig");

// Allocator..?
const Allocator = std.mem.Allocator;

const MetaCommandError = error{MetaCommandUnrecognizedCommand};
const PrepareError = error{ PrepareUnRecognizedStatement, PrepareMissingArgs, PrepareParseIntErr, PrepareStringTooLong };
const ExecuteError = error{TableFull};
const PageError = error{ OutOfBounds, PageUnitialized };
const username_size = 32 + 1;
const email_size = 255 + 1;

const Row = struct {
    id: u32,
    username: [username_size]u8,
    email: [email_size]u8,
};
const StatementTypes = union(enum) { Insert: Row, Select };
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
    pager: Pager,
    allocator: Allocator,
    /// Create a table and initialize the memory with null on the heap
    fn init(alloc: Allocator, fd: std.fs.File) !*Table {
        var table: *Table = try alloc.create(Table);
        table.allocator = alloc;
        table.pager = Pager.init(fd, alloc);
        var file_size: u32 = @intCast(try fd.getEndPos());
        std.debug.print("File size is: {d}\n", .{file_size});
        table.num_rows = file_size / row_size;
        return table;
    }
    /// Destroy also needs to dellocate all of the memory in the pages as well..
    fn deinit(self: *Table) void {
        var full_pages = self.num_rows / rows_per_page;
        // TODO: this really should be part of the pager's deinit..
        // TODO: Also - I don't really have to free memory as long as I use the Arena allocator haha.
        for (0..full_pages) |i| {
            if (self.pager.pages[i]) |p| {
                self.pager.flush(i, page_size) catch {
                    std.debug.print("Couldn't flush page..", .{});
                };
                self.allocator.free(p);
                self.pager.pages[i] = null;
            }
        }
        var add_rows = self.num_rows % rows_per_page;
        std.debug.print("Additional rows: {d}", .{add_rows});
        if (add_rows > 0) {
            var page_num = full_pages;
            if (self.pager.pages[page_num]) |p| {
                // TODO: on a partial page.. the entire page is flushed which is bad..
                self.pager.flush(page_num, add_rows * row_size) catch {
                    std.debug.print("Couldn't flush page..", .{});
                };
                self.allocator.free(p);
                self.pager.pages[page_num] = null;
            }
        }

        self.pager.fd.close();
        self.allocator.destroy(self);
    }
};

const Pager = struct {
    fd: std.fs.File,
    pages: [table_max_pages]?Page,
    allocator: Allocator,
    /// Create a pager with pages initialzed to the null pointer
    fn init(fd: std.fs.File, allocator: Allocator) Pager {
        var pages = [_]?Page{null} ** table_max_pages;
        return Pager{ .fd = fd, .pages = pages, .allocator = allocator };
    }

    fn get_page(self: *Pager, page_num: u32) ![]u8 {
        if (page_num > table_max_pages) {
            return PageError.OutOfBounds;
        }
        var page: []u8 = undefined;
        if (self.pages[page_num]) |p| {
            std.debug.print("Cache hit, reading {d} from cache", .{page_num});
            return p;
        }
        // cache miss - allocate memory and load from file
        std.debug.print("Cache miss, reading {d} from file", .{page_num});
        page = try self.allocator.alloc(u8, page_size);
        @memset(page, 0);
        // 0 out the memory
        // TODO: I think getEndPos should give me the file size, but I can stat the file as well?
        var filelen = try self.fd.getEndPos();
        var num_pages: u64 = filelen / page_size;
        // partial page at the end of a file
        if (filelen % page_size != 0) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            // seek the file to the start of the page
            try self.fd.seekTo(page_num * page_size);
            var size_read = try self.fd.read(page);
            if (size_read < page_size) {
                std.debug.print("Read partial page: {d}", .{size_read});
            }
        }
        self.pages[page_num] = page;
        return page;
    }
    // Instead of flushing the entire page, I also need to be able to
    // flush part of a page..
    fn flush(self: *Pager, page_num: usize, offset: u32) !void {
        std.debug.print("Flushing db", .{});
        if (self.pages[page_num]) |p| {
            try self.fd.seekTo(page_num * page_size);
            var s = try self.fd.write(p[0..offset]);
            std.debug.print("Wrote {d} bytes to file", .{s});
        }
    }
};

/// A cursor represent a location in the table
const Cursor = struct {
    table: *Table,
    /// the row number in the table
    row_num: u32,
    /// a position one past the last element
    /// Where we might want to insert a row
    end_of_table: bool,
    fn table_start(table: *Table) Cursor {
        return Cursor{ .table = table, .row_num = 0, .end_of_table = table.num_rows == 0 };
    }
    fn table_end(table: *Table) Cursor {
        return Cursor{ .table = table, .row_num = table.num_rows, .end_of_table = true };
    }
    fn cursor_value(self: *Cursor) ![]u8 {
        var row_num = self.row_num;
        var page_num: u32 = row_num / rows_per_page;
        var page = try self.table.pager.get_page(page_num);

        var row_offset: u32 = row_num % rows_per_page;
        var byte_offset: u32 = row_offset * row_size;
        var row = page[byte_offset..(byte_offset + row_size)];
        std.debug.print("Reading cursor {d}\n", .{row_num});
        return row;
    }

    fn advance(self: *Cursor) void {
        self.row_num += 1;
        if (self.row_num >= self.table.num_rows) {
            self.end_of_table = true;
        }
    }
    fn write(self: *Cursor, row: Row) !void {
        var slot = try self.cursor_value();
        var row_bytes = std.mem.asBytes(&row);
        @memcpy(slot, row_bytes);
    }
    fn read(self: *Cursor) !*align(1) Row {
        var slot = try self.cursor_value();
        var s: *[row_size]u8 = @as(*[row_size]u8, @ptrCast(slot.ptr));
        var my_row_ptr = std.mem.bytesAsValue(Row, s);
        return my_row_ptr;
    }
};

// The article compacts the struct to bytes (without wasted space & getting rid of alignment)
// but I'm too lazy to do that
fn do_meta_command(c: []const u8, table: *Table) MetaCommandError!void {
    if (std.mem.eql(u8, c, ".exit")) {
        table.deinit();
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
        var username: [username_size]u8 = [_]u8{0} ** username_size;
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
        var email: [email_size]u8 = [_]u8{0} ** email_size;

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
    var cursor: Cursor = Cursor.table_end(table);
    try cursor.write(row);
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
    std.debug.print("Table num rows is: {d}", .{table.num_rows});
    var cursor = Cursor.table_start(table);
    while (!cursor.end_of_table) {
        var row = try cursor.read();
        try print_row(row, logger);
        cursor.advance();
    }
    // for (0..table.num_rows) |i| {
    // }
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
    var db_filename = "dbfile.db";
    // var db_file = try util.readFileOrCreate(db_filename);
    var db_file = try std.fs.cwd().createFile(db_filename, .{ .read = true, .truncate = false });

    var table = try Table.init(std.heap.page_allocator, db_file);
    // std.debug.print("Opening file: {s}, with length: {d}\n", .{ db_filename, db_file.getEndPos() });

    defer table.deinit();

    // _ = try stdin.read
    while (true) {
        try stdout.print("sqlite>", .{});
        try bw.flush();
        var command_buf: [4096]u8 = undefined;
        var command = try stdin.readUntilDelimiterOrEof(&command_buf, '\n');
        if (command) |c| {
            if (c[0] == '.') {
                const e = do_meta_command(c, table);
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

test "Simple table test" {
    var table = try Table.init(std.testing.allocator);
    defer table.deinit(); // try commenting this out and see if zig detects the memory leak!
    for (table.pages) |page| {
        try std.testing.expectEqual(page, @as(?Page, null));
    }

    // try indexing into row 0..?
    // var page = try table.row_slot(0);
    // @compileLog("type of page is: ", @TypeOf(page));
    // TODO: Make a row constructor that takes a string literal and copies it to the array
    var usrname_buffer: [username_size]u8 = [_]u8{0} ** username_size;
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
// TODO: Assert that inserting to the table doesn't leak memory..?
// test ""

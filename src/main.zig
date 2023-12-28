const std = @import("std");
// const util = @import("util.zig");
const node = @import("node.zig");

// Allocator..?
const Allocator = std.mem.Allocator;

const MetaCommandError = error{MetaCommandUnrecognizedCommand};
const PrepareError = error{ PrepareUnRecognizedStatement, PrepareMissingArgs, PrepareParseIntErr, PrepareStringTooLong };
const ExecuteError = error{TableFull};
const PageError = error{ OutOfBounds, PageUnitialized, CorruptFile };
const username_size = 32 + 1;
const email_size = 255 + 1;

pub const Row = struct {
    id: u32,
    username: [username_size]u8,
    email: [email_size]u8,
};
const StatementTypes = union(enum) { Insert: Row, Select };
pub const row_size = @sizeOf(Row);
// 4 Kilobytes pages
pub const page_size: u32 = 4096;
// 100 page arbritary limit..
const table_max_pages: u32 = 100;

// A "page" is essentially a block of memory on the heap of size page_size
// The page size is constant - but I store it as slice b/c they are easier to work with...
pub const Page = []u8;
const page_allocator = std.heap.page_allocator;

/// The table holds pointers that we get from the os
const Table = struct {
    pager: Pager,
    root_page_num: u32,
    allocator: Allocator,
    /// Create a table and initialize the memory with null on the heap
    fn init(alloc: Allocator, fd: std.fs.File) !*Table {
        var table: *Table = try alloc.create(Table);
        table.allocator = alloc;
        table.pager = try Pager.init(fd, alloc);
        table.root_page_num = 0;
        if (table.pager.num_pages == 0) {
            var root = try table.pager.get_page(table.root_page_num);
            var n = try node.LeafNode.deserialize(root);
            n.init();
        }
        return table;
    }
    /// Destroy also needs to dellocate all of the memory in the pages as well..
    fn deinit(self: *Table) void {
        // TODO: Flushing really should be part of the pager's deinit..

        std.debug.print("Num pages: {d}\n", .{self.pager.num_pages});
        for (0..self.pager.num_pages) |i| {
            if (self.pager.pages[i]) |p| {
                std.debug.print("Flushing page: {d}\n", .{i});
                self.pager.flush(i) catch {
                    std.debug.print("Couldn't flush page..", .{});
                };
                self.allocator.free(p);
                self.pager.pages[i] = null;
            }
        }
        self.pager.fd.close();
        self.allocator.destroy(self);
    }
};

const Pager = struct {
    fd: std.fs.File,
    pages: [table_max_pages]?Page,
    num_pages: u32,
    allocator: Allocator,
    /// Create a pager with pages initialzed to the null pointer
    fn init(fd: std.fs.File, allocator: Allocator) !Pager {
        var pages = [_]?Page{null} ** table_max_pages;
        var filelen: u32 = @intCast(try fd.getEndPos());
        var num_pages = filelen / page_size;
        return Pager{ .fd = fd, .pages = pages, .num_pages = @intCast(num_pages), .allocator = allocator };
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
        var filelen: u32 = @intCast(try self.fd.getEndPos());
        self.num_pages = filelen / page_size;
        // partial page at the end of a file
        if (filelen % page_size != 0) {
            return PageError.CorruptFile;
        }

        if (page_num <= self.num_pages) {
            // seek the file to the start of the page
            try self.fd.seekTo(page_num * page_size);
            var size_read = try self.fd.read(page);
            if (size_read < page_size) {
                std.debug.print("Read partial page: {d}", .{size_read});
            }
        }

        if (page_num >= self.num_pages) {
            self.num_pages = page_num + 1;
        }
        self.pages[page_num] = page;
        return page;
    }
    fn unsafe_get_node(self: *Pager, page_num: u32) *node.LeafNode {
        var p = self.get_page(page_num) catch unreachable;
        var n = node.LeafNode.deserialize(p) catch unreachable;
        return n;
    }
    /// Flush the entire page to the file
    fn flush(self: *Pager, page_num: usize) !void {
        std.debug.print("Flushing db", .{});
        if (self.pages[page_num]) |p| {
            try self.fd.seekTo(page_num * page_size);
            var s = try self.fd.write(p);
            std.debug.print("Wrote {d} bytes to file", .{s});
        }
    }
};

/// A cursor represent a location in the table
const Cursor = struct {
    table: *Table,
    /// the row number in the table
    page_num: u32,
    cell_num: u32,
    /// a position one past the last element
    /// Where we might want to insert a row
    end_of_table: bool,
    fn table_start(table: *Table) !Cursor {
        var root = try table.pager.get_page(table.root_page_num);
        var n = try node.LeafNode.deserialize(root);
        var end_of_table = n.common.num_cells == 0;

        // var num_cells = node.
        return Cursor{ .table = table, .page_num = table.root_page_num, .cell_num = 0, .end_of_table = end_of_table };
    }
    fn table_end(table: *Table) !Cursor {
        var root = try table.pager.get_page(table.root_page_num);
        var n = try node.LeafNode.deserialize(root);
        var num_cells = n.common.num_cells;

        return Cursor{ .table = table, .page_num = table.root_page_num, .cell_num = num_cells, .end_of_table = true };
    }
    fn value(self: *Cursor) !Row {
        var page_num = self.page_num;
        var page = try self.table.pager.get_page(page_num);
        var n = try node.LeafNode.deserialize(page);
        var row = n.cells[self.cell_num];
        return row.value;
    }

    fn advance(self: *Cursor) !void {
        var page_num = self.page_num;
        var page = try self.table.pager.get_page(page_num);
        var n = try node.LeafNode.deserialize(page);
        self.cell_num += 1;
        if (self.cell_num >= n.common.num_cells) {
            self.end_of_table = true;
        }
    }
    fn leaf_node_insert(self: *Cursor, key: u32, val: Row) !void {
        var page = try self.table.pager.get_page(self.page_num);
        var n = try node.LeafNode.deserialize(page);
        var num_cells = n.common.num_cells;
        if (num_cells >= node.leaf_max_cells) {
            std.debug.print("Need to implmenet splitting", .{});
            std.os.exit(1);
            // return ExecuteError.TableFull;
        }
        if (self.cell_num < num_cells) {
            var arr = n.cells;
            var i = num_cells;
            while (i > self.cell_num) {
                arr[i] = arr[i - 1];
            }
        }
        n.common.num_cells += 1;
        n.cells[self.cell_num].key = key;
        n.cells[self.cell_num].value = val;
        // try n.serialize(page);
    }
};
fn print_leaf_node(n: *node.LeafNode, logger: anytype) void {
    logger.print("leaf (size {d})\n", .{n.common.num_cells}) catch unreachable;
    for (0..n.common.num_cells) |i| {
        logger.print("  - {d} : {d}\n", .{ i, n.cells[i].key }) catch unreachable;
    }
}

// The article compacts the struct to bytes (without wasted space & getting rid of alignment)
// but I'm too lazy to do that
fn do_meta_command(c: []const u8, table: *Table, logger: anytype) MetaCommandError!void {
    if (std.mem.eql(u8, c, ".exit")) {
        table.deinit();
        std.os.exit(0);
    } else if (std.mem.eql(u8, c, ".btree")) {
        logger.print("Tree:\n", .{}) catch unreachable;
        var n = table.pager.unsafe_get_node(0);
        print_leaf_node(n, logger);
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
    var page = try table.pager.get_page(table.root_page_num);
    var n = try node.LeafNode.deserialize(page);
    if (n.common.num_cells >= node.leaf_max_cells) {
        return ExecuteError.TableFull;
    }
    try logger.print("Executed.\n", .{});
    var cursor: Cursor = try Cursor.table_end(table);
    try cursor.leaf_node_insert(row.id, row);
}
fn print_row(r: anytype, logger: anytype) !void {
    // Need to cast into a null-terminated string first before
    // printing b/c by defautl - this would print the entire buffer..
    // var username: [*:0]const u8 = @ptrCast(&r.username);
    // var email: [*:0]const u8 = @ptrCast(&r.email);
    try logger.print("({d}, {s}, {s})\n", .{ r.id, r.username, r.email });
}
fn execute_select(table: *Table, logger: anytype) !void {
    // std.debug.print("Table num rows is: {d}", .{table.num_rows});
    var cursor = try Cursor.table_start(table);
    while (!cursor.end_of_table) {
        var row = try cursor.value();
        try print_row(row, logger);
        try cursor.advance();
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
                const e = do_meta_command(c, table, stdout);
                if (e == MetaCommandError.MetaCommandUnrecognizedCommand) {
                    try stdout.print("Unrecognized Command: {s}\n", .{c});
                }
                continue;
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

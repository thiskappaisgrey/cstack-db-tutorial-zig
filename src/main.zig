const std = @import("std");
// const util = @import("util.zig");
const node = @import("node.zig");

// Allocator..?
const Allocator = std.mem.Allocator;

const MetaCommandError = error{MetaCommandUnrecognizedCommand};
const PrepareError = error{ PrepareUnRecognizedStatement, PrepareMissingArgs, PrepareParseIntErr, PrepareStringTooLong };
const ExecuteError = error{ TableFull, DuplicateKey };
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
            var n = try node.Node.deserialize(root);
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
    fn find(self: *Table, key: u32) !Cursor {
        // var n = try self.pager.get_node(self.root_page_num);
        // TODO: Add code for handling internal nodes - and change
        // the "LeafNode" to a generic node type..
        return self.leaf_node_find(self.root_page_num, key);
    }
    /// This function will return
    /// The position of the key
    /// The position of another key that has to be moved
    /// The position one past the last key
    fn leaf_node_find(self: *Table, page_num: u32, key: u32) !Cursor {
        // _ = key;
        var n = try self.pager.get_node(page_num);
        switch (n.*) {
            node.NodeType.leaf => |l| {
                var min: u32 = 0;
                var one_past_max = l.common.num_cells;
                var cursor: Cursor = Cursor{ .table = self, .page_num = page_num, .cell_num = undefined, .end_of_table = false };
                while (one_past_max != min) {
                    var i = (min + one_past_max) / 2;
                    var key_i: u32 = l.cells[i].key;
                    if (key == key_i) {
                        // cell_num = i;
                        cursor.cell_num = i;
                        return cursor;
                    }
                    if (key < key_i) {
                        one_past_max = i;
                    } else {
                        min = i + 1;
                    }
                }

                cursor.cell_num = min;
                return cursor;
            },
            node.NodeType.internal => {
                std.debug.panic("Leaf node find called on an internal node..", .{});
            },
        }
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
            std.debug.print("Cache hit, reading {d} from cache\n", .{page_num});
            return p;
        }
        // cache miss - allocate memory and load from file
        std.debug.print("Cache miss, reading {d} from file\n", .{page_num});
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
    fn get_node(self: *Pager, page_num: u32) !*node.Node {
        var p = try self.get_page(page_num);
        var n = try node.Node.deserialize(p);
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
        var n = try node.Node.deserialize(root);
        var end_of_table = n.common().num_cells == 0;

        // var num_cells = node.
        return Cursor{ .table = table, .page_num = table.root_page_num, .cell_num = 0, .end_of_table = end_of_table };
    }
    fn table_end(table: *Table) !Cursor {
        var root = try table.pager.get_page(table.root_page_num);
        var n = try node.Node.deserialize(root);
        var num_cells = n.common().num_cells;

        return Cursor{ .table = table, .page_num = table.root_page_num, .cell_num = num_cells, .end_of_table = true };
    }
    fn value(self: *Cursor) !Row {
        var page_num = self.page_num;
        var page = try self.table.pager.get_page(page_num);
        var n = try node.Node.deserialize(page);
        // on a row - do stuff
        switch (n.*) {
            node.NodeType.leaf => |l| {
                var row = l.cells[self.cell_num];
                return row.value;
            },
            else => {
                std.debug.panic("Calling value on an internal node is unimpl", .{});
            },
        }
    }

    fn advance(self: *Cursor) !void {
        var page_num = self.page_num;
        var page = try self.table.pager.get_page(page_num);
        var n = try node.Node.deserialize(page);
        self.cell_num += 1;
        if (self.cell_num >= n.common().num_cells) {
            self.end_of_table = true;
        }
    }
    fn leaf_node_insert(self: *Cursor, key: u32, val: Row) !void {
        var page = try self.table.pager.get_page(self.page_num);
        var n = try node.Node.deserialize(page);
        switch (n.*) {
            node.NodeType.leaf => |*l| {
                var num_cells = l.common.num_cells;
                if (num_cells >= node.leaf_max_cells) {
                    std.debug.print("Need to implmenet splitting", .{});
                    std.os.exit(1);
                    // return ExecuteError.TableFull;
                }

                if (self.cell_num < num_cells) {
                    var i = num_cells;
                    while (i > self.cell_num) {
                        l.cells[i] = l.cells[i - 1];
                        i = i - 1;
                    }
                }
                l.common.num_cells += 1;
                l.cells[self.cell_num].key = key;
                l.cells[self.cell_num].value = val;
            },
            node.NodeType.internal => {
                std.debug.panic("leaf_node_insert called on internal node", .{});
            },
        }
        // try n.serialize(page);
    }
};
fn print_leaf_node(n: *node.Node, logger: anytype) void {
    switch (n.*) {
        node.NodeType.leaf => |l| {
            logger.print("leaf (size {d})\n", .{l.common.num_cells}) catch unreachable;
            for (0..n.common().num_cells) |i| {
                logger.print("  - {d} : {d}\n", .{ i, l.cells[i].key }) catch unreachable;
            }
        },
        else => {
            std.debug.panic("trying to print non-leaf node", .{});
        },
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
        var n = table.pager.get_node(0) catch unreachable;
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
                    ExecuteError.TableFull => try logger.print("Could not insert. Error: Table Full\n", .{}),
                    ExecuteError.DuplicateKey => try logger.print("Could not insert. Error: Duplicate Key\n", .{}),
                    else => try logger.print("Some other error with insertion\n", .{}),
                }
            }
        },
        // else => unreachable,
    }
}

fn execute_insert(row: Row, table: *Table, logger: anytype) !void {
    // Instead of
    var page = try table.pager.get_page(table.root_page_num);
    var n = try node.Node.deserialize(page);
    switch (n.*) {
        node.NodeType.leaf => |l| {
            var num_cells = l.common.num_cells;
            if (num_cells >= node.leaf_max_cells) {
                return ExecuteError.TableFull;
            }
            var key: u32 = row.id;
            var cursor: Cursor = try table.find(key);
            // Check if key is duplicate
            if (cursor.cell_num < num_cells) {
                var key_i = l.cells[cursor.cell_num].key;
                if (key == key_i) {
                    return ExecuteError.DuplicateKey;
                }
            }

            // var cursor: Cursor = try Cursor.table_end(table);
            try logger.print("Executed.\n", .{});
            try cursor.leaf_node_insert(row.id, row);
        },
        node.NodeType.internal => {
            std.debug.panic("unimpl", .{});
        },
    }
}
fn print_row(r: anytype, logger: anytype) !void {
    // Need to cast into a null-terminated string first before
    // printing b/c by defautl - this would print the entire buffer..
    var username: [*:0]const u8 = @ptrCast(&r.username);
    var email: [*:0]const u8 = @ptrCast(&r.email);
    try logger.print("({d}, {s}, {s})\n", .{ r.id, username, email });
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

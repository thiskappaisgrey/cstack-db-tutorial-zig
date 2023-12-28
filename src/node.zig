const std = @import("std");
const main = @import("main.zig");
const page_size = main.page_size;
const row_size = main.row_size;
const Row = main.Row;
// const Page = main.
const NodeType = enum(u8) { internal, leaf };
// Structs on the stack have to be divisible by 4
// but if you want a compact representation to store in a file
// you'd have to pack manually - and you can't get packing "for free"..
const NodeCommonHeader = struct { num_cells: u32, is_root: bool };
const LeafNodeHeader = struct { parent_pointer: u32 };
const Page = main.Page;

// TODO: Define better packing for this..
const leaf_node_header_size = @sizeOf(LeafNodeHeader) + @sizeOf(NodeCommonHeader);
const cell_space = page_size - leaf_node_header_size;
const LeafCell = struct {
    key: u32,
    value: Row,
};
const cell_size = @sizeOf(LeafCell);
pub const leaf_max_cells = cell_space / cell_size;

// const LeafNode
// The node_type will be encoded in the tagged union

const node_size = @sizeOf(LeafNode);
pub const LeafNode = struct {
    common: NodeCommonHeader,
    header: LeafNodeHeader,
    cells: [leaf_max_cells]LeafCell,
    pub fn serialize(self: *LeafNode, page: Page) !void {
        var row_bytes = std.mem.asBytes(self);
        @memcpy(page, row_bytes);
    }
    pub fn deserialize(page: Page) !*LeafNode {
        var s: *[node_size]u8 = @as(*[node_size]u8, @ptrCast(page.ptr));
        var leaf_node_ptr = std.mem.bytesAsValue(LeafNode, s);
        return @alignCast(leaf_node_ptr);
    }
    pub fn init(self: *LeafNode) void {
        self.common.num_cells = 0;
    }
};
// const Node = union(NodeType) { internal: u8, leaf: void };
test "Node test" {
    // In the node test, we log the sizes of the node
    // There's more "bloat" than the article but it's not too bad..
    var alloc = std.testing.allocator;
    var mem = try alloc.alloc(u8, page_size);
    @memset(mem, 0);
    defer alloc.free(mem);
    var node: *LeafNode = try LeafNode.deserialize(mem);
    node.init();
    try std.testing.expect(node.common.num_cells == 0);
    // assert

    @compileLog("Size of leaf node is: ", @sizeOf(LeafNode));
    @compileLog("Size of leaf header is: ", leaf_node_header_size);
    @compileLog("Size of leaf cell is: ", cell_size);
    @compileLog("Space for cells is: ", cell_space);
    @compileLog("Max cells is: ", leaf_max_cells);
}

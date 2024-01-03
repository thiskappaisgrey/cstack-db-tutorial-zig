// The article uses a slightly more compact representation for nodes(by removing the in-memory alignment when serializing to bytes)
// but I didn't bother b/c it wouldn't even save that much space anyways.. (less than 10 diff on the nums)
const std = @import("std");
const main = @import("main.zig");
const page_size = main.page_size;
const row_size = main.row_size;
const Row = main.Row;
// const Page = main.
pub const NodeType = enum(u8) { internal, leaf };
// Structs on the stack have to be divisible by 4
// but if you want a compact representation to store in a file
// you'd have to pack manually - and you can't get packing "for free"..
const NodeCommonHeader = struct { num_cells: u32, is_root: bool };
const LeafNodeHeader = struct { parent_pointer: u32 };
const Page = main.Page;

// TODO: Define better packing for this..
const node_common_header_size = @sizeOf(NodeCommonHeader) + @sizeOf(NodeType);
const leaf_node_header_size = @sizeOf(LeafNodeHeader) + node_common_header_size;
const cell_space = page_size - leaf_node_header_size;
const LeafCell = struct {
    key: u32,
    value: Row,
};
const cell_size = @sizeOf(LeafCell);
pub const leaf_max_cells = cell_space / cell_size;

pub const leaf_right_split_count = (leaf_max_cells + 1) / 2;
pub const leaf_left_split_count = (leaf_max_cells + 1) - leaf_right_split_count;

// const LeafNode
// The node_type will be encoded in the tagged union

const node_size = @sizeOf(Node);
const padding: u32 = page_size - node_size;
pub const LeafNode = struct {
    common: NodeCommonHeader,
    header: LeafNodeHeader,
    cells: [leaf_max_cells]LeafCell,
};

pub const InternalNodeHeader = struct {
    num_keys: u32,
    /// Page number of the rightmost child
    right_child: u32,
};
pub const InternalNodeCell = struct {
    /// The key is the maximum key contained in the child to it's left
    key: u32,
    child: u32,
};

const internal_node_header_size = @sizeOf(InternalNodeHeader) + node_common_header_size;
const internal_node_max_cells = (page_size - internal_node_header_size) / @sizeOf(InternalNodeCell);
pub const InternalNode = struct {
    common: NodeCommonHeader,
    header: InternalNodeHeader,
    cells: [internal_node_max_cells]InternalNodeCell,
    // header:
};

pub const Node = union(NodeType) {
    internal: InternalNode,
    leaf: LeafNode,
    pub fn serialize(self: *Node, page: Page) !void {
        var row_bytes = std.mem.asBytes(self);
        std.debug.print("Row bytes: {d}, page bytes: {d}\n", .{ page.len, row_bytes.len });
        // This assumes the size of the node is smaller than the page size
        @memcpy(page[0..node_size], row_bytes);
    }
    pub fn deserialize(page: Page) !*Node {
        // Instead of deserialzing - I think I need to set set the memory..?
        var s: *[node_size]u8 = @as(*[node_size]u8, @ptrCast(page.ptr));
        var leaf_node_ptr = std.mem.bytesAsValue(Node, s);
        return @alignCast(leaf_node_ptr);
    }
    /// The leaf node's fields is left uninitialized
    pub fn leaf_node() Node {
        return Node{ .leaf = LeafNode{ .common = NodeCommonHeader{ .is_root = false, .num_cells = 0 }, .header = undefined, .cells = undefined } };
    }
    pub fn internal_node() Node {
        return Node{ .internal = InternalNode{ .common = NodeCommonHeader{ .is_root = false, .num_cells = 0 }, .header = undefined, .cells = undefined } };
    }

    // Intialize a node based on the type..
    pub fn init(self: *Node) void {
        switch (self.*) {
            NodeType.leaf => |*l| {
                l.common.num_cells = 0;
            },
            NodeType.internal => |*i| {
                i.common.num_cells = 0;
            },
        }
    }

    pub fn common(self: *Node) *NodeCommonHeader {
        switch (self.*) {
            NodeType.leaf => |*l| {
                return &l.common;
            },
            NodeType.internal => |*i| {
                return &i.common;
            },
        }
    }

    pub fn get_node_max_key(self: *Node) u32 {
        switch (self.*) {
            Node.internal => |i| {
                return i.cells[i.header.num_keys - 1].key;
            },
            Node.leaf => |i| {
                return i.cells[i.common.num_cells - 1].key;
            },
        }
    }
};

test "Node test" {
    // In the node test, we log the sizes of the node
    // There's more "bloat" than the article but it's not too bad..
    // var alloc = std.testing.allocator;
    // var mem = try alloc.alloc(u8, page_size);
    // @memset(mem, 1);
    // defer alloc.free(mem);
    // var node: *Node = try Node.deserialize(mem);
    // node.init();
    // _ = node.common();
    // switch (node.*) {
    //     NodeType.leaf => |l| {
    //         try std.testing.expect(l.common.num_cells == 0);
    //     },
    //     NodeType.internal => |i| {
    //         try std.testing.expect(i.common.num_cells == 0);
    //     },
    //     // else => {
    //     //     try std.testing.expect(false);
    //     // },
    // }
    try std.testing.expect(node_size < page_size);

    // @compileLog("Size of leaf node is: ", @sizeOf(LeafNode));
    // @compileLog("Size of leaf header is: ", leaf_node_header_size);
    // @compileLog("Size of leaf cell is: ", cell_size);
    // @compileLog("Space for cells is: ", cell_space);
    // @compileLog("Max cells is: ", leaf_max_cells);
    // @compileLog("Size of internal node is: ", @sizeOf(InternalNode));
    // @compileLog("Max internal node cells is: ", internal_node_max_cells);
    // @compileLog("Size of internal node cell: ", @sizeOf(InternalNodeCell));
    // @compileLog("Size of node is: ", @sizeOf(Node));
}

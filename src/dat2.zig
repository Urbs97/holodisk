const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @cImport(@cInclude("zlib.h"));

pub const Entry = struct {
    filename: []const u8,
    is_compressed: bool,
    decompressed_size: u32,
    packed_size: u32,
    offset: u32,
};

pub const Archive = struct {
    entries: []Entry,
    filenames: [][]const u8,
    allocator: Allocator,

    pub fn deinit(self: *Archive) void {
        for (self.filenames) |name| {
            self.allocator.free(name);
        }
        self.allocator.free(self.filenames);
        self.allocator.free(self.entries);
    }
};

pub const Dat2Error = error{
    FileTooSmall,
    FileTooLarge,
    FileSizeMismatch,
    TreeSizeInvalid,
    TreeParseError,
    DecompressionFailed,
    CompressionFailed,
};

fn readExact(file: std.fs.File, buf: []u8) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try file.read(buf[total..]);
        if (n == 0) return error.UnexpectedEof;
        total += n;
    }
}

fn readU32Le(file: std.fs.File) !u32 {
    var buf: [4]u8 = undefined;
    try readExact(file, &buf);
    return std.mem.readInt(u32, &buf, .little);
}

pub fn readArchive(allocator: Allocator, file: std.fs.File) !Archive {
    const file_size = try file.getEndPos();

    if (file_size < 12) return error.FileTooSmall;

    const fs32 = std.math.cast(u32, file_size) orelse return error.FileTooLarge;

    // Read file_size field from last 4 bytes
    try file.seekTo(file_size - 4);
    const file_size_field = try readU32Le(file);
    if (file_size_field != fs32) return error.FileSizeMismatch;

    // Read tree_size
    try file.seekTo(file_size - 8);
    const tree_size_raw = try readU32Le(file);
    if (tree_size_raw < 4) return error.TreeSizeInvalid;

    const tree_entries_size: u32 = tree_size_raw - 4;
    const tree_end: u64 = file_size - 8;
    if (tree_entries_size > tree_end) return error.TreeSizeInvalid;

    const tree_start: u64 = tree_end - tree_entries_size;
    if (tree_start < 4) return error.TreeSizeInvalid;

    // Read num_files
    try file.seekTo(tree_start - 4);
    const num_files = try readU32Le(file);

    // Each tree entry is at least 17 bytes (4 filename_len + 13 metadata)
    if (num_files > tree_entries_size / 17) return error.TreeParseError;

    // Parse tree entries
    try file.seekTo(tree_start);

    const entries = try allocator.alloc(Entry, num_files);
    errdefer allocator.free(entries);
    const filenames = try allocator.alloc([]const u8, num_files);
    errdefer allocator.free(filenames);

    var parsed: u32 = 0;
    errdefer {
        for (filenames[0..parsed]) |name| allocator.free(name);
    }

    for (0..num_files) |i| {
        const filename_len = readU32Le(file) catch return error.TreeParseError;

        const filename_bytes = allocator.alloc(u8, filename_len) catch return error.TreeParseError;
        errdefer allocator.free(filename_bytes);
        readExact(file, filename_bytes) catch return error.TreeParseError;
        filenames[i] = filename_bytes;
        parsed = @intCast(i + 1);

        var meta_buf: [13]u8 = undefined; // 1 + 4 + 4 + 4
        readExact(file, &meta_buf) catch return error.TreeParseError;

        entries[i] = .{
            .filename = filename_bytes,
            .is_compressed = meta_buf[0] > 0,
            .decompressed_size = std.mem.readInt(u32, meta_buf[1..5], .little),
            .packed_size = std.mem.readInt(u32, meta_buf[5..9], .little),
            .offset = std.mem.readInt(u32, meta_buf[9..13], .little),
        };
    }

    return .{
        .entries = entries,
        .filenames = filenames,
        .allocator = allocator,
    };
}

fn zlibDecompress(allocator: Allocator, input: []const u8, output_size: u32) ![]u8 {
    const output = try allocator.alloc(u8, output_size);
    errdefer allocator.free(output);

    if (output_size == 0) return output;

    var strm: c.z_stream = std.mem.zeroes(c.z_stream);
    strm.next_in = @constCast(input.ptr);
    strm.avail_in = @intCast(input.len);
    strm.next_out = output.ptr;
    strm.avail_out = output_size;

    if (c.inflateInit(&strm) != c.Z_OK) return error.DecompressionFailed;
    defer _ = c.inflateEnd(&strm);

    const ret = c.inflate(&strm, c.Z_FINISH);
    if (ret != c.Z_STREAM_END) return error.DecompressionFailed;

    return output;
}

fn zlibCompress(allocator: Allocator, input: []const u8) ![]u8 {
    var compressed_size: c.uLongf = c.compressBound(@intCast(input.len));
    const output = try allocator.alloc(u8, compressed_size);
    errdefer allocator.free(output);

    const ret = c.compress2(
        output.ptr,
        &compressed_size,
        input.ptr,
        @intCast(input.len),
        c.Z_DEFAULT_COMPRESSION,
    );
    if (ret != c.Z_OK) return error.CompressionFailed;

    // Shrink to actual size
    return allocator.realloc(output, compressed_size);
}

const max_file_size: u32 = std.math.maxInt(u32);

pub fn extractEntry(allocator: Allocator, archive_file: std.fs.File, entry: Entry, output_dir: std.fs.Dir) !void {
    // Validate decompressed size before allocating
    if (entry.decompressed_size > max_file_size) return error.DecompressionFailed;

    // Convert backslash paths to forward slash for POSIX
    const path = try allocator.alloc(u8, entry.filename.len);
    defer allocator.free(path);
    for (entry.filename, 0..) |ch, i| {
        path[i] = if (ch == '\\') '/' else ch;
    }

    // Create parent directories
    if (std.fs.path.dirname(path)) |dir| {
        try output_dir.makePath(dir);
    }

    // Short-circuit for empty files
    if (entry.packed_size == 0 and entry.decompressed_size == 0) {
        const out_file = try output_dir.createFile(path, .{});
        defer out_file.close();
        return;
    }

    // Read raw data from archive
    try archive_file.seekTo(entry.offset);
    const raw_data = try allocator.alloc(u8, entry.packed_size);
    defer allocator.free(raw_data);
    try readExact(archive_file, raw_data);

    // Decompress if needed
    const output_data = if (entry.is_compressed)
        try zlibDecompress(allocator, raw_data, entry.decompressed_size)
    else
        raw_data;
    defer if (entry.is_compressed) allocator.free(output_data);

    // Write file
    const out_file = try output_dir.createFile(path, .{});
    defer out_file.close();
    try out_file.writeAll(output_data);
}

pub fn extractAll(allocator: Allocator, archive_file: std.fs.File, archive: Archive, output_dir: std.fs.Dir) !void {
    for (archive.entries) |entry| {
        try extractEntry(allocator, archive_file, entry, output_dir);
    }
}

pub const CreateOptions = struct {
    compress: bool = true,
};

pub fn createArchive(allocator: Allocator, source_dir: std.fs.Dir, output_file: std.fs.File, options: CreateOptions) !void {
    // Collect all file paths
    var paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (paths.items) |p| allocator.free(p);
        paths.deinit(allocator);
    }

    var walker = try source_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        const path_copy = try allocator.dupe(u8, entry.path);
        try paths.append(allocator, path_copy);
    }

    // Sort alphabetically for deterministic output
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);

    // Phase 1: Write data section, collect metadata
    const EntryMeta = struct {
        archive_path: []const u8,
        offset: u32,
        packed_size: u32,
        decompressed_size: u32,
        is_compressed: bool,
    };

    var metas: std.ArrayListUnmanaged(EntryMeta) = .empty;
    defer {
        for (metas.items) |meta| allocator.free(meta.archive_path);
        metas.deinit(allocator);
    }

    var data_offset: u32 = 0;

    for (paths.items) |rel_path| {
        // Read file contents
        const file_data = try source_dir.readFileAlloc(allocator, rel_path, max_file_size);
        defer allocator.free(file_data);

        // Convert forward slashes to backslashes for archive path
        const archive_path = try allocator.dupe(u8, rel_path);
        errdefer allocator.free(archive_path);
        for (archive_path) |*ch| {
            if (ch.* == '/') ch.* = '\\';
        }

        if (options.compress and file_data.len > 0) {
            // Compress data using zlib
            const compressed_data = try zlibCompress(allocator, file_data);
            defer allocator.free(compressed_data);

            try output_file.writeAll(compressed_data);

            try metas.append(allocator, .{
                .archive_path = archive_path,
                .offset = data_offset,
                .packed_size = @intCast(compressed_data.len),
                .decompressed_size = @intCast(file_data.len),
                .is_compressed = true,
            });
            data_offset += @intCast(compressed_data.len);
        } else if (file_data.len > 0) {
            // Store uncompressed
            try output_file.writeAll(file_data);

            try metas.append(allocator, .{
                .archive_path = archive_path,
                .offset = data_offset,
                .packed_size = @intCast(file_data.len),
                .decompressed_size = @intCast(file_data.len),
                .is_compressed = false,
            });
            data_offset += @intCast(file_data.len);
        } else {
            try metas.append(allocator, .{
                .archive_path = archive_path,
                .offset = data_offset,
                .packed_size = 0,
                .decompressed_size = 0,
                .is_compressed = false,
            });
        }
    }

    // Phase 2: Write num_files
    var num_files_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &num_files_buf, @intCast(metas.items.len), .little);
    try output_file.writeAll(&num_files_buf);

    // Phase 3: Write tree entries
    var tree_size: u32 = 0;
    for (metas.items) |meta| {
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(meta.archive_path.len), .little);
        try output_file.writeAll(&len_buf);
        try output_file.writeAll(meta.archive_path);
        try output_file.writeAll(&[_]u8{if (meta.is_compressed) 1 else 0});

        var val_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &val_buf, meta.decompressed_size, .little);
        try output_file.writeAll(&val_buf);
        std.mem.writeInt(u32, &val_buf, meta.packed_size, .little);
        try output_file.writeAll(&val_buf);
        std.mem.writeInt(u32, &val_buf, meta.offset, .little);
        try output_file.writeAll(&val_buf);

        tree_size += 4 + @as(u32, @intCast(meta.archive_path.len)) + 1 + 4 + 4 + 4;
    }

    // Phase 4: Write tree_size (includes itself)
    tree_size += 4;
    var ts_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &ts_buf, tree_size, .little);
    try output_file.writeAll(&ts_buf);

    // Phase 5: Write total file_size
    const total_size: u32 = data_offset + 4 + (tree_size - 4) + 4 + 4;
    var total_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &total_buf, total_size, .little);
    try output_file.writeAll(&total_buf);
}

pub fn listArchive(writer: *std.Io.Writer, archive: Archive) !void {
    try writer.print("{s:<60} {s:>10} {s:>10} {s}\n", .{ "FILENAME", "PACKED", "UNPACKED", "COMPRESSED" });
    try writer.print("{s:-<60} {s:->10} {s:->10} {s:-<10}\n", .{ "", "", "", "" });

    for (archive.entries) |entry| {
        try writer.print("{s:<60} {d:>10} {d:>10} {s}\n", .{
            entry.filename,
            entry.packed_size,
            entry.decompressed_size,
            if (entry.is_compressed) "yes" else "no",
        });
    }
}

fn roundTripTest(compress: bool) !void {
    const allocator = std.testing.allocator;

    // Create source directory with test files
    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();

    const file1_content = "Hello, World!";
    const file2_content = "This is a test file with some content.\nLine 2.\n";
    const file3_content = "";

    try source_tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = file1_content });
    try source_tmp.dir.makePath("subdir");
    try source_tmp.dir.writeFile(.{ .sub_path = "subdir/file2.txt", .data = file2_content });
    try source_tmp.dir.writeFile(.{ .sub_path = "empty.txt", .data = file3_content });

    // Create archive
    var archive_tmp = std.testing.tmpDir(.{});
    defer archive_tmp.cleanup();

    const archive_file = try archive_tmp.dir.createFile("test.dat2", .{ .read = true });
    defer archive_file.close();

    try createArchive(allocator, source_tmp.dir, archive_file, .{ .compress = compress });

    // Read archive back
    try archive_file.seekTo(0);
    var archive = try readArchive(allocator, archive_file);
    defer archive.deinit();

    try std.testing.expectEqual(@as(usize, 3), archive.entries.len);

    // Verify filenames (sorted: empty.txt, file1.txt, subdir\file2.txt)
    try std.testing.expectEqualStrings("empty.txt", archive.entries[0].filename);
    try std.testing.expectEqualStrings("file1.txt", archive.entries[1].filename);
    try std.testing.expectEqualStrings("subdir\\file2.txt", archive.entries[2].filename);

    // Extract to another temp dir
    var extract_tmp = std.testing.tmpDir(.{});
    defer extract_tmp.cleanup();

    try extractAll(allocator, archive_file, archive, extract_tmp.dir);

    // Verify extracted contents match originals
    const extracted1 = try extract_tmp.dir.readFileAlloc(allocator, "file1.txt", 4096);
    defer allocator.free(extracted1);
    try std.testing.expectEqualStrings(file1_content, extracted1);

    const extracted2 = try extract_tmp.dir.readFileAlloc(allocator, "subdir/file2.txt", 4096);
    defer allocator.free(extracted2);
    try std.testing.expectEqualStrings(file2_content, extracted2);

    const extracted3 = try extract_tmp.dir.readFileAlloc(allocator, "empty.txt", 4096);
    defer allocator.free(extracted3);
    try std.testing.expectEqualStrings(file3_content, extracted3);
}

test "round-trip compressed" {
    try roundTripTest(true);
}

test "round-trip uncompressed" {
    try roundTripTest(false);
}

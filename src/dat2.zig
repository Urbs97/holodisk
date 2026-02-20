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
    EntryOutOfBounds,
};

const max_file_size: u32 = std.math.maxInt(u32);

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

        const filename_bytes = try allocator.alloc(u8, filename_len);
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

pub fn readArchiveFromMemory(allocator: Allocator, data: []const u8) !Archive {
    const file_size = data.len;

    if (file_size < 12) return error.FileTooSmall;

    const fs32 = std.math.cast(u32, file_size) orelse return error.FileTooLarge;

    // Read file_size field from last 4 bytes
    const file_size_field = std.mem.readInt(u32, data[file_size - 4 ..][0..4], .little);
    if (file_size_field != fs32) return error.FileSizeMismatch;

    // Read tree_size
    const tree_size_raw = std.mem.readInt(u32, data[file_size - 8 ..][0..4], .little);
    if (tree_size_raw < 4) return error.TreeSizeInvalid;

    const tree_entries_size: usize = tree_size_raw - 4;
    const tree_end: usize = file_size - 8;
    if (tree_entries_size > tree_end) return error.TreeSizeInvalid;

    const tree_start: usize = tree_end - tree_entries_size;
    if (tree_start < 4) return error.TreeSizeInvalid;

    // Read num_files
    const num_files = std.mem.readInt(u32, data[tree_start - 4 ..][0..4], .little);

    // Each tree entry is at least 17 bytes (4 filename_len + 13 metadata)
    if (num_files > tree_entries_size / 17) return error.TreeParseError;

    // Parse tree entries
    var pos: usize = tree_start;

    const entries = try allocator.alloc(Entry, num_files);
    errdefer allocator.free(entries);
    const filenames = try allocator.alloc([]const u8, num_files);
    errdefer allocator.free(filenames);

    var parsed: u32 = 0;
    errdefer {
        for (filenames[0..parsed]) |name| allocator.free(name);
    }

    for (0..num_files) |i| {
        if (pos + 4 > data.len) return error.TreeParseError;
        const filename_len = std.mem.readInt(u32, data[pos..][0..4], .little);
        pos += 4;

        if (filename_len > tree_entries_size) return error.TreeParseError;
        if (pos + filename_len > data.len) return error.TreeParseError;
        const filename_bytes = try allocator.alloc(u8, filename_len);
        errdefer allocator.free(filename_bytes);
        @memcpy(filename_bytes, data[pos..][0..filename_len]);
        pos += filename_len;
        filenames[i] = filename_bytes;
        parsed = @intCast(i + 1);

        if (pos + 13 > data.len) return error.TreeParseError;
        const meta = data[pos..][0..13];

        const packed_size = std.mem.readInt(u32, meta[5..9], .little);
        const offset = std.mem.readInt(u32, meta[9..13], .little);

        // Validate that entry data falls within the data section
        const data_end: u64 = @as(u64, offset) + packed_size;
        if (data_end > tree_start - 4) return error.TreeParseError;

        entries[i] = .{
            .filename = filename_bytes,
            .is_compressed = meta[0] > 0,
            .decompressed_size = std.mem.readInt(u32, meta[1..5], .little),
            .packed_size = packed_size,
            .offset = offset,
        };
        pos += 13;
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

/// Extract a single entry from an archive file into a caller-owned memory buffer.
pub fn extractEntryToBuffer(allocator: Allocator, archive_file: std.fs.File, entry: Entry) ![]u8 {
    if (entry.decompressed_size > max_file_size) return error.DecompressionFailed;
    if (entry.packed_size > max_file_size) return error.DecompressionFailed;

    if (entry.packed_size == 0 and entry.decompressed_size == 0) {
        return allocator.alloc(u8, 0);
    }

    try archive_file.seekTo(entry.offset);
    const raw_data = try allocator.alloc(u8, entry.packed_size);

    if (!entry.is_compressed) {
        errdefer allocator.free(raw_data);
        try readExact(archive_file, raw_data);
        return raw_data;
    }

    defer allocator.free(raw_data);
    try readExact(archive_file, raw_data);
    return zlibDecompress(allocator, raw_data, entry.decompressed_size);
}

pub fn extractEntry(allocator: Allocator, archive_file: std.fs.File, entry: Entry, output_dir: std.fs.Dir) !void {
    const output_data = try extractEntryToBuffer(allocator, archive_file, entry);
    defer allocator.free(output_data);

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

    const out_file = try output_dir.createFile(path, .{});
    defer out_file.close();
    try out_file.writeAll(output_data);
}

pub fn extractAll(allocator: Allocator, archive_file: std.fs.File, archive: Archive, output_dir: std.fs.Dir) !void {
    for (archive.entries) |entry| {
        try extractEntry(allocator, archive_file, entry, output_dir);
    }
}

pub fn extractEntryFromMemory(allocator: Allocator, data: []const u8, entry: Entry) ![]u8 {
    if (entry.decompressed_size > max_file_size or entry.packed_size > max_file_size) return error.DecompressionFailed;

    // Short-circuit for empty files
    if (entry.packed_size == 0 and entry.decompressed_size == 0) {
        return allocator.alloc(u8, 0);
    }

    const end: u64 = @as(u64, entry.offset) + entry.packed_size;
    if (end > data.len) return error.EntryOutOfBounds;
    const raw_data = data[entry.offset..][0..entry.packed_size];

    if (entry.is_compressed) {
        return zlibDecompress(allocator, raw_data, entry.decompressed_size);
    } else {
        return allocator.dupe(u8, raw_data);
    }
}

pub fn extractAllFromMemory(allocator: Allocator, data: []const u8, archive: Archive) ![][]u8 {
    const results = try allocator.alloc([]u8, archive.entries.len);
    var extracted: usize = 0;
    errdefer {
        for (results[0..extracted]) |buf| allocator.free(buf);
        allocator.free(results);
    }

    for (archive.entries, 0..) |entry, i| {
        results[i] = try extractEntryFromMemory(allocator, data, entry);
        extracted = i + 1;
    }

    return results;
}

fn writeFooter(file: std.fs.File, entries: []const Entry) !void {
    var buf4: [4]u8 = undefined;

    // Write num_files
    std.mem.writeInt(u32, &buf4, @intCast(entries.len), .little);
    try file.writeAll(&buf4);

    // Write tree entries
    var tree_size: u32 = 0;
    for (entries) |entry| {
        std.mem.writeInt(u32, &buf4, @intCast(entry.filename.len), .little);
        try file.writeAll(&buf4);
        try file.writeAll(entry.filename);
        try file.writeAll(&[_]u8{if (entry.is_compressed) 1 else 0});

        std.mem.writeInt(u32, &buf4, entry.decompressed_size, .little);
        try file.writeAll(&buf4);
        std.mem.writeInt(u32, &buf4, entry.packed_size, .little);
        try file.writeAll(&buf4);
        std.mem.writeInt(u32, &buf4, entry.offset, .little);
        try file.writeAll(&buf4);

        tree_size += 4 + @as(u32, @intCast(entry.filename.len)) + 1 + 4 + 4 + 4;
    }

    // Write tree_size (includes itself)
    tree_size += 4;
    std.mem.writeInt(u32, &buf4, tree_size, .little);
    try file.writeAll(&buf4);

    // Write file_size
    const cur_pos = try file.getPos();
    const new_file_size = std.math.cast(u32, cur_pos + 4) orelse return error.FileTooLarge;
    std.mem.writeInt(u32, &buf4, new_file_size, .little);
    try file.writeAll(&buf4);

    // Truncate any leftover bytes from old footer
    try file.setEndPos(cur_pos + 4);
}

pub fn replaceEntry(
    allocator: Allocator,
    archive_file: std.fs.File,
    archive: *Archive,
    entry_index: usize,
    new_data: []const u8,
    compress: bool,
) !void {
    if (entry_index >= archive.entries.len) return error.EntryOutOfBounds;

    // Read current footer to locate the data section end
    const end_pos = try archive_file.getEndPos();
    const end_pos_32 = std.math.cast(u32, end_pos) orelse return error.FileTooLarge;
    try archive_file.seekTo(end_pos - 4);
    const file_size = try readU32Le(archive_file);
    if (file_size != end_pos_32) return error.FileSizeMismatch;
    try archive_file.seekTo(end_pos - 8);
    const tree_size = try readU32Le(archive_file);
    if (tree_size < 4) return error.TreeSizeInvalid;

    const tree_entries_size: u32 = tree_size - 4;
    const tree_end: u32 = file_size - 8;
    if (tree_entries_size > tree_end) return error.TreeSizeInvalid;

    const tree_start: u32 = tree_end - tree_entries_size;
    if (tree_start < 4) return error.TreeSizeInvalid;

    // data_end = tree_start - 4(num_files)
    const data_end: u32 = tree_start - 4;

    // Compress if requested; fall back to uncompressed when compression doesn't help
    const compressed = if (compress and new_data.len > 0) try zlibCompress(allocator, new_data) else null;
    defer if (compressed) |buf| allocator.free(buf);

    const use_compressed = compressed != null and compressed.?.len < new_data.len;
    const write_data = if (use_compressed) compressed.? else new_data;

    // Always append at data_end rather than reusing the old slot. This ensures
    // existing entry data is never overwritten. Dead space is reclaimed by
    // compactArchive. Note: a crash mid-write can leave the footer incomplete,
    // making the archive unreadable until repaired.
    const old_entry = archive.entries[entry_index];

    try archive_file.seekTo(data_end);
    try archive_file.writeAll(write_data);

    // Update in-memory entry; restore on footer-write failure
    archive.entries[entry_index] = .{
        .filename = old_entry.filename,
        .offset = data_end,
        .packed_size = std.math.cast(u32, write_data.len) orelse return error.FileTooLarge,
        .decompressed_size = std.math.cast(u32, new_data.len) orelse return error.FileTooLarge,
        .is_compressed = use_compressed,
    };
    errdefer archive.entries[entry_index] = old_entry;

    try writeFooter(archive_file, archive.entries);
}

pub const CompactOptions = struct {
    /// Keep the original file contents in memory so the archive can be
    /// restored if compaction fails partway through. When false, a failure
    /// leaves both the on-disk file and the in-memory entry offsets in an
    /// inconsistent, irrecoverable state.
    safe: bool = false,
};

pub fn compactArchive(
    allocator: Allocator,
    archive_file: std.fs.File,
    archive: *Archive,
    options: CompactOptions,
) !void {
    if (archive.entries.len == 0) return;

    // Snapshot the original file so we can restore on failure
    const end_pos = try archive_file.getEndPos();
    const end_pos_usize = std.math.cast(usize, end_pos) orelse return error.FileTooLarge;
    const snapshot = if (options.safe) blk: {
        const buf = try allocator.alloc(u8, end_pos_usize);
        errdefer allocator.free(buf);
        try archive_file.seekTo(0);
        try readExact(archive_file, buf);
        break :blk buf;
    } else null;
    defer if (snapshot) |s| allocator.free(s);

    // Save original entry offsets so we can roll back the in-memory state
    const old_offsets = if (options.safe) blk: {
        const offsets = try allocator.alloc(u32, archive.entries.len);
        for (archive.entries, 0..) |entry, i| offsets[i] = entry.offset;
        break :blk offsets;
    } else null;
    defer if (old_offsets) |o| allocator.free(o);

    compactArchiveInner(allocator, archive_file, archive, snapshot) catch |err| {
        if (snapshot) |s| {
            // Restore in-memory offsets regardless of file restore outcome
            defer for (archive.entries, 0..) |*entry, i| {
                entry.offset = old_offsets.?[i];
            };
            // Restore original file contents
            archive_file.seekTo(0) catch return err;
            archive_file.writeAll(s) catch return err;
            archive_file.setEndPos(end_pos) catch return err;
        }
        return err;
    };
}

fn compactArchiveInner(
    allocator: Allocator,
    archive_file: std.fs.File,
    archive: *Archive,
    snapshot: ?[]const u8,
) !void {
    // Build index sorted by offset so we always move data forward (no overlap)
    const indices = try allocator.alloc(usize, archive.entries.len);
    defer allocator.free(indices);
    for (indices, 0..) |*idx, i| idx.* = i;

    std.mem.sort(usize, indices, archive.entries, struct {
        fn lessThan(entries: []Entry, a: usize, b: usize) bool {
            return entries[a].offset < entries[b].offset;
        }
    }.lessThan);

    // When we have a snapshot we can read entry data directly from it,
    // otherwise allocate a temporary buffer for file-based reads.
    var max_packed: u32 = 0;
    if (snapshot == null) {
        for (archive.entries) |entry| {
            if (entry.packed_size > max_packed) max_packed = entry.packed_size;
        }
    }

    const buf = if (max_packed > 0) try allocator.alloc(u8, max_packed) else @as([]u8, &.{});
    defer if (max_packed > 0) allocator.free(buf);

    // Compact: move each entry's data forward to eliminate gaps
    var write_pos: u32 = 0;
    for (indices) |idx| {
        const entry = &archive.entries[idx];
        if (entry.packed_size == 0) {
            entry.offset = write_pos;
            continue;
        }

        if (entry.offset != write_pos) {
            const data: []const u8 = if (snapshot) |s|
                s[entry.offset..][0..entry.packed_size]
            else blk: {
                try archive_file.seekTo(entry.offset);
                try readExact(archive_file, buf[0..entry.packed_size]);
                break :blk @as([]const u8, buf[0..entry.packed_size]);
            };
            try archive_file.seekTo(write_pos);
            try archive_file.writeAll(data);
        }

        entry.offset = write_pos;
        write_pos += entry.packed_size;
    }

    // Rewrite footer at the new data end
    try archive_file.seekTo(write_pos);
    try writeFooter(archive_file, archive.entries);
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

    // Sort case-insensitively using backslash convention so entry order matches the Fallout 2
    // engine, which uses binary search on tree entries. Paths still use '/' for filesystem reads.
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            const len = @min(a.len, b.len);
            for (a[0..len], b[0..len]) |ac, bc| {
                const an: u8 = if (ac == '/') '\\' else std.ascii.toLower(ac);
                const bn: u8 = if (bc == '/') '\\' else std.ascii.toLower(bc);
                if (an < bn) return true;
                if (an > bn) return false;
            }
            return a.len < b.len;
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

    // Phase 2: Write footer (num_files, tree entries, tree_size, file_size)
    const entries = try allocator.alloc(Entry, metas.items.len);
    defer allocator.free(entries);
    for (metas.items, 0..) |meta, i| {
        entries[i] = .{
            .filename = meta.archive_path,
            .is_compressed = meta.is_compressed,
            .decompressed_size = meta.decompressed_size,
            .packed_size = meta.packed_size,
            .offset = meta.offset,
        };
    }
    try writeFooter(output_file, entries);
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

test "sort order matches DAT2 convention" {
    const allocator = std.testing.allocator;

    var paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer paths.deinit(allocator);

    // These paths exercise both bugs:
    // 1. Backslash convention: '/' (47) vs '\' (92) changes order around 'A' (65)
    // 2. Case-insensitivity: 'V' (86) vs 'a' (97) — case-sensitive would put uppercase first
    try paths.append(allocator, "art/misc/file.frm");
    try paths.append(allocator, "art/miscA.frm");
    try paths.append(allocator, "art/cuts/VSUIT.mve");
    try paths.append(allocator, "art/cuts/afailed.cfg");

    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            const len = @min(a.len, b.len);
            for (a[0..len], b[0..len]) |ac, bc| {
                const an: u8 = if (ac == '/') '\\' else std.ascii.toLower(ac);
                const bn: u8 = if (bc == '/') '\\' else std.ascii.toLower(bc);
                if (an < bn) return true;
                if (an > bn) return false;
            }
            return a.len < b.len;
        }
    }.lessThan);

    // Case-insensitive: 'a' < 'v', so afailed sorts before VSUIT
    try std.testing.expectEqualStrings("art/cuts/afailed.cfg", paths.items[0]);
    try std.testing.expectEqualStrings("art/cuts/VSUIT.mve", paths.items[1]);
    // Backslash convention: 'a' (97) > '\' (92), so miscA sorts before misc/file
    try std.testing.expectEqualStrings("art/miscA.frm", paths.items[2]);
    try std.testing.expectEqualStrings("art/misc/file.frm", paths.items[3]);
}

test "extractEntryToBuffer" {
    const allocator = std.testing.allocator;

    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();

    const file1_content = "Hello, World!";
    const file2_content = "This is a test file with some content.\nLine 2.\n";
    const file3_content = "";

    try source_tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = file1_content });
    try source_tmp.dir.makePath("subdir");
    try source_tmp.dir.writeFile(.{ .sub_path = "subdir/file2.txt", .data = file2_content });
    try source_tmp.dir.writeFile(.{ .sub_path = "empty.txt", .data = file3_content });

    // Create compressed archive
    var archive_tmp = std.testing.tmpDir(.{});
    defer archive_tmp.cleanup();

    const archive_file = try archive_tmp.dir.createFile("test.dat2", .{ .read = true });
    defer archive_file.close();

    try createArchive(allocator, source_tmp.dir, archive_file, .{ .compress = true });

    try archive_file.seekTo(0);
    var archive = try readArchive(allocator, archive_file);
    defer archive.deinit();

    // Sorted order: empty.txt, file1.txt, subdir\file2.txt
    const expected = [_][]const u8{ file3_content, file1_content, file2_content };

    for (archive.entries, 0..) |entry, i| {
        const buf = try extractEntryToBuffer(allocator, archive_file, entry);
        defer allocator.free(buf);
        try std.testing.expectEqualStrings(expected[i], buf);
    }

    // Also test with an uncompressed archive
    const archive_file2 = try archive_tmp.dir.createFile("test_uncomp.dat2", .{ .read = true });
    defer archive_file2.close();

    try createArchive(allocator, source_tmp.dir, archive_file2, .{ .compress = false });

    try archive_file2.seekTo(0);
    var archive2 = try readArchive(allocator, archive_file2);
    defer archive2.deinit();

    for (archive2.entries, 0..) |entry, i| {
        const buf = try extractEntryToBuffer(allocator, archive_file2, entry);
        defer allocator.free(buf);
        try std.testing.expectEqualStrings(expected[i], buf);
    }
}

test "round-trip from memory" {
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

    // Create archive to a temp file
    var archive_tmp = std.testing.tmpDir(.{});
    defer archive_tmp.cleanup();

    const archive_file = try archive_tmp.dir.createFile("test.dat2", .{ .read = true });
    defer archive_file.close();

    try createArchive(allocator, source_tmp.dir, archive_file, .{ .compress = true });

    // Read entire file into memory
    try archive_file.seekTo(0);
    const file_size = try archive_file.getEndPos();
    const buffer = try allocator.alloc(u8, file_size);
    defer allocator.free(buffer);
    try readExact(archive_file, buffer);

    // Parse from memory
    var archive = try readArchiveFromMemory(allocator, buffer);
    defer archive.deinit();

    // Verify entry count
    try std.testing.expectEqual(@as(usize, 3), archive.entries.len);

    // Verify filenames (sorted: empty.txt, file1.txt, subdir\file2.txt)
    try std.testing.expectEqualStrings("empty.txt", archive.entries[0].filename);
    try std.testing.expectEqualStrings("file1.txt", archive.entries[1].filename);
    try std.testing.expectEqualStrings("subdir\\file2.txt", archive.entries[2].filename);

    // Verify entry metadata
    try std.testing.expectEqual(@as(u32, 0), archive.entries[0].decompressed_size);
    try std.testing.expectEqual(false, archive.entries[0].is_compressed);

    try std.testing.expectEqual(@as(u32, file1_content.len), archive.entries[1].decompressed_size);
    try std.testing.expectEqual(true, archive.entries[1].is_compressed);

    try std.testing.expectEqual(@as(u32, file2_content.len), archive.entries[2].decompressed_size);
    try std.testing.expectEqual(true, archive.entries[2].is_compressed);

    // Extract from memory and verify contents
    const results = try extractAllFromMemory(allocator, buffer, archive);
    defer {
        for (results) |buf| allocator.free(buf);
        allocator.free(results);
    }

    try std.testing.expectEqual(@as(usize, 3), results.len);
    // Sorted order: empty.txt, file1.txt, subdir\file2.txt
    try std.testing.expectEqualStrings(file3_content, results[0]);
    try std.testing.expectEqualStrings(file1_content, results[1]);
    try std.testing.expectEqualStrings(file2_content, results[2]);
}

test "replaceEntry" {
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

    // Create compressed archive
    var archive_tmp = std.testing.tmpDir(.{});
    defer archive_tmp.cleanup();

    const archive_file = try archive_tmp.dir.createFile("test.dat2", .{ .read = true });
    defer archive_file.close();

    try createArchive(allocator, source_tmp.dir, archive_file, .{ .compress = true });

    // Read archive back
    try archive_file.seekTo(0);
    var archive = try readArchive(allocator, archive_file);
    defer archive.deinit();

    try std.testing.expectEqual(@as(usize, 3), archive.entries.len);
    const file1_idx = for (archive.entries, 0..) |entry, i| {
        if (std.mem.eql(u8, entry.filename, "file1.txt")) break i;
    } else return error.TestUnexpectedResult;

    // Replace file1.txt with new content
    const new_content = "Replaced content! This is entirely different data.";
    try replaceEntry(allocator, archive_file, &archive, file1_idx, new_content, true);

    // Verify in-memory entry was updated
    try std.testing.expectEqual(@as(u32, new_content.len), archive.entries[file1_idx].decompressed_size);
    try std.testing.expectEqual(true, archive.entries[file1_idx].is_compressed);

    // Re-read the archive from disk to verify on-disk consistency
    try archive_file.seekTo(0);
    var archive2 = try readArchive(allocator, archive_file);
    defer archive2.deinit();

    try std.testing.expectEqual(@as(usize, 3), archive2.entries.len);
    const file1_idx2 = for (archive2.entries, 0..) |entry, i| {
        if (std.mem.eql(u8, entry.filename, "file1.txt")) break i;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, new_content.len), archive2.entries[file1_idx2].decompressed_size);

    // Extract the replaced entry and verify new content
    const extracted = try extractEntryToBuffer(allocator, archive_file, archive2.entries[file1_idx2]);
    defer allocator.free(extracted);
    try std.testing.expectEqualStrings(new_content, extracted);

    // Verify other entries are unchanged
    const original_contents = [_]struct { name: []const u8, data: []const u8 }{
        .{ .name = "empty.txt", .data = file3_content },
        .{ .name = "subdir\\file2.txt", .data = file2_content },
    };
    for (archive2.entries) |entry| {
        if (std.mem.eql(u8, entry.filename, "file1.txt")) continue;
        const content = for (original_contents) |item| {
            if (std.mem.eql(u8, entry.filename, item.name)) break item.data;
        } else unreachable;
        const buf = try extractEntryToBuffer(allocator, archive_file, entry);
        defer allocator.free(buf);
        try std.testing.expectEqualStrings(content, buf);
    }
}

test "replaceEntry always appends to preserve old data" {
    const allocator = std.testing.allocator;

    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();

    // Use uncompressed so packed_size == data.len for predictable slot sizes
    try source_tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = "AAAAAAAAAA" }); // 10 bytes
    try source_tmp.dir.writeFile(.{ .sub_path = "file2.txt", .data = "BBBBBBBBBB" }); // 10 bytes

    var archive_tmp = std.testing.tmpDir(.{});
    defer archive_tmp.cleanup();

    const archive_file = try archive_tmp.dir.createFile("test.dat2", .{ .read = true });
    defer archive_file.close();

    try createArchive(allocator, source_tmp.dir, archive_file, .{ .compress = false });

    try archive_file.seekTo(0);
    var archive = try readArchive(allocator, archive_file);
    defer archive.deinit();

    try std.testing.expectEqualStrings("file1.txt", archive.entries[0].filename);

    const old_offset = archive.entries[0].offset;

    // Replace with smaller data — should still append (not reuse old slot)
    try replaceEntry(allocator, archive_file, &archive, 0, "CCCCC", false);

    // Offset should have changed (appended, old data preserved)
    try std.testing.expect(archive.entries[0].offset != old_offset);

    // Verify data round-trips correctly
    try archive_file.seekTo(0);
    var archive2 = try readArchive(allocator, archive_file);
    defer archive2.deinit();

    const extracted = try extractEntryToBuffer(allocator, archive_file, archive2.entries[0]);
    defer allocator.free(extracted);
    try std.testing.expectEqualStrings("CCCCC", extracted);

    const extracted1 = try extractEntryToBuffer(allocator, archive_file, archive2.entries[1]);
    defer allocator.free(extracted1);
    try std.testing.expectEqualStrings("BBBBBBBBBB", extracted1);
}

test "compactArchive removes dead space" {
    const allocator = std.testing.allocator;

    var source_tmp = std.testing.tmpDir(.{});
    defer source_tmp.cleanup();

    // Uncompressed for predictable sizes
    try source_tmp.dir.writeFile(.{ .sub_path = "file1.txt", .data = "AAAAAAAAAA" }); // 10 bytes
    try source_tmp.dir.writeFile(.{ .sub_path = "file2.txt", .data = "BBBBBBBBBB" }); // 10 bytes

    var archive_tmp = std.testing.tmpDir(.{});
    defer archive_tmp.cleanup();

    const archive_file = try archive_tmp.dir.createFile("test.dat2", .{ .read = true });
    defer archive_file.close();

    try createArchive(allocator, source_tmp.dir, archive_file, .{ .compress = false });

    try archive_file.seekTo(0);
    var archive = try readArchive(allocator, archive_file);
    defer archive.deinit();

    // Replace with larger data to force an append (creates dead space at old slot)
    try replaceEntry(allocator, archive_file, &archive, 0, "CCCCCCCCCCCCCCCC", false); // 16 bytes > 10

    const size_before_compact = try archive_file.getEndPos();

    // Compact should reclaim the 10 bytes of dead space
    try compactArchive(allocator, archive_file, &archive, .{ .safe = true });

    const size_after_compact = try archive_file.getEndPos();
    try std.testing.expect(size_after_compact < size_before_compact);

    // Verify the archive is still valid and data is correct
    try archive_file.seekTo(0);
    var archive2 = try readArchive(allocator, archive_file);
    defer archive2.deinit();

    try std.testing.expectEqual(@as(usize, 2), archive2.entries.len);

    const extracted0 = try extractEntryToBuffer(allocator, archive_file, archive2.entries[0]);
    defer allocator.free(extracted0);
    try std.testing.expectEqualStrings("CCCCCCCCCCCCCCCC", extracted0);

    const extracted1 = try extractEntryToBuffer(allocator, archive_file, archive2.entries[1]);
    defer allocator.free(extracted1);
    try std.testing.expectEqualStrings("BBBBBBBBBB", extracted1);
}

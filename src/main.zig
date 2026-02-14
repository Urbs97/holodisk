const std = @import("std");
const dat2 = @import("holodisk").dat2;

pub fn main() u8 {
    return run() catch |err| {
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "error: {s}\n", .{@errorName(err)}) catch "error: unknown\n";
        std.fs.File.stderr().writeAll(msg) catch {};
        return 2;
    };
}

fn run() !u8 {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = std.process.args();
    _ = args.next(); // skip program name

    const command = args.next() orelse {
        printUsage();
        return 1;
    };

    if (std.mem.eql(u8, command, "list")) {
        return cmdList(allocator, &args);
    } else if (std.mem.eql(u8, command, "extract")) {
        return cmdExtract(allocator, &args);
    } else if (std.mem.eql(u8, command, "create")) {
        return cmdCreate(allocator, &args);
    } else {
        printUsage();
        return 1;
    }
}

fn printUsage() void {
    const stderr = std.fs.File.stderr();
    stderr.writeAll(
        \\Usage: holodisk <command> [args...]
        \\
        \\Commands:
        \\  list <archive.dat>                            List archive contents
        \\  extract <archive.dat> [output_dir]            Extract all files
        \\  create [--no-compress] <directory> <output.dat>  Pack directory into archive
        \\
    ) catch {};
}

fn cmdList(allocator: std.mem.Allocator, args: *std.process.ArgIterator) u8 {
    const archive_path = args.next() orelse {
        std.fs.File.stderr().writeAll("error: missing archive path\n") catch {};
        return 1;
    };

    const file = std.fs.cwd().openFile(archive_path, .{}) catch |err| {
        printErr("failed to open '{s}': {s}", .{ archive_path, @errorName(err) });
        return 2;
    };
    defer file.close();

    var archive = dat2.readArchive(allocator, file) catch |err| {
        printErr("failed to read archive: {s}", .{@errorName(err)});
        return 2;
    };
    defer archive.deinit();

    const stdout = std.fs.File.stdout();
    var write_buf: [4096]u8 = undefined;
    var writer = stdout.writer(&write_buf);

    dat2.listArchive(&writer.interface, archive) catch |err| {
        printErr("failed to write output: {s}", .{@errorName(err)});
        return 2;
    };
    writer.interface.flush() catch {};

    return 0;
}

fn cmdExtract(allocator: std.mem.Allocator, args: *std.process.ArgIterator) u8 {
    const archive_path = args.next() orelse {
        std.fs.File.stderr().writeAll("error: missing archive path\n") catch {};
        return 1;
    };

    // Default output dir: archive name without extension
    const output_path = args.next() orelse blk: {
        const basename = std.fs.path.basename(archive_path);
        const stem = std.fs.path.stem(basename);
        break :blk stem;
    };

    const file = std.fs.cwd().openFile(archive_path, .{}) catch |err| {
        printErr("failed to open '{s}': {s}", .{ archive_path, @errorName(err) });
        return 2;
    };
    defer file.close();

    var archive = dat2.readArchive(allocator, file) catch |err| {
        printErr("failed to read archive: {s}", .{@errorName(err)});
        return 2;
    };
    defer archive.deinit();

    // Create output directory
    std.fs.cwd().makePath(output_path) catch |err| {
        printErr("failed to create output directory '{s}': {s}", .{ output_path, @errorName(err) });
        return 2;
    };

    var output_dir = std.fs.cwd().openDir(output_path, .{}) catch |err| {
        printErr("failed to open output directory '{s}': {s}", .{ output_path, @errorName(err) });
        return 2;
    };
    defer output_dir.close();

    dat2.extractAll(allocator, file, archive, output_dir) catch |err| {
        printErr("extraction failed: {s}", .{@errorName(err)});
        return 2;
    };

    printErr("extracted {d} files to '{s}'", .{ archive.entries.len, output_path });
    return 0;
}

fn cmdCreate(allocator: std.mem.Allocator, args: *std.process.ArgIterator) u8 {
    var compress = true;
    var dir_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--no-compress")) {
            compress = false;
        } else if (dir_path == null) {
            dir_path = arg;
        } else if (output_path == null) {
            output_path = arg;
        } else {
            printErr("unexpected argument: '{s}'", .{arg});
            return 1;
        }
    }

    const dir = dir_path orelse {
        std.fs.File.stderr().writeAll("error: missing source directory\n") catch {};
        return 1;
    };

    const out = output_path orelse {
        std.fs.File.stderr().writeAll("error: missing output path\n") catch {};
        return 1;
    };

    var source_dir = std.fs.cwd().openDir(dir, .{ .iterate = true }) catch |err| {
        printErr("failed to open directory '{s}': {s}", .{ dir, @errorName(err) });
        return 2;
    };
    defer source_dir.close();

    const output_file = std.fs.cwd().createFile(out, .{}) catch |err| {
        printErr("failed to create '{s}': {s}", .{ out, @errorName(err) });
        return 2;
    };
    defer output_file.close();

    dat2.createArchive(allocator, source_dir, output_file, .{ .compress = compress }) catch |err| {
        printErr("archive creation failed: {s}", .{@errorName(err)});
        return 2;
    };

    printErr("created archive '{s}'", .{out});
    return 0;
}

fn printErr(comptime fmt: []const u8, args: anytype) void {
    const stderr = std.fs.File.stderr();
    var buf: [4096]u8 = undefined;
    var writer = stderr.writer(&buf);
    writer.interface.print(fmt ++ "\n", args) catch {};
    writer.interface.flush() catch {};
}

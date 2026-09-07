const std = @import("std");
const Io = std.Io;
const dat2 = @import("holodisk").dat2;

pub fn main(init: std.process.Init) u8 {
    return run(init) catch |err| {
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "error: {s}\n", .{@errorName(err)}) catch "error: unknown\n";
        Io.File.stderr().writeStreamingAll(init.io, msg) catch {};
        return 2;
    };
}

fn run(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next(); // skip program name

    const command = args.next() orelse {
        printUsage(io);
        return 1;
    };

    if (std.mem.eql(u8, command, "list")) {
        return cmdList(allocator, io, &args);
    } else if (std.mem.eql(u8, command, "extract")) {
        return cmdExtract(allocator, io, &args);
    } else if (std.mem.eql(u8, command, "create")) {
        return cmdCreate(allocator, io, &args);
    } else {
        printUsage(io);
        return 1;
    }
}

fn printUsage(io: Io) void {
    Io.File.stderr().writeStreamingAll(io,
        \\Usage: holodisk <command> [args...]
        \\
        \\Commands:
        \\  list <archive.dat>                            List archive contents
        \\  extract <archive.dat> [output_dir]            Extract all files
        \\  create [--no-compress] <directory> <output.dat>  Pack directory into archive
        \\
    ) catch {};
}

fn cmdList(allocator: std.mem.Allocator, io: Io, args: *std.process.Args.Iterator) u8 {
    const archive_path = args.next() orelse {
        Io.File.stderr().writeStreamingAll(io, "error: missing archive path\n") catch {};
        return 1;
    };

    const file = Io.Dir.cwd().openFile(io, archive_path, .{}) catch |err| {
        printErr(io, "failed to open '{s}': {s}", .{ archive_path, @errorName(err) });
        return 2;
    };
    defer file.close(io);

    var archive = dat2.readArchive(allocator, io, file) catch |err| {
        printErr(io, "failed to read archive: {s}", .{@errorName(err)});
        return 2;
    };
    defer archive.deinit();

    const stdout = Io.File.stdout();
    var write_buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &write_buf);

    dat2.listArchive(&writer.interface, archive) catch |err| {
        printErr(io, "failed to write output: {s}", .{@errorName(err)});
        return 2;
    };
    writer.interface.flush() catch {};

    return 0;
}

fn cmdExtract(allocator: std.mem.Allocator, io: Io, args: *std.process.Args.Iterator) u8 {
    const archive_path = args.next() orelse {
        Io.File.stderr().writeStreamingAll(io, "error: missing archive path\n") catch {};
        return 1;
    };

    // Default output dir: archive name without extension
    const output_path = args.next() orelse blk: {
        const basename = std.fs.path.basename(archive_path);
        const stem = std.fs.path.stem(basename);
        break :blk stem;
    };

    const file = Io.Dir.cwd().openFile(io, archive_path, .{}) catch |err| {
        printErr(io, "failed to open '{s}': {s}", .{ archive_path, @errorName(err) });
        return 2;
    };
    defer file.close(io);

    var archive = dat2.readArchive(allocator, io, file) catch |err| {
        printErr(io, "failed to read archive: {s}", .{@errorName(err)});
        return 2;
    };
    defer archive.deinit();

    // Create output directory
    Io.Dir.cwd().createDirPath(io, output_path) catch |err| {
        printErr(io, "failed to create output directory '{s}': {s}", .{ output_path, @errorName(err) });
        return 2;
    };

    var output_dir = Io.Dir.cwd().openDir(io, output_path, .{}) catch |err| {
        printErr(io, "failed to open output directory '{s}': {s}", .{ output_path, @errorName(err) });
        return 2;
    };
    defer output_dir.close(io);

    dat2.extractAll(allocator, io, file, archive, output_dir) catch |err| {
        printErr(io, "extraction failed: {s}", .{@errorName(err)});
        return 2;
    };

    printErr(io, "extracted {d} files to '{s}'", .{ archive.entries.len, output_path });
    return 0;
}

fn cmdCreate(allocator: std.mem.Allocator, io: Io, args: *std.process.Args.Iterator) u8 {
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
            printErr(io, "unexpected argument: '{s}'", .{arg});
            return 1;
        }
    }

    const dir = dir_path orelse {
        Io.File.stderr().writeStreamingAll(io, "error: missing source directory\n") catch {};
        return 1;
    };

    const out = output_path orelse {
        Io.File.stderr().writeStreamingAll(io, "error: missing output path\n") catch {};
        return 1;
    };

    var source_dir = Io.Dir.cwd().openDir(io, dir, .{ .iterate = true }) catch |err| {
        printErr(io, "failed to open directory '{s}': {s}", .{ dir, @errorName(err) });
        return 2;
    };
    defer source_dir.close(io);

    const output_file = Io.Dir.cwd().createFile(io, out, .{}) catch |err| {
        printErr(io, "failed to create '{s}': {s}", .{ out, @errorName(err) });
        return 2;
    };
    defer output_file.close(io);

    dat2.createArchive(allocator, io, source_dir, output_file, .{ .compress = compress }) catch |err| {
        printErr(io, "archive creation failed: {s}", .{@errorName(err)});
        return 2;
    };

    printErr(io, "created archive '{s}'", .{out});
    return 0;
}

fn printErr(io: Io, comptime fmt: []const u8, args: anytype) void {
    const stderr = Io.File.stderr();
    var buf: [4096]u8 = undefined;
    var writer = stderr.writer(io, &buf);
    writer.interface.print(fmt ++ "\n", args) catch {};
    writer.interface.flush() catch {};
}

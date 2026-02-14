# holodisk

A CLI tool for working with Fallout 2 DAT2 archive files. Extract, create, and inspect DAT2 archives from the command line.

## Usage

```
holodisk <command> [args...]
```

### Commands

**List** archive contents:
```sh
holodisk list master.dat
```

**Extract** all files from an archive:
```sh
holodisk extract master.dat output_dir
```

If no output directory is specified, files are extracted to a directory named after the archive (e.g. `master.dat` extracts to `master/`).

**Create** an archive from a directory:
```sh
holodisk create my_mod/ my_mod.dat
```

Files are sorted alphabetically and compressed with zlib by default. To store files without compression:
```sh
holodisk create --no-compress my_mod/ my_mod.dat
```

The resulting archive is compatible with the original game engine and other DAT2 tools.

## Building

Requires [Zig](https://ziglang.org/) 0.15+ and zlib development headers.

```sh
zig build
```

The binary is placed in `zig-out/bin/holodisk`.

To build an optimized release binary:
```sh
zig build -Doptimize=ReleaseFast
```

To build and run in one step:
```sh
zig build run -- list master.dat
```

### Dependencies

- **zlib** (system library) — used for DEFLATE compression and decompression
- **libc** — required by the zlib linkage

These are temporary dependencies. The Zig standard library's `std.compress.flate` module is currently undergoing a refactoring that leaves compression unusable in Zig 0.15. Once that work is complete, holodisk will switch to the pure-Zig implementation and drop both the libc and zlib requirements, enabling fully static, cross-compilable builds with no external dependencies.

> **Windows:** Windows support is currently blocked by the zlib system dependency. It will be automatically available once holodisk switches to the Zig standard library's compression. If you need Windows support now, you can vendor zlib and let Zig compile everything from source or [open an issue](../../issues) and ask.

## DAT2 Format

DAT2 is the archive format used by Fallout 2 for packaging game data. The format stores files with optional zlib compression and uses a trailing metadata tree:

```
[file data: concatenated entries, possibly zlib-compressed]
[num_files: u32 LE]
[tree entries: variable-length metadata for each file]
[tree_size: u32 LE]
[file_size: u32 LE]
```

## License

[MIT](LICENSE)

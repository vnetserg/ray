# Ray
A minimalistic key-value storage.

> Disclaimer: this project is made solely for educational purposes and for the fun of the author.
> PRs and issues are welcome, but don't expect them to be addressed in a timely manner.

## About Ray

Ray is persistent key-value storage with linearizability guarantee written in async Rust.
It works over gRPC, has a simple proto api, extensive debug logging, metrics collection
and even a Graphana dashboard to visualize them.

The project consists of:

* Server binary - `rayd`;
* Rust client library suitable for use with Tokio runtime;
* Command-line client - `ray`;
* Benchmarking tool - `ray-benchmark`.

## Running `rayd`

```
$ cargo run --release --bin rayd
```

This will run `rayd` in default configuration. It will:

* Create `journal` directory for write-ahead log;
* Create `snapshots` directory for storage snapshots;
* Create `rayd.debug.log` file (if does not exist) and write debug logs to it;
* Listen on port 39172 for client connections.

Options are supplied to `rayd` via config file. Example config can be found in `examples/config.yml`.
To run `rayd` with config file, do

```
$ cargo run --release --bin rayd -- -c example/config.yml
```

## Using `ray`

`ray` is a command-line tool that allows you to interact with `rayd` manually. For example:

```
$ cargo run --bin ray -- set my_key my_value
$ cargo run --bin ray -- get my_key
```

This will try to connect to `rayd` assuming it is listening on `localhost:39172`.
Use `--address` and `--port` keys to connect to a different address and port.

## Running `ray-benchmark`

Benchmarking tool supports two modes: `read` and `write`. Example usage:

```
$ cargo run --release --bin ray-benchmark -- --tasks 1000 --delay 10000 --key-len 8 read
```

This will run 1000 tasks, each making a read request of a random 8-byte key approximately every 10000 mcs.

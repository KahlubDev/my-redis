# My-Redis

A custom Redis-compatible server built with **Tokio** in Rust. This project implements core Redis commands (`GET`, `SET`) using asynchronous I/O, demonstrating idiomatic async Rust patterns.

## Features

- **Asynchronous I/O**: Built on the Tokio runtime for high-performance, non-blocking network operations.
- **Core Commands**: Implements `GET` and `SET` commands with proper Redis protocol framing.
- **Task Management**: Uses `tokio::spawn` to handle multiple client connections concurrently on a single thread.
- **Observability**: Integrated with `tracing` and `console-subscriber` for real-time runtime inspection (tasks, resources, and spans).
- **Unit Testing**: Includes robust async unit tests using `#[tokio::test(start_paused = true)]` and mocked I/O streams.

## Tech Stack

- **Language**: Rust (Edition 2024)
- **Runtime**: Tokio (with `full` and `tracing` features)
- **Serialization**: `bytes`, `mini-redis` (for protocol framing)
- **Observability**: `tracing`, `tracing-subscriber`, `console-subscriber`
- **Testing**: `tokio-test`, `tokio` (with `test-util` feature)

## Project Structure
```
my-redis/
├── src/
│   ├── lib.rs              # Library crate exposing core types
│   ├── connection.rs       # Redis protocol framing logic
│   └── bin/
│       ├── server.rs       # Main server binary
│       ├── tracing_demo.rs # Example tracing instrumentation
│       ├── server_test.rs  # Unit tests for async logic
│       └── ...             # Other demo binaries
├── Cargo.toml              # Dependencies and configuration
└── README.md               # This file
```
# Quick Start

## Prerequisites

- Rust toolchain (1.70+ recommended)
- Cargo

## Build and Run

1. **Build the server:**

   ```bash```
   
   ```cargo build --bin server```

  

3. **Run the server:**
        ```bash```
   
     ```cargo run --bin server```
   
**Note:** To enable `tokio-console` integration, compile with the unstable flag: 
 ```bash```
 
``RUSTFLAGS="--cfg tokio_unstable" cargo run --bin server``


4. ### Run the Client
Run the client in a separate terminal: 
 ```bash```
 
 ```cargo run --example hello-redis```

### Run Unit Tests
Execute the async unit tests (which use paused time for speed):  
 ```bash```
 
```cargo test```


## What I Learned

Building this project provided deep insights into the complexities and power of asynchronous Rust:

- **The Power of `async/await`**: I learned how `async` functions return `Futures` that are lazy and do nothing until executed. Understanding the difference between defining a future and spawning a task (`tokio::spawn`) was crucial for managing concurrency.
- **The Critical Role of the Executor**: The Tokio runtime is not just a wrapper; it is the engine that polls futures. I learned that blocking the thread (e.g., with heavy CPU work or blocking I/O) stops the entire runtime from processing other tasks, emphasizing the need for non-blocking code.
- **Observability Challenges**: Setting up `tokio-console` taught me that configuration is key. It revealed that simply adding a dependency isn't enough; the `tokio_unstable` flag must be present at *compile time* for the runtime to expose the necessary internal data structures. It highlighted the fragility of debug tools and the importance of clean builds.
- **Testing Async Code**: I discovered that traditional testing fails with `tokio::time::sleep`. Using `#[tokio::test(start_paused = true)]` allowed me to write tests that run instantly, proving that time-based logic (like timeouts) works correctly without waiting seconds.
- **Mocking I/O**: Writing unit tests without network dependencies was a breakthrough. Using `tokio_test::io::Builder` to simulate network streams allowed me to test the protocol logic in isolation, making tests fast and reliable.
- **Yielding and Backpressure**: I learned that a future must "yield" at every `.await` to let the executor switch tasks. This concept is fundamental to how a single thread can handle thousands of concurrent connections efficiently.
- **Glossary Mastery**: Moving from abstract terms to concrete code, I now understand the practical difference between **Concurrency** (interleaving tasks) and **Parallelism** (running tasks simultaneously), and how **Backpressure** prevents memory exhaustion in high-load systems.

## Key Concepts Implemented

This project demonstrates several core Tokio and asynchronous Rust concepts:

| Concept | Implementation in `my-redis` |
| :--- | :--- |
| **Async/Await** | The `process` function uses `await` for non-blocking I/O on network sockets. |
| **Future** | Every `async fn` returns a `Future` that is polled by the Tokio executor. |
| **Executor** | The `#[tokio::main]` macro initializes the Tokio runtime (executor). |
| **Task** | Each client connection is spawned as a new task via `tokio::spawn`. |
| **Yielding** | The server yields control back to the executor at every `.await` point (e.g., `read_frame()`). |
| **Blocking** | No code blocks the thread; all I/O is non-blocking to prevent starvation. |
| **Stream** | The server reads a continuous stream of frames from the client socket. |
| **Channel** | While not heavily used here, the design supports actor patterns via channels for future scaling. |
| **Backpressure** | The bounded nature of the `TcpStream` buffer inherently handles backpressure for high-load scenarios. |
| **Actor** | A design pattern where an independent task manages a resource (like the database `HashMap` in this server). |

## Icon Attribution

The icons used in the project documentation and UI are sourced from **Flaticon**:

- **Source**: [Flaticon - Project Icons](https://www.flaticon.com/free-icons/project)
- **License**: Free for personal and commercial use with attribution.
- **Usage**: Download the `.svg` or `.png` files and place them in an `assets/` folder in your repository if you wish to display them in the README or documentation.

## Glossary Reference

For a deeper understanding of the terminology used in this project, refer to the **Tokio Glossary**:

- **Asynchronous**: Code that uses `async/await` to run many tasks concurrently on few threads.
- **Concurrency vs. Parallelism**: This project achieves **concurrency** (interleaving tasks) on a single thread, not necessarily parallelism (multiple cores).
- **Future**: A value representing a computation that may not have completed yet (e.g., the result of `read_frame()`).
- **Executor**: Tokio's runtime that repeatedly polls futures to drive progress.
- **Runtime**: The collection of utilities (executor, timers, I/O drivers) provided by Tokio.
- **Task**: An independent unit of execution managed by the runtime (created via `tokio::spawn`).
- **Spawning**: The act of creating a new task with `tokio::spawn`.
- **Yielding**: When a future returns control to the executor, allowing other tasks to run.
- **Blocking**: Waiting without yielding (e.g., a long CPU calculation), which is avoided here.
- **Stream**: An asynchronous iterator of values (e.g., a stream of Redis frames).
- **Channel**: A mechanism for sending messages between tasks (used in more complex actor patterns).
- **Backpressure**: A pattern (often via bounded channels or buffers) to prevent memory exhaustion under load.
- **Actor**: A design pattern where an independent task manages a resource (like the database `HashMap` in this server).

## License

MIT License.

// src/bin/tracing_demo.rs

use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, debug, warn, instrument, Level};
use tracing_subscriber;

// 1. Use the #[instrument] macro to automatically create a Span
// This function will log:
// - "Entering function" when called
// - "Exiting function" when returned
// - The values of `x` and `y` as fields
#[instrument(name = "add_numbers", skip())]
async fn add_numbers(x: u32, y: u32) -> u32 {
    info!("Starting addition calculation");
    sleep(Duration::from_millis(100)).await; // Simulate work
    let result = x + y;
    debug!("Result calculated: {}", result);
    result
}

// A more complex function with nested spans
#[instrument(name = "process_task", fields(id = 123, user = "alice"))]
async fn process_task(task_name: &str) -> Result<(), &'static str> {
    info!("Task started: {}", task_name);
    
    // Simulate some async work
    sleep(Duration::from_millis(200)).await;
    
    if task_name == "fail" {
        warn!("Task '{}' failed due to invalid input", task_name);
        return Err("Invalid input");
    }
    
    // Call another instrumented function (nested span)
    let sum = add_numbers(10, 20).await;
    info!("Task '{}' completed with sum: {}", task_name, sum);
    
    Ok(())
}

#[tokio::main]
async fn main() {
    // 2. Set up the Tracing Subscriber
    // This must be done ONCE at the start of the application
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG) // Set global log level
        .with_target(true)             // Show module path
        .with_thread_ids(true)         // Show thread ID (useful in multi-threaded)
        .compact()                     // Use a compact, single-line format
        .init();                       // Sets the global default

    info!("Application starting up...");

    // 3. Run tasks that emit traces
    info!("Spawning task 1");
    let handle1 = tokio::spawn(process_task("success_task"));

    info!("Spawning task 2");
    let handle2 = tokio::spawn(process_task("fail"));

    // Wait for tasks
    let _ = handle1.await;
    let _ = handle2.await;

    info!("Application shutting down.");
}
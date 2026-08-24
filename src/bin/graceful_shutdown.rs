// src/bin/graceful_shutdown.rs

use std::time::Duration;
use tokio::signal;
use tokio::time::sleep;
use tokio_util::task::TaskTracker;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    println!("=== Graceful Shutdown Demo ===\n");
    println!("Press Ctrl+C to trigger graceful shutdown.");

    // 1. Create a CancellationToken and a TaskTracker
    let token = CancellationToken::new();
    let tracker = TaskTracker::new();

        // 2. Spawn some background tasks
    for i in 0..5 {
        let task_token = token.clone();

        tracker.spawn(async move {
            println!("Task {} started.", i);
            
            tokio::select! {
                // If token is cancelled, start shutdown procedure
                _ = task_token.cancelled() => {
                    println!("Task {} received shutdown signal. Cleaning up...", i);
                    // Simulate cleanup
                    sleep(Duration::from_millis(1000)).await; // 1 second cleanup
                    println!("Task {} finished cleanup.", i);
                }
                // Normal completion (30 seconds)
                _ = sleep(Duration::from_millis(30000)) => {
                    println!("Task {} completed naturally.", i);
                }
            }
        });
    }

    // 3. Spawn a monitor task to detect Ctrl+C
    let monitor_token = token.clone();
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                println!("\n[Signal] Ctrl+C received. Initiating shutdown...");
                // Cancel all tasks
                monitor_token.cancel();
            }
            Err(err) => {
                eprintln!("Unable to listen for shutdown signal: {}", err);
                monitor_token.cancel();
            }
        }
    });

    // 4. Close the tracker to prevent new tasks
    tracker.close();

    // 5. Wait for all tasks to finish
    println!("Waiting for tasks to finish gracefully...");
    tracker.wait().await;

    println!("\n=== All tasks shut down successfully. Goodbye! ===");
}
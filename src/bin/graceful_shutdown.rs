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

    let token = CancellationToken::new();
    let tracker = TaskTracker::new();

    // Spawn tasks with 60-second durations
    for i in 0..5 {
        let task_token = token.clone();

        tracker.spawn(async move {
            println!("Task {} started.", i);
            
            tokio::select! {
                _ = task_token.cancelled() => {
                    println!("Task {} received shutdown signal. Cleaning up...", i);
                    sleep(Duration::from_millis(1000)).await; // 1 second cleanup
                    println!("Task {} finished cleanup.", i);
                }
                _ = sleep(Duration::from_millis(60000)) => { // 60 seconds
                    println!("Task {} completed naturally.", i);
                }
            }
        });
    }

    // Spawn monitor task
    let monitor_token = token.clone();
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                println!("\n[Signal] Ctrl+C received. Initiating shutdown...");
                monitor_token.cancel();
            }
            Err(err) => {
                eprintln!("Unable to listen for shutdown signal: {}", err);
                monitor_token.cancel();
            }
        }
    });

    tracker.close();
    println!("Waiting for tasks to finish gracefully...");
    tracker.wait().await;

    println!("\n=== All tasks shut down successfully. Goodbye! ===");
}
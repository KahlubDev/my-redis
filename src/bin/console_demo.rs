// src/bin/console_demo.rs

use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, instrument}; 

// A simple instrumented task
#[instrument(name = "worker_task", skip_all)]
async fn worker(id: u32) {
    info!("Worker {} starting", id);
    for i in 0..5 {
        info!("Worker {} processing item {}", id, i);
        sleep(Duration::from_millis(200)).await;
    }
    info!("Worker {} finished", id);
}

#[tokio::main]
async fn main() {
    // 1. Initialize the Console Subscriber
    console_subscriber::init();

    info!("Application starting. Open a second terminal and run: tokio-console");

    // 2. Spawn some tasks
    let mut handles = vec![];
    for i in 0..3 {
        let handle = tokio::spawn(worker(i));
        handles.push(handle);
    }

    // 3. Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    info!("All workers done. Goodbye!");

    // ✅ FIX: Keep the process alive for 30 seconds so you can inspect it
    info!("Waiting 30 seconds to let you inspect the console...");
    sleep(Duration::from_secs(30)).await;
    
    info!("Shutting down now.");
}
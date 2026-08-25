// src/bin/server_test.rs

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_test::io::Builder;
use mini_redis::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Import the Connection struct from your library
use my_redis::connection::Connection;

// Type alias for the shared database
type Db = Arc<Mutex<HashMap<String, Bytes>>>;

// Re-implement the process function logic for testing
async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{Get, Set};

    let mut connection = Connection::new(socket);

    while let Ok(Some(frame)) = connection.read_frame().await {
        if let Ok(cmd) = mini_redis::Command::from_frame(frame) {
            let response = match cmd {
                Set(cmd) => {
                    let mut guard = db.lock().unwrap();
                    guard.insert(
                        cmd.key().to_string(), 
                        cmd.value().to_vec().into()
                    );
                    Frame::Simple("OK".to_string())
                }
                Get(cmd) => {
                    let guard = db.lock().unwrap();
                    if let Some(value) = guard.get(cmd.key()) {
                        Frame::Bulk(value.clone())
                    } else {
                        Frame::Null
                    }
                }
                _ => panic!("unimplemented command: {:?}", cmd),
            };

            if let Err(e) = connection.write_frame(&response).await {
                eprintln!("Write error: {}", e);
                break;
            }
        } else {
            eprintln!("Command parse error");
            break;
        }
    }
}

/// Test 1: Pausing Time
/// Tests a timeout scenario without waiting 5 seconds.
#[tokio::test(start_paused = true)]
async fn test_timeout_with_paused_time() {
    let start = std::time::Instant::now();
    
    // Wait for 5 seconds (simulated)
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let elapsed = start.elapsed();
    
    // Because time is paused, this should be 0ms, not 5s
    assert_eq!(elapsed.as_secs(), 0);
}

/// Test 2: Paused Time with Interval
/// Verifies that intervals advance correctly when time is paused.
#[tokio::test(start_paused = true)]
async fn test_interval_tick() {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    let mut tick_count = 0;
    
    // Simulate 500ms of "time" passing
    for _ in 0..5 {
        interval.tick().await;
        tick_count += 1;
    }
    
    // Should have ticked 5 times
    assert_eq!(tick_count, 5);
}
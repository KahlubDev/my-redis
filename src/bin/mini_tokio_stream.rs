// src/bin/mini_tokio_stream.rs

use mini_redis::client;
use tokio::pin;
use tokio_stream::StreamExt;
use bytes::Bytes;
use std::time::Duration;

#[tokio::main]
async fn main() -> mini_redis::Result<()> {
    println!("=== Mini-Redis Stream Demo ===\n");

    // 1. CONNECT SUBSCRIBER FIRST
    // We connect and pin the stream BEFORE spawning the publisher.
    // This ensures we are ready to catch the very first message.
    let client = client::connect("127.0.0.1:6379").await?;
    let subscriber = client.subscribe(vec!["numbers".to_string()]).await?;
    let mut messages = subscriber.into_stream();
    pin!(messages);

    println!("Subscriber connected. Waiting for publisher...");

    // 2. SPAWN PUBLIGGER AFTER SUBSCRIBER IS READY
    tokio::spawn(async {
        // Small delay just to let the "Waiting" message print
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        if let Ok(mut pub_client) = client::connect("127.0.0.1:6379").await {
            let messages = vec!["1", "two", "3", "four", "five", "6"];
            for msg in messages {
                println!("Publisher sending: {}", msg);
                
                let bytes = Bytes::from(msg.to_string());
                if let Err(e) = pub_client.publish("numbers", bytes).await {
                    eprintln!("Publish error: {:?}", e);
                }
                
                // Wait 100ms between messages so we can see them clearly
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            println!("Publisher finished.");
        }
    });

    // 3. CHAIN ADAPTERS
    let processed = messages
        .filter(|msg| {
            match msg {
                Ok(m) if m.content.len() == 1 => true,
                _ => false,
            }
        })
        .take(3)
        .map(|msg| {
            let content = msg.unwrap().content;
            Bytes::copy_from_slice(&content)
        });

    pin!(processed);

    // 4. ITERATE
    println!("Listening...");
    let mut count = 0;
    while let Some(content) = processed.next().await {
        let text = std::str::from_utf8(&content).unwrap_or("<invalid>");
        println!("Received: {}", text);
        count += 1;
    }

    println!("\nProgram finished. Received {} valid messages.", count);

    Ok(())
}
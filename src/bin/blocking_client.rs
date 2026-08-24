// src/bin/blocking_client.rs

use mini_redis::client;
use bytes::Bytes;
use tokio::runtime::Runtime;

pub struct BlockingClient {
    inner: client::Client,
    rt: Runtime,
}

impl BlockingClient {
    pub fn connect(addr: &str) -> mini_redis::Result<Self> {
        let rt = Runtime::new()?;
        let inner = rt.block_on(client::connect(addr))?;
        Ok(BlockingClient { inner, rt })
    }

    pub fn get(&mut self, key: &str) -> mini_redis::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
    }

    pub fn set(&mut self, key: &str, value: Bytes) -> mini_redis::Result<()> {
        self.rt.block_on(self.inner.set(key, value))
    }

    pub fn publish(&mut self, channel: &str, message: Bytes) -> mini_redis::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }
}

pub struct BlockingSubscriber {
    inner: client::Subscriber,
    rt: Runtime,
}

impl BlockingSubscriber {
    pub fn next_message(&mut self) -> mini_redis::Result<Option<client::Message>> {
        self.rt.block_on(self.inner.next_message())
    }
    
    pub fn get_subscribed(&self) -> &[String] {
        self.inner.get_subscribed()
    }
}

impl BlockingClient {
    pub fn subscribe(self, channels: Vec<String>) -> mini_redis::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;
        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

fn main() -> mini_redis::Result<()> {
    println!("=== Blocking Client Demo ===\n");

    // 1. Connect
    let mut client = BlockingClient::connect("127.0.0.1:6379")?;
    println!("Connected to Redis (Synchronously).");

    // 2. Set/Get (Synchronous)
    let key = "hello";
    let value = Bytes::from("world");
    client.set(key, value.clone())?;
    println!("Set key '{}' to '{}'", key, std::str::from_utf8(&value).unwrap());
    
    let result = client.get(key)?;
    if let Some(val) = result {
        println!("Got key '{}' -> '{}'", key, std::str::from_utf8(&val).unwrap());
    }

    // 3. Subscribe FIRST
    let channel = "test-channel";
    let mut subscriber = client.subscribe(vec![channel.to_string()])?;
    println!("Subscribed to '{}'", channel);

    // 4. IMMEDIATELY publish from a NEW connection
    // This ensures the message arrives while the subscriber is waiting
    println!("Publishing message from a new connection...");
    let mut publisher = BlockingClient::connect("127.0.0.1:6379")?;
    let msg = Bytes::from("Hello from new connection!");
    let count = publisher.publish(channel, msg)?;
    println!("Published message (received by {} subscribers)", count);

    // 5. Receive
    println!("Waiting for message...");
    if let Some(msg) = subscriber.next_message()? {
        let text = std::str::from_utf8(&msg.content).unwrap_or("<invalid>");
        println!("Received: {}", text);
    }

    // 6. Publish again to show multiple messages
    println!("\nPublishing second message...");
    let msg2 = Bytes::from("Second message!");
    let count2 = publisher.publish(channel, msg2)?;
    println!("Published second message (received by {} subscribers)", count2);

    if let Some(msg) = subscriber.next_message()? {
        let text = std::str::from_utf8(&msg.content).unwrap_or("<invalid>");
        println!("Received: {}", text);
    }

    println!("\nDone!");
    Ok(())
}
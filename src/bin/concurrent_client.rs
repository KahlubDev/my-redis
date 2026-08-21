use mini_redis::client;
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

/// Multiple different commands are multiplexed over a single channel.
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

#[tokio::main]
async fn main() {
    // 1. Create the channel (mpsc)
    let (tx, mut rx) = mpsc::channel(32);

    // 2. Spawn the Manager Task
    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            use Command::*;
            match cmd {
                Get { key, resp } => {
                    let res = client.get(&key).await;
                    let _ = resp.send(res);
                }
                Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    let _ = resp.send(res);
                }
            }
        }
    });

    // 3. Clone the sender for multiple tasks
    let tx2 = tx.clone();

    // Task 1: GET "foo" (with a small delay to let SET finish first)
    let t1 = tokio::spawn(async move {
        // Add a tiny delay to ensure SET finishes first
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: "foo".to_string(),
            resp: resp_tx,
        };

        tx.send(cmd).await.unwrap();

        match resp_rx.await {
            Ok(Ok(Some(val))) => println!("GET 'foo' returned: {:?}", val),
            Ok(Ok(None)) => println!("GET 'foo' returned: None"),
            Ok(Err(e)) => println!("GET 'foo' error: {:?}", e),
            Err(_) => println!("GET 'foo' channel closed"),
        }
    });

    // Task 2: SET "foo" to "bar"
    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: resp_tx,
        };

        tx2.send(cmd).await.unwrap();

        match resp_rx.await {
            Ok(Ok(())) => println!("SET 'foo' to 'bar' succeeded"),
            Ok(Err(e)) => println!("SET 'foo' error: {:?}", e),
            Err(_) => println!("SET 'foo' channel closed"),
        }
    });

    // Wait for tasks
    t1.await.unwrap();
    t2.await.unwrap();
    
    // Wait for manager to finish
    manager.await.unwrap();
}
use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Type alias for the shared database
type Db = Arc<Mutex<HashMap<String, Bytes>>>;

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{Get, Set}; // Removed unused 'self'

    let mut connection = Connection::new(socket);

    // Fixed: Handle the Result properly
    while let Ok(Some(frame)) = connection.read_frame().await {
        // Handle command parsing errors gracefully
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bind to port 6379
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Custom Mini-Redis server listening on 127.0.0.1:6379");

    // Initialize the shared database
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        
        // Clone the handle to the database for the new task
        let db = db.clone();

        println!("New connection accepted");
        
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
    #[tokio::main]
async fn main() {
    // 1. Create the channel (mpsc)
    // Capacity of 32 allows some buffering if requests come in fast
    let (tx, mut rx) = mpsc::channel(32);

    // 2. Spawn the Manager Task
    // This task holds the single Redis connection
    let manager = tokio::spawn(async move {
        // Establish the connection
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        // Loop to receive commands
        while let Some(cmd) = rx.recv().await {
            use Command::*;
            match cmd {
                Get { key, resp } => {
                    let res = client.get(&key).await;
                    // Send result back (ignore error if receiver dropped)
                    let _ = resp.send(res);
                }
                Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    let _ = resp.send(res);
                }
            }
        }
    });

   #[tokio::main]
async fn main() {
    // 1. Create the channel (mpsc)
    // Capacity of 32 allows some buffering if requests come in fast
    let (tx, mut rx) = mpsc::channel(32);

    // 2. Spawn the Manager Task
    // This task holds the single Redis connection
    let manager = tokio::spawn(async move {
        // Establish the connection
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        // Loop to receive commands
        while let Some(cmd) = rx.recv().await {
            use Command::*;
            match cmd {
                Get { key, resp } => {
                    let res = client.get(&key).await;
                    // Send result back (ignore error if receiver dropped)
                    let _ = resp.send(res);
                }
                Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    let _ = resp.send(res);
                }
            }
        }
    });

        // Clone the sender so we have two independent handles
    let tx2 = tx.clone();

    // Task 1: GET "foo"
    let t1 = tokio::spawn(async move {
        // Create a oneshot channel for this specific request
        let (resp_tx, resp_rx) = oneshot::channel();
        
        let cmd = Command::Get {
            key: "foo".to_string(),
            resp: resp_tx,
        };

        // Send the command to the manager
        tx.send(cmd).await.unwrap();

        // Wait for the response
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

    // Wait for both tasks to finish
    t1.await.unwrap();
    t2.await.unwrap();
    
    // Wait for manager to finish (it won't until all senders are dropped)
    manager.await.unwrap();
}
}
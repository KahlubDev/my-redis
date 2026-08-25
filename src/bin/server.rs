use tokio::net::{TcpListener, TcpStream};
use my_redis::connection::Connection;
use mini_redis::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Import console-subscriber and tracing
use console_subscriber;
use tracing::info;

// Type alias for the shared database
type Db = Arc<Mutex<HashMap<String, Bytes>>>;

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{Get, Set};

    let mut connection = Connection::new(socket);

    info!("New connection established, processing commands");

    // Handle the Result properly
    while let Ok(Some(frame)) = connection.read_frame().await {
        if let Ok(cmd) = mini_redis::Command::from_frame(frame) {
            let response = match cmd {
                Set(cmd) => {
                    let mut guard = db.lock().unwrap();
                    let key = cmd.key().to_string();
                    info!("SET key: {}", key);
                    guard.insert(
                        key, 
                        cmd.value().to_vec().into()
                    );
                    Frame::Simple("OK".to_string())
                }
                Get(cmd) => {
                    let key = cmd.key().to_string();
                    let guard = db.lock().unwrap();
                    if let Some(value) = guard.get(cmd.key()) {
                        info!("GET key: {} -> found", key);
                        Frame::Bulk(value.clone())
                    } else {
                        info!("GET key: {} -> null", key);
                        Frame::Null
                    }
                }
                _ => {
                    info!("Unimplemented command: {:?}", cmd);
                    panic!("unimplemented command: {:?}", cmd);
                }
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
    // Initialize the console subscriber
    // This must be called before any async tasks are spawned
    console_subscriber::init();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    info!("Custom Mini-Redis server (with raw framing) listening on 127.0.0.1:6379");

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();

        info!("New connection accepted");
        
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}
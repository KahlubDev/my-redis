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
}
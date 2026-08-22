use tokio::net::{TcpListener, TcpStream};
use my_redis::connection::Connection;
use mini_redis::Frame;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Type alias for the shared database
type Db = Arc<Mutex<HashMap<String, Bytes>>>;

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{Get, Set};

    let mut connection = Connection::new(socket);

    // Handle the Result properly
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Custom Mini-Redis server (with raw framing) listening on 127.0.0.1:6379");

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();

        println!("New connection accepted");
        
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}
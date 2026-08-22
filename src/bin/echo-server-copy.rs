use tokio::io;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Bind to a port that is unlikely to conflict with your Redis server
    let listener = TcpListener::bind("127.0.0.1:6142").await?;
    println!("Echo server listening on 127.0.0.1:6142");

    loop {
        let (socket, _) = listener.accept().await?;

        // Split the socket into a reader and a writer
        // This allows us to pass them to io::copy independently
        let (mut rd, mut wr) = socket.into_split();

        // Spawn a task to copy data from reader to writer
        // io::copy will read from 'rd' and write to 'wr' until EOF
        tokio::spawn(async move {
            if let Err(e) = io::copy(&mut rd, &mut wr).await {
                eprintln!("Echo error: {}", e);
            }
        });
    }
}
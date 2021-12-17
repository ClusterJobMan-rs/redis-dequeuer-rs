use std::process::Stdio;
use std::io::ErrorKind;
//use std::io::Write;
//use std::time::Duration;
//use redis::AsyncCommands;
//use std::net::TcpStream;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream; 
use tokio::process::Command;
use tokio_util::codec::{FramedRead, LinesCodec};
use futures::prelude::*;

#[tokio::main]
async fn main() { //-> redis::RedisResult<()> {
    let rd_client = redis::Client::open("redis://127.0.0.1").unwrap();
    let mut rdconn = rd_client.get_multiplexed_async_connection().await.expect("could not estalbish redis connection");

    let mut stream: TcpStream;

    loop {
        match TcpStream::connect("127.0.0.1:33333").await {
            Err(err) => {
                match err.kind() {
                    ErrorKind::ConnectionRefused => { continue ;},
                    kind => panic!("Error occured: {:?}", kind),
                };
            },
            Ok(s) => {
                stream = s;
                break;
            }
        };
    }

    loop {
        let res: Vec<String> = redis::cmd("BLPOP")
            .arg(&["jobQueue", "0"])
            .query_async(&mut rdconn)
            .await
            .expect("could not execute redis command");

        //println!("{}", res[1]);

        let mut child = Command::new("bash")
            .arg("-c")
            .arg(&res[1])
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to execute command");
        let stdout = child.stdout.take().unwrap();
        let mut reader = FramedRead::new(stdout, LinesCodec::new());
        while let Some(line) = reader.next().await {
            //println!("{}", line.unwrap());
            stream.write(format!("{}\n", line.unwrap()).as_bytes()).await.expect("failed to write");
        }
    }

    //Ok(())
}

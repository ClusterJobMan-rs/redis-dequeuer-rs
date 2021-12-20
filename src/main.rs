use std::str;
use std::process::Stdio;
use std::io::ErrorKind;

use redis::FromRedisValue;
//use redis::AsyncCommands;
use redis::streams::{StreamReadOptions, StreamReadReply};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio_util::codec::{FramedRead, LinesCodec};
use futures::prelude::*;

#[tokio::main]
async fn main() { //-> redis::RedisResult<()> {
    let rd_client = redis::Client::open("redis://127.0.0.1").unwrap();
    let mut rdconn = rd_client.get_multiplexed_async_connection().await.expect("could not estalbish redis connection");

    loop {
        /*
        let res: Vec<String> = redis::cmd("BLPOP")
            .arg(&["jobQueue", "0"])
            .query_async(&mut rdconn)
            .await
            .expect("could not execute redis command");
            */

        //println!("{}", res[1]);

        let queue: StreamReadReply = redis::cmd("XREAD")
            .arg(&["BLOCK", "0", "STREAMS", "jobStream", "$"])
            .query_async(&mut rdconn)
            .await
            .expect("could not execute redis command");
        
        let id = queue.keys[0].ids[0].id.clone();
        
        match FromRedisValue::from_redis_value(&queue.keys[0].ids[0].map["script"]) {
            Ok(v) => {
                match v {
                    redis::Value::Data(d) => {
                        let mut child = Command::new("bash")
                            .arg("-c")
                            .arg(str::from_utf8(&d).unwrap())
                            .stdout(Stdio::piped())
                            .spawn()
                            .expect("Failed to execute command");
                        let stdout = child.stdout.take().unwrap();
                        let mut reader = FramedRead::new(stdout, LinesCodec::new());
                        while let Some(line) = reader.next().await {
                            //println!("{}", line.unwrap());
                            let _: Vec<u8> = redis::cmd("XADD")
                                .arg(&[format!("{}_output", id), "*".to_string(), "line".to_string(), line.unwrap()])
                                .query_async(&mut rdconn)
                                .await
                                .expect("could not execute redis command");
                        }
                    }
                    _ => {
                        panic!("err");
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                continue;
            }
        }
        

            /*
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
        */
    }

    //Ok(())
}

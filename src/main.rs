use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Error, Write};
use std::process::Stdio;
use std::str;

use redis::FromRedisValue;
//use redis::AsyncCommands;
use futures::prelude::*;
use redis::streams::StreamReadReply;
use tokio::process::Command;
use tokio_util::codec::{FramedRead, LinesCodec};

struct node {
    hostname: String,
    cores_total: u32,
    cores_free: u32,
}

#[tokio::main]
async fn main() {
    //-> redis::RedisResult<()> {
    let rd_client = redis::Client::open("redis://127.0.0.1").unwrap();
    let mut rdconn = rd_client
        .get_multiplexed_async_connection()
        .await
        .expect("could not estalbish redis connection");

    let cpu_cores: Vec<node> = Vec::new();

    let nodes = match redis::cmd("SMEMBERS")
        .arg(&["nodes"])
        .query_async(&mut rdconn)
        .await
        .expect("could not execute redis command")
    {
        redis::Value::Bulk(a) => a,
        _ => panic!("error"),
    };

    /*
    for i in nodes {
        i
    }
    */

    loop {
        let queue: StreamReadReply = redis::cmd("XREAD")
            .arg(&["BLOCK", "0", "STREAMS", "jobStream", "$"])
            .query_async(&mut rdconn)
            .await
            .expect("could not execute redis command");
        let id = queue.keys[0].ids[0].id.clone();

        let parallel_num: u32 =
            match FromRedisValue::from_redis_value(&queue.keys[0].ids[0].map["num"]) {
                Ok(v) => match v {
                    redis::Value::Data(d) => {
                        let s = str::from_utf8(&d).unwrap();
                        match s.parse() {
                            Ok(n) => n,
                            Err(_) => continue,
                        }
                    }
                    _ => {
                        continue;
                    }
                },
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    continue;
                }
            };
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
                            match line {
                                Ok(l) => {
                                    let _: Vec<u8> = redis::cmd("XADD")
                                        .arg(&[
                                            format!("{}_output", id),
                                            "*".to_string(),
                                            "line".to_string(),
                                            l,
                                        ])
                                        .query_async(&mut rdconn)
                                        .await
                                        .expect("could not execute redis command");
                                }
                                Err(e) => {
                                    eprintln!("Error: {:?}", e);
                                }
                            }
                        }
                    }
                    _ => {
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                continue;
            }
        }
    }

    //Ok(())
}

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Error, Write};
use std::path::PathBuf;
use std::process::Stdio;
use std::str;

use redis::FromRedisValue;
//use redis::AsyncCommands;
use futures::prelude::*;
use redis::streams::StreamReadReply;
use tokio::process::Command;
use tokio_util::codec::{FramedRead, LinesCodec};

struct Node {
    hostname: String,
    cores_total: u32,
    cores_free: u32,
}

fn create_hostfile(mut dir: String) {
    dir = dir.trim_end_matches("/").to_string();
    let path = PathBuf::from(dir + "/hosts");
    let mut file = match File::create(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error: {:?}", e);
            return;
        }
    };
}

#[tokio::main]
async fn main() {
    //-> redis::RedisResult<()> {
    let rd_client = redis::Client::open("redis://127.0.0.1").unwrap();
    let mut rdconn = rd_client
        .get_multiplexed_async_connection()
        .await
        .expect("could not estalbish redis connection");

    let mut cpu_cores: Vec<Node> = Vec::new();

    let nodes = match redis::cmd("SMEMBERS")
        .arg(&["nodes"])
        .query_async(&mut rdconn)
        .await
        .expect("could not execute redis command")
    {
        redis::Value::Bulk(a) => a,
        _ => panic!("error"),
    };

    for i in nodes {
        let hostname = match FromRedisValue::from_redis_value(&i) {
            Ok(redis::Value::Data(d)) => match str::from_utf8(&d) {
                Ok(s) => s.to_string(),
                Err(e) => panic!("error: {:?}", e),
            },
            _ => continue,
        };
        let cores: u32 = match redis::cmd("HGET")
            .arg(&[format!("{}_status", hostname), "cpu_num".to_string()])
            .query_async(&mut rdconn)
            .await
        {
            Ok(redis::Value::Data(d)) => match str::from_utf8(&d) {
                Ok(s) => match s.parse() {
                    Ok(n) => n,
                    Err(e) => panic!("error: {:?}", e),
                },
                Err(e) => panic!("error: {:?}", e),
            },
            default => panic!("error: {:?}", default),
        };

        cpu_cores.push(Node {
            hostname: hostname,
            cores_total: cores,
            cores_free: cores,
        });
    }

    loop {
        let queue: StreamReadReply = redis::cmd("XREAD")
            .arg(&["BLOCK", "0", "STREAMS", "jobStream", "$"])
            .query_async(&mut rdconn)
            .await
            .expect("could not execute redis command");
        let id = queue.keys[0].ids[0].id.clone();
        println!("a");

        let work_dir: String =
            match FromRedisValue::from_redis_value(&queue.keys[0].ids[0].map["dir"]) {
                Ok(v) => match v {
                    redis::Value::Data(d) => str::from_utf8(&d)
                        .unwrap()
                        .to_string()
                        .trim_end_matches("/")
                        .to_string(),
                    _ => continue,
                },
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    continue;
                }
            };
        println!("b");

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
        println!("c");
        let path = PathBuf::from(work_dir.clone() + "/hosts");
        let mut file = match File::create(path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return;
            }
        };

        let hosts: Vec<String> = Vec::new();
        for n in 1..cpu_cores.len() as u32 {
            let thread_per_cpu: u32 = match parallel_num % n {
                0 => parallel_num / n,
                _ => break,
            };
            for i in 0..cpu_cores.len() {
                if cpu_cores[i].cores_free >= thread_per_cpu {
                    cpu_cores[i].cores_free -= thread_per_cpu;
                    file.write_all(
                        format!("{} slots={}", cpu_cores[i].hostname, thread_per_cpu).as_bytes(),
                    )
                    .unwrap();
                    break;
                }
            }
        }
        //create_hostfile(work_dir.clone());
        match FromRedisValue::from_redis_value(&queue.keys[0].ids[0].map["script"]) {
            Ok(v) => {
                match v {
                    redis::Value::Data(d) => {
                        let mut child = Command::new("bash")
                            .arg("-c")
                            .arg(format!(
                                "cd {};echo {};{}",
                                work_dir,
                                parallel_num,
                                str::from_utf8(&d).unwrap()
                            ))
                            .stdout(Stdio::piped())
                            .spawn()
                            .expect("Failed to execute command");
                        let stdout = child.stdout.take().unwrap();
                        let mut reader = FramedRead::new(stdout, LinesCodec::new());
                        while let Some(line) = reader.next().await {
                            //println!("{}", line.unwrap());
                            match line {
                                Ok(l) => {
                                    println!("{}", &l);
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

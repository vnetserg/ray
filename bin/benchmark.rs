#[macro_use]
extern crate log;

use ray::client::RayClient;

use tokio::{task, time};

use futures::{select, channel::mpsc, stream::StreamExt};

use log::LevelFilter;
use simplelog::{SimpleLogger, Config};

use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = SimpleLogger::init(LevelFilter::Info, Config::default());

    let mut client = RayClient::connect("127.0.0.1", 39172).await?;
    client.set("hello".into(), "world".into()).await?;

    let (sender, mut receiver) = mpsc::unbounded();

    for _ in 0..256 {
        let task_sender = sender.clone();
        task::spawn(async move {
            let mut client = RayClient::connect("127.0.0.1", 39172).await.unwrap();
            loop {
                let now = Instant::now();
                client.get("hello".into()).await.unwrap();
                let elapsed = now.elapsed();
                task_sender.unbounded_send(elapsed.as_secs_f64()).unwrap();
            }
        });
    }

    let (interval_sender, mut interval_receiver) = mpsc::unbounded();
    task::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            interval_sender.unbounded_send(()).unwrap();
        }
    });

    let mut latencies = vec![];
    loop {
        select! {
            value = receiver.next() => {
                latencies.push(value.unwrap());
            }
            _ = interval_receiver.next() => {
                let requests = latencies.len();
                let median = if requests > 0 {
                    let sum: f64 = latencies.iter().sum();
                    sum / (requests as f64)
                } else {
                    0.
                };
                info!("RPS: {} (median latency: {})", requests, median);
                latencies.clear();
            }
        }
    }
}

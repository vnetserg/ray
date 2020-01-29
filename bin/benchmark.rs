#[macro_use]
extern crate log;

use ray::{client::RayClient, server::Config};

use clap::{value_t_or_exit, App, Arg};

use tokio::{task, time};

use futures::{select, channel::mpsc, stream::StreamExt};

use log::LevelFilter;
use simplelog::SimpleLogger;

use std::time::{Duration, Instant};

const ABOUT: &str = "Ray benchmark tool";

struct Arguments {
    address: String,
    port: u16,
    clients: u16,
}

fn parse_arguments() -> Arguments {
    let default_port_string = Config::default().rpc.port.to_string();
    let parser = App::new("ray-benchmark")
        .version(ray::VERSION)
        .author(ray::AUTHORS)
        .about(ABOUT)
        .arg(
            Arg::with_name("address")
                .short("a")
                .long("address")
                .value_name("ADDRESS")
                .help("rayd host address")
                .takes_value(true)
                .default_value("localhost"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("rayd TCP port")
                .takes_value(true)
                .default_value(&default_port_string),
        )
        .arg(
            Arg::with_name("clients")
                .short("c")
                .long("clients")
                .value_name("COUNT")
                .help("concurrent clients count")
                .takes_value(true)
                .default_value("256"),
        );

    let matches = parser.get_matches();

    let address = matches.value_of("address").unwrap().to_string();
    let port = value_t_or_exit!(matches, "port", u16);
    let clients = value_t_or_exit!(matches, "clients", u16);

    Arguments {
        address,
        port,
        clients,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SimpleLogger::init(LevelFilter::Info, simplelog::Config::default()).unwrap();

    let arguments = parse_arguments();

    let mut client = RayClient::connect(&arguments.address, arguments.port).await?;
    client.set("hello".into(), "world".into()).await?;

    let (sender, mut receiver) = mpsc::unbounded();

    info!("Creating {} clients", arguments.clients);

    for _ in 0..arguments.clients {
        let task_sender = sender.clone();
        let address = arguments.address.clone();
        let port = arguments.port;
        task::spawn(async move {
            let mut client = RayClient::connect(&address, port).await.unwrap();
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
                info!("RPS: {} (average latency: {})", requests, median);
                latencies.clear();
            }
        }
    }
}

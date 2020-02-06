use crate::client::{RayClient, RayClientConnector};

use tokio::{runtime, time};

use futures::{channel::mpsc, select, stream::StreamExt};

use std::{
    error::Error,
    time::{Duration, Instant},
};

#[tonic::async_trait]
pub trait Benchmark: 'static {
    const NAME: &'static str;

    type Message: Send + 'static;

    async fn do_task(
        client: RayClient,
        key: Vec<u8>,
        value: Vec<u8>,
        delay: Duration,
        sender: mpsc::UnboundedSender<Self::Message>,
    );

    fn handle_message(&mut self, message: Self::Message);
    fn handle_tick(&mut self);
}

#[derive(Debug)]
pub struct BenchmarkConfig {
    pub address: String,
    pub port: u16,
    pub threads: u16,
    pub tasks: u16,
    pub idle: u16,
    pub key_length: usize,
    pub value_length: usize,
    pub delay: Duration,
}

#[derive(Default)]
pub struct SimpleReadBenchmark {
    latencies: Vec<f64>,
}

#[tonic::async_trait]
impl Benchmark for SimpleReadBenchmark {
    const NAME: &'static str = "simple read";

    type Message = f64;

    async fn do_task(
        mut client: RayClient,
        key: Vec<u8>,
        value: Vec<u8>,
        delay: Duration,
        sender: mpsc::UnboundedSender<Self::Message>,
    ) {
        client
            .set(key.clone(), value)
            .await
            .unwrap_or_else(|err| panic!("Set failed: {}", err));

        loop {
            let now = Instant::now();
            if let Err(err) = client.get(key.clone()).await {
                error!("Failed to get key '{:?}': {}", key, err);
                continue;
            }
            let elapsed = now.elapsed();
            sender.unbounded_send(elapsed.as_secs_f64()).unwrap();

            time::delay_for(delay).await;
        }
    }

    fn handle_message(&mut self, message: Self::Message) {
        self.latencies.push(message);
    }

    fn handle_tick(&mut self) {
        let requests = self.latencies.len();
        let average = if requests > 0 {
            let sum: f64 = self.latencies.iter().sum();
            sum / (requests as f64)
        } else {
            0.
        };
        info!("RPS: {} (average latency: {})", requests, average);
        self.latencies.clear();
    }
}

#[derive(Default)]
pub struct SimpleWriteBenchmark {
    latencies: Vec<f64>,
}

#[tonic::async_trait]
impl Benchmark for SimpleWriteBenchmark {
    const NAME: &'static str = "simple write";

    type Message = f64;

    async fn do_task(
        mut client: RayClient,
        key: Vec<u8>,
        value: Vec<u8>,
        delay: Duration,
        sender: mpsc::UnboundedSender<Self::Message>,
    ) {
        loop {
            let now = Instant::now();
            if let Err(err) = client.set(key.clone(), value.clone()).await {
                error!("Set failed: {}", err);
                continue;
            }
            let elapsed = now.elapsed();

            sender.unbounded_send(elapsed.as_secs_f64()).unwrap();

            time::delay_for(delay).await;
        }
    }

    fn handle_message(&mut self, message: Self::Message) {
        self.latencies.push(message);
    }

    fn handle_tick(&mut self) {
        let requests = self.latencies.len();
        let average = if requests > 0 {
            let sum: f64 = self.latencies.iter().sum();
            sum / (requests as f64)
        } else {
            0.
        };
        info!("RPS: {} (average latency: {})", requests, average);
        self.latencies.clear();
    }
}

pub fn run_benchmark<B: Benchmark>(benchmark: B, config: BenchmarkConfig) {
    let num_threads = if config.threads > 0 {
        config.threads as usize
    } else {
        num_cpus::get()
    };
    let mut runtime = runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(num_threads)
        .thread_name("ray-bench-worker")
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");

    runtime
        .block_on(run_benchmark_inner(benchmark, config))
        .unwrap_or_else(|err| panic!("Benchmark failed: {}", err))
}

async fn run_benchmark_inner<B: Benchmark>(
    mut benchmark: B,
    config: BenchmarkConfig,
) -> Result<(), Box<dyn Error>> {
    info!("Starting benchmark: {}", B::NAME);
    info!("Benchmark config: {:?}", config);

    let connector = RayClientConnector::new(config.address.clone(), config.port);

    for _ in 0..config.idle {
        let idle_connector = connector.clone();
        tokio::spawn(async move {
            let mut client = idle_connector
                .connect()
                .await
                .unwrap_or_else(|err| panic!("Connection failed: {}", err));
            time::delay_for(Duration::from_secs(100_500)).await;
            client.get(vec![]).await.unwrap();
        });
    }

    let (sender, mut receiver) = mpsc::unbounded();
    for _ in 0..config.tasks {
        let task_sender = sender.clone();
        let task_connector = connector.clone();
        let BenchmarkConfig {
            key_length,
            value_length,
            delay,
            ..
        } = config;
        tokio::spawn(async move {
            let task_client = task_connector
                .connect()
                .await
                .unwrap_or_else(|err| panic!("Connection failed: {}", err));
            let key = random_bytes(key_length);
            let value = random_bytes(value_length);
            B::do_task(task_client, key, value, delay, task_sender).await
        });
    }

    let (interval_sender, mut interval_receiver) = mpsc::unbounded();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            interval_sender.unbounded_send(()).unwrap();
        }
    });

    loop {
        select! {
            maybe_message = receiver.next() => {
                let message = maybe_message.expect("All tasks are dead");
                benchmark.handle_message(message);
            }
            _ = interval_receiver.next() => benchmark.handle_tick(),
        }
    }
}

fn random_bytes(length: usize) -> Vec<u8> {
    (0..length).map(|_| rand::random::<u8>()).collect()
}

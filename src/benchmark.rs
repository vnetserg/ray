use crate::client::RayClient;

use tokio::{
    runtime,
    time,
};

use futures::{channel::mpsc, select, stream::StreamExt};

use std::{
    error::Error,
    time::{Duration, Instant},
};

#[tonic::async_trait]
pub trait Benchmark: 'static {
    const NAME: &'static str;
    type Message: Send + 'static;

    async fn setup(&mut self, client: RayClient);
    async fn do_task(client: RayClient, sender: mpsc::UnboundedSender<Self::Message>);

    fn handle_message(&mut self, message: Self::Message);
    fn handle_tick(&mut self);
}

pub struct BenchmarkConfig {
    pub address: String,
    pub port: u16,
    pub threads: u16,
    pub tasks: u16,
}

#[derive(Default)]
pub struct SimpleReadBenchmark {
    latencies: Vec<f64>,
}

#[tonic::async_trait]
impl Benchmark for SimpleReadBenchmark {
    const NAME: &'static str = "simple read";
    type Message = f64;

    async fn setup(&mut self, mut client: RayClient) {
        client
            .set("hello".into(), "world".into())
            .await
            .unwrap_or_else(|err| panic!("failed to set key: {}", err));
    }

    async fn do_task(
        mut client: RayClient,
        sender: mpsc::UnboundedSender<Self::Message>,
    ) {
        loop {
            let now = Instant::now();
            if let Err(err) = client.get("hello".into()).await {
                error!("Failed to get key 'hello': {}", err);
                continue;
            }
            let elapsed = now.elapsed();
            sender.unbounded_send(elapsed.as_secs_f64()).unwrap();
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

async fn run_benchmark_inner<B: Benchmark>(mut benchmark: B, config: BenchmarkConfig)
-> Result<(), Box<dyn Error>> {
    info!("Starting benchmark: {}", B::NAME);

    let client = RayClient::connect(&config.address, config.port).await?;
    benchmark.setup(client).await;

    let (sender, mut receiver) = mpsc::unbounded();
    for _ in 0..config.tasks {
        let task_client = RayClient::connect(&config.address, config.port).await?;
        let task_sender = sender.clone();
        tokio::spawn(B::do_task(task_client, task_sender));
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

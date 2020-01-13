use tokio::runtime;

use prost::Message;

use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    select,
    sink::SinkExt,
    stream::StreamExt,
};

use std::{
    collections::BinaryHeap,
    cmp::{
        self,
        Ordering,
    },
    io::{
        self,
        Write,
        Read,
    },
    future::Future,
    thread,
};


pub trait Machine : Default + Send + 'static {
    type Mutation : Message + Default;
    type Query : Send;
    type Status : Send;

    fn apply_mutation(&mut self, mutation: Self::Mutation);
    fn query_state(&self, query: Self::Query) -> Self::Status;
}

pub trait PersistentLog : Read + Write + Send + 'static {
    fn persist(&mut self) -> io::Result<()>;
}

pub fn run<M: Machine, L: PersistentLog>(log: L) -> PersistentMachineHandle<M> {
    let (log_sender, log_receiver) = mpsc::channel(1000);
    let (machine_sender, machine_receiver) = mpsc::channel(1000);
    let (mutation_sender, mutation_receiver) = mpsc::channel(1000);
    run_in_dedicated_thread(async move {
        let mut log_service = LogService::<L, M::Mutation> {
            log,
            mutation_sender,
            request_receiver: log_receiver,
            persisted_epoch: 0,
        };
        log_service.recover().await;
        log_service.serve().await;
    });
    run_in_dedicated_thread(async move {
        let mut machine_service = MachineService {
            machine: M::default(),
            mutation_receiver,
            request_receiver: machine_receiver,
            epoch: 0,
            request_queue: BinaryHeap::new(),
        };
        machine_service.serve().await;
    });
    PersistentMachineHandle::<M> {
        log_sender,
        machine_sender,
    }
}

fn run_in_dedicated_thread<T: Future + Send + 'static>(task: T) {
    thread::spawn(move || {
        let mut rt = runtime::Builder::new()
            .basic_scheduler()
            .build()
            .expect("runtime creation failed");
        rt.block_on(task);
        panic!("Service terminated (run in dedicated thread)");
    });
}


enum LogServiceRequest<U: Message> {
    PersistMutation {
        mutation: U,
        notify: oneshot::Sender<()>,
    },
    GetPersistedEpoch(oneshot::Sender<u64>),
}

struct MachineServiceRequest<M: Machine> {
    query: M::Query,
    min_epoch: u64,
    result: oneshot::Sender<M::Status>,
}

impl<M: Machine> cmp::PartialEq for MachineServiceRequest<M> {
    fn eq(&self, other: &Self) -> bool {
        self.min_epoch == other.min_epoch
    }
}

impl<M: Machine> cmp::Eq for MachineServiceRequest<M> { }

impl<M: Machine> cmp::PartialOrd for MachineServiceRequest<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.min_epoch.cmp(&other.min_epoch).reverse())
    }
}

impl<M: Machine> cmp::Ord for MachineServiceRequest<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Clone)]
pub struct PersistentMachineHandle<M: Machine> {
    log_sender: mpsc::Sender<LogServiceRequest<M::Mutation>>,
    machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
}

impl<M: Machine> PersistentMachineHandle<M> {
    pub async fn apply_mutation(&mut self, mutation: M::Mutation) {
        let (sender, receiver) = oneshot::channel();
        let request = LogServiceRequest::PersistMutation {
            mutation,
            notify: sender,
        };
        self.log_sender.send(request).await.expect("log_receiver dropped");
        receiver.await.expect("sender dropped");
    }

    pub async fn query_state(&mut self, query: M::Query) -> M::Status {
        let epoch = self.get_persisted_epoch().await;
        self.query_state_after(query, epoch).await
    }

    async fn get_persisted_epoch(&mut self) -> u64 {
        let (sender, receiver) = oneshot::channel();
        let request = LogServiceRequest::GetPersistedEpoch(sender);
        self.log_sender.send(request).await.expect("log_receiver dropped");
        receiver.await.expect("sender dropped")
    }

    async fn query_state_after(&mut self, query: M::Query, epoch: u64) -> M::Status {
        let (sender, receiver) = oneshot::channel();
        let request = MachineServiceRequest::<M> {
            query,
            min_epoch: epoch,
            result: sender,
        };
        self.machine_sender.send(request).await.expect("machine_receiver dropped");
        receiver.await.expect("sender dropped")
    }
}


struct LogService<L: PersistentLog, U: Message + Default> {
    log: L,
    mutation_sender: mpsc::Sender<U>,
    request_receiver: mpsc::Receiver<LogServiceRequest<U>>,
    persisted_epoch: u64,
}

impl<L: PersistentLog, U: Message + Default> LogService<L, U> {
    pub async fn recover(&mut self) {
        loop {
            let len = match self.read_u32() {
                Some(n) => n,
                None => return,
            };
            let mutation = self.read_mutation(len as usize);
            self.persisted_epoch += 1;
            self.mutation_sender.send(mutation).await.expect("mutation receiver dropped");
        }
    }

    fn read_u32(&mut self) -> Option<u32> {
        let mut buf: [u8; 4] = [0; 4];
        if let Err(err) = self.log.read_exact(&mut buf) {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            } else {
                panic!("Failed to read PersistentLog: {}", err);
            }
        }
        Some(unsafe { std::mem::transmute(buf) })
    }

    fn read_mutation(&mut self, len: usize) -> U {
        let mut buf = vec![0; len];
        self.log.read_exact(&mut buf)
            .unwrap_or_else(|err| panic!("Failed to read PersistentLog: {}", err));
        U::decode(buf).unwrap_or_else(|err| panic!("Failed to parse mutation: {}", err))
    }

    pub async fn serve(&mut self) {
        loop {
            let mut mutations = vec![];
            let mut notifiers = vec![];

            for i in 0..100 {
                let request = if i == 0i32 {
                    self.request_receiver.next().await.expect("all request senders dropped")
                } else {
                    match self.request_receiver.try_next() {
                        Ok(Some(req)) => req,
                        Ok(None) => panic!("PersistentLog request_receiver stream terminated"),
                        Err(_) => break,
                    }
                };
                match request {
                    LogServiceRequest::GetPersistedEpoch(response) => {
                        response.send(self.persisted_epoch).ok(); // Ignore error
                    },
                    LogServiceRequest::PersistMutation{ mutation, notify } => {
                        mutations.push(mutation);
                        notifiers.push(notify);
                    },
                }
            }

            for mutation in mutations.iter() {
                let len = mutation.encoded_len();
                self.write_u32(len as u32);
                self.write_mutation(mutation, len);
            }

            self.log.persist().unwrap_or_else(|err| panic!("Failed to persist log: {}", err));

            for notify in notifiers.into_iter() {
                notify.send(()).ok(); // Ignore error
            }

            for mutation in mutations.into_iter() {
                self.mutation_sender.send(mutation).await
                    .expect("PersistentLog mutation_sender failed");
            }
        }
    }

    fn write_u32(&mut self, value: u32) {
        let buf: [u8; 4] = unsafe { std::mem::transmute(value) };
        self.log.write_all(&buf).unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
    }

    fn write_mutation(&mut self, mutation: &U, len: usize) {
        let mut buf = vec![0; len];
        mutation.encode(&mut buf).unwrap_or_else(|err| panic!("Failed to encode mutation: {}", err));
        self.log.write_all(&buf).unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
    }
}


struct MachineService<M: Machine> {
    machine: M,
    mutation_receiver: mpsc::Receiver<M::Mutation>,
    request_receiver: mpsc::Receiver<MachineServiceRequest<M>>,
    epoch: u64,
    request_queue: BinaryHeap<MachineServiceRequest<M>>,
}

impl<M: Machine> MachineService<M> {
    pub async fn serve(&mut self) {
        loop {
            select! {
                mutation = self.mutation_receiver.next() => {
                    let mutation = mutation.expect("MachineService mutation_receiver terminated");
                    self.handle_mutation_request(mutation);
                },
                request = self.request_receiver.next() => {
                    let request = request.expect("MachineService request_receiver terminated");
                    self.handle_query_request(request);
                },
            }
        }
    }

    fn handle_mutation_request(&mut self, mutation: M::Mutation) {
        self.machine.apply_mutation(mutation);
        self.epoch += 1;
        while !self.request_queue.is_empty()
            && self.epoch >= self.request_queue.peek().unwrap().min_epoch
        {
            let request = self.request_queue.pop().unwrap();
            let status = self.machine.query_state(request.query);
            request.result.send(status).ok();
        }
    }

    fn handle_query_request(&mut self, request: MachineServiceRequest<M>) {
        if self.epoch >= request.min_epoch {
            let status = self.machine.query_state(request.query);
            request.result.send(status).ok();
        } else {
            self.request_queue.push(request);
        }
    }
}

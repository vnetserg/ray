use super::journal_service::JournalServiceRequest;

use prost::Message;

use futures::{
    channel::{mpsc, oneshot},
    sink::SinkExt,
    stream::StreamExt,
};

use std::{
    cmp::{self, Ordering},
    collections::BinaryHeap,
    fmt::Display,
    io::{self, Read, Write},
};

pub trait Machine: Default + Clone + Send + 'static {
    type Mutation: Message + Default + Clone + Display;
    type Query: Send;
    type Status: Send;

    fn apply_mutation(&mut self, mutation: Self::Mutation);
    fn query_state(&self, query: Self::Query) -> Self::Status;
    fn write_snapshot<T: Write>(&self, writer: &mut T) -> io::Result<()>;
    fn from_snapshot<T: Read>(reader: &mut T) -> io::Result<Self>;
}

pub enum MachineServiceRequest<M: Machine> {
    Query {
        query: M::Query,
        min_epoch: u64,
        result: oneshot::Sender<M::Status>,
    },
    Proposal {
        mutation: M::Mutation,
        epoch: u64,
    },
}

#[derive(Clone)]
pub struct MachineServiceHandle<M: Machine> {
    journal_sender: mpsc::Sender<JournalServiceRequest<M::Mutation>>,
    machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
}

impl<M: Machine> MachineServiceHandle<M> {
    pub fn new(
        journal_sender: mpsc::Sender<JournalServiceRequest<M::Mutation>>,
        machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
    ) -> Self {
        Self {
            journal_sender,
            machine_sender,
        }
    }

    pub async fn apply_mutation(&mut self, mutation: M::Mutation) {
        let (sender, receiver) = oneshot::channel();
        let request = JournalServiceRequest::PersistMutation {
            mutation,
            notify: sender,
        };
        self.journal_sender
            .send(request)
            .await
            .expect("MachinsServiceHandle journal_sender failed");
        receiver.await.expect("sender dropped");
    }

    pub async fn query_state(&mut self, query: M::Query) -> M::Status {
        let epoch = self.get_persisted_epoch().await;
        self.query_state_after(query, epoch).await
    }

    async fn get_persisted_epoch(&mut self) -> u64 {
        let (sender, receiver) = oneshot::channel();
        let request = JournalServiceRequest::GetPersistedEpoch(sender);
        self.journal_sender
            .send(request)
            .await
            .expect("MachinsServiceHandle journal_sender failed");
        receiver.await.expect("sender dropped")
    }

    async fn query_state_after(&mut self, query: M::Query, epoch: u64) -> M::Status {
        let (sender, receiver) = oneshot::channel();
        let request = MachineServiceRequest::Query {
            query,
            min_epoch: epoch,
            result: sender,
        };
        self.machine_sender
            .send(request)
            .await
            .expect("machine_receiver dropped");
        receiver.await.expect("sender dropped")
    }
}

struct QueryPqItem<M: Machine> {
    query: M::Query,
    min_epoch: u64,
    result: oneshot::Sender<M::Status>,
}

impl<M: Machine> cmp::PartialEq for QueryPqItem<M> {
    fn eq(&self, other: &Self) -> bool {
        self.min_epoch == other.min_epoch
    }
}

impl<M: Machine> cmp::Eq for QueryPqItem<M> {}

impl<M: Machine> cmp::PartialOrd for QueryPqItem<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // NB: reverse is needed for min-heap
        Some(self.min_epoch.cmp(&other.min_epoch).reverse())
    }
}

impl<M: Machine> cmp::Ord for QueryPqItem<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct MachineService<M: Machine> {
    machine: M,
    request_receiver: mpsc::Receiver<MachineServiceRequest<M>>,
    epoch: u64,
    query_queue: BinaryHeap<QueryPqItem<M>>,
}

impl<M: Machine> MachineService<M> {
    pub fn new(
        machine: M,
        request_receiver: mpsc::Receiver<MachineServiceRequest<M>>,
        epoch: u64,
    ) -> Self {
        Self {
            machine,
            request_receiver,
            epoch,
            query_queue: BinaryHeap::new(),
        }
    }

    pub async fn serve(&mut self) {
        loop {
            match self
                .request_receiver
                .next()
                .await
                .expect("MachineService request_receiver failed")
            {
                MachineServiceRequest::Proposal { mutation, epoch } => {
                    self.handle_proposal(mutation, epoch).await;
                }
                MachineServiceRequest::Query {
                    query,
                    min_epoch,
                    result,
                } => {
                    self.handle_query(query, min_epoch, result);
                }
            }
        }
    }

    async fn handle_proposal(&mut self, mutation: M::Mutation, epoch: u64) {
        assert_eq!(epoch, self.epoch + 1);
        debug!(
            "Applying mutation: {} (new epoch: {})",
            mutation,
            self.epoch + 1
        );
        self.machine.apply_mutation(mutation);
        self.epoch += 1;

        while !self.query_queue.is_empty()
            && self.epoch >= self.query_queue.peek().unwrap().min_epoch
        {
            let QueryPqItem { query, result, .. } = self.query_queue.pop().unwrap();
            let status = self.machine.query_state(query);
            result.send(status).ok();
        }
    }

    fn handle_query(
        &mut self,
        query: M::Query,
        min_epoch: u64,
        result: oneshot::Sender<M::Status>,
    ) {
        if self.epoch >= min_epoch {
            let status = self.machine.query_state(query);
            result.send(status).ok();
        } else {
            let pq_item = QueryPqItem {
                query,
                min_epoch,
                result,
            };
            self.query_queue.push(pq_item);
        }
    }
}

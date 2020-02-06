use super::journal_service::JournalServiceRequest;

use crate::{
    errors::*,
    util::{Traced, ProfiledReceiver, ProfiledSender},
};

use prost::Message;

use tokio::sync::oneshot;

use metrics::{counter, gauge};

use std::{
    cmp,
    collections::BinaryHeap,
    fmt::{self, Debug, Display},
    io::{Read, Write},
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
};

pub trait Machine: Default + Clone + Send + 'static {
    type Mutation: Message + Default + Clone + Display;
    type Query: Send;
    type Status: Send;

    fn apply_mutation(&mut self, mutation: Self::Mutation);
    fn query_state(&self, query: Self::Query) -> Self::Status;
    fn write_snapshot<T: Write>(&self, writer: &mut T) -> Result<()>;
    fn from_snapshot<T: Read>(reader: &mut T) -> Result<Self>;
}

pub enum MachineServiceRequest<M: Machine> {
    Query {
        query: Traced<M::Query>,
        min_epoch: u64,
        result: oneshot::Sender<M::Status>,
    },
    Proposal {
        mutation: Traced<M::Mutation>,
        epoch: u64,
    },
}

// Only need Debug to make tokio::sync::mpsc::errors::SendError<_> implement Error.
impl<M: Machine> Debug for MachineServiceRequest<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MachineServiceRequest")
    }
}

#[derive(Clone)]
pub struct MachineServiceHandle<M: Machine> {
    journal_sender: ProfiledSender<JournalServiceRequest<M::Mutation>>,
    machine_sender: ProfiledSender<MachineServiceRequest<M>>,
    persisted_epoch: Arc<AtomicU64>,
}

impl<M: Machine> MachineServiceHandle<M> {
    pub fn new(
        journal_sender: ProfiledSender<JournalServiceRequest<M::Mutation>>,
        machine_sender: ProfiledSender<MachineServiceRequest<M>>,
        persisted_epoch: Arc<AtomicU64>,
    ) -> Self {
        Self {
            journal_sender,
            machine_sender,
            persisted_epoch,
        }
    }

    pub async fn apply_mutation(&mut self, mutation: Traced<M::Mutation>) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let request = JournalServiceRequest {
            mutation,
            notify: sender,
        };
        self.journal_sender
            .send(request)
            .await
            .chain_err(|| "journal_sender failed")?;
        receiver.await.chain_err(|| "sender dropped")
    }

    pub async fn query_state(&mut self, query: Traced<M::Query>) -> Result<M::Status> {
        let epoch = self.persisted_epoch.load(atomic::Ordering::Acquire);
        let (sender, receiver) = oneshot::channel();
        let request = MachineServiceRequest::Query {
            query,
            min_epoch: epoch,
            result: sender,
        };
        self.machine_sender
            .send(request)
            .await
            .chain_err(|| "machine_receiver dropped")?;
        receiver.await.chain_err(|| "sender dropped")
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
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        // NB: reverse is needed for min-heap
        Some(self.min_epoch.cmp(&other.min_epoch).reverse())
    }
}

impl<M: Machine> cmp::Ord for QueryPqItem<M> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct MachineService<M: Machine> {
    machine: M,
    request_receiver: ProfiledReceiver<MachineServiceRequest<M>>,
    epoch: u64,
    query_queue: BinaryHeap<QueryPqItem<M>>,
}

impl<M: Machine> MachineService<M> {
    pub fn new(
        machine: M,
        request_receiver: ProfiledReceiver<MachineServiceRequest<M>>,
        epoch: u64,
    ) -> Self {
        Self {
            machine,
            request_receiver,
            epoch,
            query_queue: BinaryHeap::new(),
        }
    }

    pub async fn serve(&mut self) -> Result<()> {
        loop {
            gauge!(
                "rayd.machine_service.queue_size",
                self.request_receiver.approx_len()
            );
            match self
                .request_receiver
                .recv()
                .await
                .chain_err(|| "request_receiver failed")?
            {
                MachineServiceRequest::Proposal { mutation, epoch } => {
                    debug!("Applying mutation (id: {}, new epoch: {})", mutation.id, epoch);
                    counter!("rayd.machine_service.proposal_count", 1);
                    self.handle_proposal(mutation.into_payload(), epoch).await;
                    gauge!("rayd.machine_service.epoch", self.epoch as i64);
                }
                MachineServiceRequest::Query {
                    query,
                    min_epoch,
                    result,
                } => {
                    debug!("Serving query (id: {}, epoch: {})", query.id, self.epoch);
                    counter!("rayd.machine_service.query_count", 1);
                    self.handle_query(query.into_payload(), min_epoch, result);
                }
            }
        }
    }

    async fn handle_proposal(&mut self, mutation: M::Mutation, epoch: u64) {
        assert_eq!(epoch, self.epoch + 1);
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

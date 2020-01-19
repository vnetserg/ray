use super::log_service::LogServiceRequest;

use prost::Message;

use futures::{
    channel::{mpsc, oneshot},
    select,
    sink::SinkExt,
    stream::StreamExt,
};

use std::{
    cmp::{self, Ordering},
    collections::BinaryHeap,
    fmt::Display,
};

pub trait Machine: Default + Send + 'static {
    type Mutation: Message + Default + Display;
    type Query: Send;
    type Status: Send;

    fn apply_mutation(&mut self, mutation: Self::Mutation);
    fn query_state(&self, query: Self::Query) -> Self::Status;
}

pub struct MachineServiceRequest<M: Machine> {
    query: M::Query,
    min_epoch: u64,
    result: oneshot::Sender<M::Status>,
}

impl<M: Machine> cmp::PartialEq for MachineServiceRequest<M> {
    fn eq(&self, other: &Self) -> bool {
        self.min_epoch == other.min_epoch
    }
}

impl<M: Machine> cmp::Eq for MachineServiceRequest<M> {}

impl<M: Machine> cmp::PartialOrd for MachineServiceRequest<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // NB: reverse is needed for min-heap
        Some(self.min_epoch.cmp(&other.min_epoch).reverse())
    }
}

impl<M: Machine> cmp::Ord for MachineServiceRequest<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Clone)]
pub struct MachineServiceHandle<M: Machine> {
    log_sender: mpsc::Sender<LogServiceRequest<M::Mutation>>,
    machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
}

impl<M: Machine> MachineServiceHandle<M> {
    pub fn new(
        log_sender: mpsc::Sender<LogServiceRequest<M::Mutation>>,
        machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
    ) -> Self {
        Self {
            log_sender,
            machine_sender,
        }
    }

    pub async fn apply_mutation(&self, mutation: M::Mutation) {
        let (sender, receiver) = oneshot::channel();
        let request = LogServiceRequest::PersistMutation {
            mutation,
            notify: sender,
        };
        self.log_sender
            .clone()
            .send(request)
            .await
            .expect("log_receiver dropped");
        receiver.await.expect("sender dropped");
    }

    pub async fn query_state(&self, query: M::Query) -> M::Status {
        let epoch = self.get_persisted_epoch().await;
        self.query_state_after(query, epoch).await
    }

    async fn get_persisted_epoch(&self) -> u64 {
        let (sender, receiver) = oneshot::channel();
        let request = LogServiceRequest::GetPersistedEpoch(sender);
        self.log_sender
            .clone()
            .send(request)
            .await
            .expect("log_receiver dropped");
        receiver.await.expect("sender dropped")
    }

    async fn query_state_after(&self, query: M::Query, epoch: u64) -> M::Status {
        let (sender, receiver) = oneshot::channel();
        let request = MachineServiceRequest::<M> {
            query,
            min_epoch: epoch,
            result: sender,
        };
        self.machine_sender
            .clone()
            .send(request)
            .await
            .expect("machine_receiver dropped");
        receiver.await.expect("sender dropped")
    }
}

pub struct MutationProposal<U> {
    mutation: U,
    epoch: u64,
}

impl<U> MutationProposal<U> {
    pub fn new(mutation: U, epoch: u64) -> Self {
        Self { mutation, epoch }
    }

    pub fn get_mutation(&self) -> &U {
        &self.mutation
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch
    }
}

pub struct MachineService<M: Machine> {
    proposal_receiver: mpsc::Receiver<MutationProposal<M::Mutation>>,
    request_receiver: mpsc::Receiver<MachineServiceRequest<M>>,
    machine: M,
    epoch: u64,
    request_queue: BinaryHeap<MachineServiceRequest<M>>,
}

impl<M: Machine> MachineService<M> {
    pub fn new(
        proposal_receiver: mpsc::Receiver<MutationProposal<M::Mutation>>,
        request_receiver: mpsc::Receiver<MachineServiceRequest<M>>,
    ) -> Self {
        Self {
            proposal_receiver,
            request_receiver,
            machine: M::default(),
            epoch: 0,
            request_queue: BinaryHeap::new(),
        }
    }

    pub async fn serve(&mut self) {
        loop {
            select! {
                proposal = self.proposal_receiver.next() => {
                    let proposal = proposal.expect("MachineService proposal_receiver terminated");
                    self.handle_mutation_proposal(proposal);
                },
                request = self.request_receiver.next() => {
                    let request = request.expect("MachineService request_receiver terminated");
                    self.handle_query(request);
                },
            }
        }
    }

    fn handle_mutation_proposal(&mut self, proposal: MutationProposal<M::Mutation>) {
        let MutationProposal { epoch, mutation } = proposal;
        if epoch <= self.epoch {
            debug!(
                "Rejected proposal: stale epoch (machine epoch: {}, proposal epoch: {}",
                self.epoch, proposal.epoch,
            );
            return;
        }

        assert_eq!(proposal.epoch, self.epoch + 1);
        debug!(
            "Applying mutation: {} (new epoch: {})",
            mutation,
            self.epoch + 1
        );
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

    fn handle_query(&mut self, request: MachineServiceRequest<M>) {
        if self.epoch >= request.min_epoch {
            let status = self.machine.query_state(request.query);
            request.result.send(status).ok();
        } else {
            self.request_queue.push(request);
        }
    }
}

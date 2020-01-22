use super::{
    machine_service::{Machine, MachineServiceRequest},
    snapshot_service::MutationProposal,
};

use prost::Message;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use futures::{
    channel::{mpsc, oneshot},
    sink::SinkExt,
    stream::StreamExt,
};

use std::io;

pub trait PersistentLog: Send + 'static {
    fn read_blob(&mut self) -> io::Result<Option<Vec<u8>>>;
    fn append_blob(&mut self, blob: &[u8]) -> io::Result<()>;
    fn persist(&mut self) -> io::Result<()>;
}

pub enum LogServiceRequest<U: Message> {
    PersistMutation {
        mutation: U,
        notify: oneshot::Sender<()>,
    },
    GetPersistedEpoch(oneshot::Sender<u64>),
}

pub struct LogService<L: PersistentLog, M: Machine> {
    log: L,
    machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
    snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
    request_receiver: mpsc::Receiver<LogServiceRequest<M::Mutation>>,
    batch_size: usize,
    persisted_epoch: u64,
}

impl<L: PersistentLog, M: Machine> LogService<L, M> {
    pub fn new(
        log: L,
        machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
        snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
        request_receiver: mpsc::Receiver<LogServiceRequest<M::Mutation>>,
        batch_size: usize,
    ) -> Self {
        Self {
            log,
            machine_sender,
            snapshot_sender,
            request_receiver,
            batch_size,
            persisted_epoch: 0,
        }
    }

    pub async fn recover(&mut self) {
        let mut mutation_count = 0;
        while let Some((mutation, epoch)) = self.read_mutation() {
            if self.persisted_epoch > 0 && epoch != self.persisted_epoch + 1 {
                panic!(
                    "Missing mutation(s): expected epoch {}, got {}",
                    self.persisted_epoch + 1,
                    epoch
                );
            }

            self.persisted_epoch = epoch;
            self.send_proposal(mutation, epoch).await;

            mutation_count += 1;
        }

        if self.persisted_epoch > 0 {
            info!(
                "Recovered {} mutations from log (persisted epoch: {})",
                mutation_count, self.persisted_epoch
            );
        }
    }

    fn read_mutation(&mut self) -> Option<(M::Mutation, u64)> {
        let blob = self.log.read_blob().unwrap_or_else(|err| {
            panic!("Failed to read persistent log: {}", err);
        })?;
        if blob.len() < 9 {
            panic!("Persistent log blob is too short: expected at least 9 bytes, got {}", blob.len());
        }

        let epoch = (&blob[..8]).read_u64::<LittleEndian>().unwrap();
        let mutation = M::Mutation::decode(&blob[8..])
            .unwrap_or_else(|err| panic!("Failed to decode mutation: {}", err));

        Some((mutation, epoch))
    }

    fn write_mutation(&mut self, mutation: &M::Mutation, epoch: u64) {
        let mut blob = vec![0u8; 8 + mutation.encoded_len()];
        (&mut blob[..8]).write_u64::<LittleEndian>(epoch).unwrap();
        mutation
            .encode(&mut &mut blob[8..])
            .unwrap_or_else(|err| panic!("Failed to encode mutation: {}", err));
        self.log
            .append_blob(&blob)
            .unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
    }

    pub async fn send_proposal(&mut self, mutation: M::Mutation, epoch: u64) {
        self.snapshot_sender
            .unbounded_send(MutationProposal {
                mutation: mutation.clone(),
                epoch,
            })
            .expect("LogService snapshot_sender failed");
        self.machine_sender
            .send(MachineServiceRequest::Proposal { mutation, epoch })
            .await
            .expect("LogService machine_sender failed");
    }

    pub async fn serve(&mut self) {
        loop {
            let mut proposals = vec![];
            let mut notifiers = vec![];
            let mut current_epoch = self.persisted_epoch;

            for i in 0..self.batch_size {
                let maybe_request = if i == 0 {
                    self.request_receiver.next().await
                } else {
                    match self.request_receiver.try_next() {
                        Ok(mb_req) => mb_req,
                        Err(_) => break,
                    }
                };
                let request = maybe_request.expect("PersistentLog request_receiver failed");
                match request {
                    LogServiceRequest::GetPersistedEpoch(response) => {
                        response.send(self.persisted_epoch).ok(); // Ignore error
                    }
                    LogServiceRequest::PersistMutation { mutation, notify } => {
                        current_epoch += 1;
                        proposals.push((mutation, current_epoch));
                        notifiers.push(notify);
                    }
                }
            }

            for &(ref mutation, epoch) in proposals.iter() {
                self.write_mutation(mutation, epoch);
            }

            if !proposals.is_empty() {
                self.log
                    .persist()
                    .unwrap_or_else(|err| panic!("Failed to persist log: {}", err));
                self.persisted_epoch += proposals.len() as u64;
                debug!(
                    "Wrote {} mutations to log (persisted epoch: {})",
                    proposals.len(),
                    self.persisted_epoch,
                );
            }

            for notify in notifiers.into_iter() {
                notify.send(()).ok(); // Ignore error
            }

            for (mutation, epoch) in proposals.into_iter() {
                self.send_proposal(mutation, epoch).await;
            }
        }
    }
}

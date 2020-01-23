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

pub enum ReadResult<R, W> {
    Blob(Vec<u8>, R),
    End(W),
}

pub trait PersistentLogReader: Sized + Send + 'static {
    type Writer: PersistentLogWriter;

    fn read_blob(self) -> io::Result<ReadResult<Self, Self::Writer>>;
}

pub trait PersistentLogWriter: Send + 'static {
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

pub struct LogService<L: PersistentLogReader, M: Machine> {
    writer: L::Writer,
    machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
    snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
    request_receiver: mpsc::Receiver<LogServiceRequest<M::Mutation>>,
    batch_size: usize,
    persisted_epoch: u64,
}

impl<L: PersistentLogReader, M: Machine> LogService<L, M> {
    pub async fn recover(
        reader: L,
        mut machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
        mut snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
        request_receiver: mpsc::Receiver<LogServiceRequest<M::Mutation>>,
        batch_size: usize,
        snapshot_epoch: u64,
    ) -> Self {
        let mut persisted_epoch = snapshot_epoch;
        let mut proposals = vec![];
        let mut mutation_count = 0usize;
        let mut last_epoch = None;

        let writer = Self::reader_into_writer(reader, |blob| {
            let (mutation, epoch) = Self::decode_blob(blob);

            if last_epoch
                .as_ref()
                .map(|last| last + 1 != epoch)
                .unwrap_or(false)
            {
                panic!(
                    "Missing mutation(s): expected epoch {}, got {}",
                    last_epoch.unwrap() + 1,
                    epoch
                );
            }

            if epoch > persisted_epoch + 1 {
                panic!(
                    "Missing mutation(s): persisted epoch {}, got epoch {}",
                    persisted_epoch, epoch
                );
            }

            last_epoch = Some(epoch);
            mutation_count += 1;
            persisted_epoch = epoch;
            proposals.push((mutation, epoch));
        });

        let last_epoch = last_epoch.unwrap_or(0);
        if last_epoch != persisted_epoch {
            panic!(
                "Missing mutation(s): snapshot epoch {}, got mutations only up to epoch {}",
                persisted_epoch, last_epoch
            );
        }

        for (mutation, epoch) in proposals.into_iter() {
            if epoch > snapshot_epoch {
                Self::send_proposal(mutation, epoch, &mut machine_sender, &mut snapshot_sender)
                    .await;
            }
        }

        if mutation_count > 0 {
            info!(
                "Recovered {} mutations from log (persisted epoch: {})",
                mutation_count, persisted_epoch
            );
        }

        Self {
            writer,
            machine_sender,
            snapshot_sender,
            request_receiver,
            batch_size,
            persisted_epoch,
        }
    }

    fn reader_into_writer<F>(mut reader: L, mut callback: F) -> L::Writer
    where
        F: FnMut(Vec<u8>),
    {
        loop {
            reader = match reader.read_blob() {
                Ok(ReadResult::Blob(data, reader)) => {
                    callback(data);
                    reader
                }
                Ok(ReadResult::End(writer)) => return writer,
                Err(err) => panic!("Failed to read blob: {}", err),
            }
        }
    }

    fn decode_blob(blob: Vec<u8>) -> (M::Mutation, u64) {
        if blob.len() < 9 {
            panic!(
                "Persistent log blob is too short: expected at least 9 bytes, got {}",
                blob.len()
            );
        }

        let epoch = (&blob[..8]).read_u64::<LittleEndian>().unwrap();
        let mutation = M::Mutation::decode(&blob[8..])
            .unwrap_or_else(|err| panic!("Failed to decode mutation: {}", err));

        (mutation, epoch)
    }

    async fn send_proposal(
        mutation: M::Mutation,
        epoch: u64,
        machine_sender: &mut mpsc::Sender<MachineServiceRequest<M>>,
        snapshot_sender: &mut mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
    ) {
        snapshot_sender
            .unbounded_send(MutationProposal {
                mutation: mutation.clone(),
                epoch,
            })
            .expect("LogService snapshot_sender failed");
        machine_sender
            .send(MachineServiceRequest::Proposal { mutation, epoch })
            .await
            .expect("LogService machine_sender failed");
    }

    fn write_mutation(&mut self, mutation: &M::Mutation, epoch: u64) {
        let mut blob = vec![0u8; 8 + mutation.encoded_len()];
        (&mut blob[..8]).write_u64::<LittleEndian>(epoch).unwrap();
        mutation
            .encode(&mut &mut blob[8..])
            .unwrap_or_else(|err| panic!("Failed to encode mutation: {}", err));
        self.writer
            .append_blob(&blob)
            .unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
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
                self.writer
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
                Self::send_proposal(
                    mutation,
                    epoch,
                    &mut self.machine_sender,
                    &mut self.snapshot_sender,
                )
                .await;
            }
        }
    }
}

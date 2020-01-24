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

struct LogServiceBase<M: Machine> {
    machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
    snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
    request_receiver: mpsc::Receiver<LogServiceRequest<M::Mutation>>,
    batch_size: usize,
}

impl<M: Machine> LogServiceBase<M> {
    async fn send_proposal(&mut self, mutation: M::Mutation, epoch: u64) {
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

    async fn serve_batch(
        &mut self,
        persisted_epoch: u64,
    ) -> (Vec<M::Mutation>, Vec<oneshot::Sender<()>>) {
        let mut mutations = vec![];
        let mut notifiers = vec![];

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
                    response.send(persisted_epoch).ok(); // Ignore error
                }
                LogServiceRequest::PersistMutation { mutation, notify } => {
                    mutations.push(mutation);
                    notifiers.push(notify);
                }
            }
        }

        (mutations, notifiers)
    }
}

pub struct LogServiceRestorer<R: PersistentLogReader, M: Machine> {
    reader: R,
    snapshot_epoch: u64,
    base: LogServiceBase<M>,
}

impl<R: PersistentLogReader, M: Machine> LogServiceRestorer<R, M> {
    pub fn new(
        reader: R,
        machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
        snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
        request_receiver: mpsc::Receiver<LogServiceRequest<M::Mutation>>,
        batch_size: usize,
        snapshot_epoch: u64,
    ) -> Self {
        let base = LogServiceBase {
            machine_sender,
            snapshot_sender,
            request_receiver,
            batch_size,
        };
        Self {
            reader,
            snapshot_epoch,
            base,
        }
    }

    pub async fn restore(mut self) -> LogService<R::Writer, M> {
        let mut mutation_count = 0usize;
        let mut last_epoch = None;

        let mut maybe_reader = Some(self.reader);
        let mut maybe_writer = None;

        while let Some(reader) = maybe_reader {
            maybe_reader = match reader.read_blob() {
                Ok(ReadResult::Blob(data, reader)) => {
                    let (mutation, epoch) = Self::decode_blob(data);
                    Self::validate_blob_epoch(epoch, self.snapshot_epoch, last_epoch);

                    if epoch > self.snapshot_epoch {
                        self.base.send_proposal(mutation, epoch).await;
                    }

                    last_epoch = Some(epoch);
                    mutation_count += 1;

                    Some(reader)
                }
                Ok(ReadResult::End(writer)) => {
                    maybe_writer = Some(writer);
                    None
                }
                Err(err) => panic!("Failed to read blob: {}", err),
            };
        }

        let last_epoch = last_epoch.unwrap_or(0);
        if last_epoch < self.snapshot_epoch {
            panic!(
                "Missing mutation(s): snapshot epoch {}, got mutations only up to epoch {}",
                self.snapshot_epoch, last_epoch
            );
        }

        if mutation_count > 0 {
            info!(
                "Recovered {} mutations from log (persisted epoch: {})",
                mutation_count, last_epoch
            );
        }

        LogService {
            writer: maybe_writer.unwrap(),
            persisted_epoch: last_epoch,
            base: self.base,
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

    fn validate_blob_epoch(epoch: u64, snapshot_epoch: u64, last_epoch: Option<u64>) {
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

        if last_epoch.is_none() && epoch > snapshot_epoch + 1 {
            panic!(
                "Missing mutation(s): expected epoch {}, got epoch {}",
                snapshot_epoch + 1,
                epoch
            );
        }
    }
}

pub struct LogService<W: PersistentLogWriter, M: Machine> {
    writer: W,
    persisted_epoch: u64,
    base: LogServiceBase<M>,
}

impl<W: PersistentLogWriter, M: Machine> LogService<W, M> {
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
            let (mutations, notifiers) = self.base.serve_batch(self.persisted_epoch).await;

            if mutations.is_empty() {
                continue;
            }

            let proposals: Vec<_> = mutations
                .into_iter()
                .enumerate()
                .map(|(index, mutation)| (mutation, self.persisted_epoch + 1 + index as u64))
                .collect();

            for (mutation, epoch) in proposals.iter() {
                self.write_mutation(mutation, *epoch);
            }

            self.writer
                .persist()
                .unwrap_or_else(|err| panic!("Failed to persist log: {}", err));
            self.persisted_epoch += proposals.len() as u64;

            debug!(
                "Wrote {} mutations to log (persisted epoch: {})",
                proposals.len(),
                self.persisted_epoch,
            );

            for notify in notifiers.into_iter() {
                notify.send(()).ok(); // Ignore error
            }

            for (mutation, epoch) in proposals.into_iter() {
                self.base.send_proposal(mutation, epoch).await;
            }
        }
    }
}

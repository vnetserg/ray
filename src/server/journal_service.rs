use super::{
    machine_service::{Machine, MachineServiceRequest},
    snapshot_service::MutationProposal,
};

use prost::Message;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use futures::{
    channel::{mpsc, oneshot},
    select,
    sink::SinkExt,
    stream::StreamExt,
};

use std::io;

pub enum ReadResult<R, W> {
    Blob(Vec<u8>, R),
    End(W),
}

pub trait JournalReader: Sized + Send + 'static {
    type Writer: JournalWriter;

    fn read_blob(self) -> io::Result<ReadResult<Self, Self::Writer>>;
}

pub trait JournalWriter: Send + 'static {
    fn append_blob(&mut self, blob: &[u8]) -> io::Result<()>;
    fn persist(&mut self) -> io::Result<()>;
    fn get_blob_count(&self) -> usize;
    fn dispose_oldest_blobs(&mut self, blob_count: usize) -> io::Result<()>;
}

pub enum JournalServiceRequest<U: Message> {
    PersistMutation {
        mutation: U,
        notify: oneshot::Sender<()>,
    },
    GetPersistedEpoch {
        epoch_sender: oneshot::Sender<u64>,
    },
}

struct BatchResult<U> {
    mutations: Vec<U>,
    notifiers: Vec<oneshot::Sender<()>>,
    min_epoch: Option<u64>,
}

struct JournalServiceBase<M: Machine> {
    machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
    snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
    request_receiver: mpsc::Receiver<JournalServiceRequest<M::Mutation>>,
    min_epoch_receiver: mpsc::UnboundedReceiver<u64>,
    batch_size: usize,
}

impl<M: Machine> JournalServiceBase<M> {
    async fn send_proposal(&mut self, mutation: M::Mutation, epoch: u64) {
        self.snapshot_sender
            .unbounded_send(MutationProposal {
                mutation: mutation.clone(),
                epoch,
            })
            .expect("JournalService snapshot_sender failed");
        self.machine_sender
            .send(MachineServiceRequest::Proposal { mutation, epoch })
            .await
            .expect("JournalService machine_sender failed");
    }

    async fn serve_batch(&mut self, persisted_epoch: u64) -> BatchResult<M::Mutation> {
        select! {
            maybe_min_epoch = self.min_epoch_receiver.next() => {
                let min_epoch = maybe_min_epoch.expect("JournalService min_epoch_receiver failed");
                return BatchResult {
                    mutations: vec![],
                    notifiers: vec![],
                    min_epoch: Some(min_epoch),
                }
            },
            maybe_request = self.request_receiver.next() => {
                let request = maybe_request.expect("JournalService request_receiver failed");
                let (mutations, notifiers) = self.process_request_batch(request, persisted_epoch);
                return BatchResult {
                    mutations,
                    notifiers,
                    min_epoch: None,
                }
            },
        }
    }

    fn process_request_batch(
        &mut self,
        first: JournalServiceRequest<M::Mutation>,
        persisted_epoch: u64,
    ) -> (Vec<M::Mutation>, Vec<oneshot::Sender<()>>) {
        let mut mutations = vec![];
        let mut notifiers = vec![];
        let mut request = first;
        let mut processed_requests = 0;

        loop {
            match request {
                JournalServiceRequest::GetPersistedEpoch { epoch_sender } => {
                    epoch_sender.send(persisted_epoch).ok(); // Ignore error
                }
                JournalServiceRequest::PersistMutation { mutation, notify } => {
                    mutations.push(mutation);
                    notifiers.push(notify);
                }
            }
            processed_requests += 1;

            if processed_requests < self.batch_size {
                match self.request_receiver.try_next() {
                    Ok(maybe_request) => {
                        request = maybe_request.expect("JournalService request_receiver failed");
                    }
                    Err(_) => break,
                }
            } else {
                break;
            }
        }

        (mutations, notifiers)
    }
}

pub struct JournalServiceRestorer<R: JournalReader, M: Machine> {
    reader: R,
    snapshot_epoch: u64,
    base: JournalServiceBase<M>,
}

impl<R: JournalReader, M: Machine> JournalServiceRestorer<R, M> {
    pub fn new(
        reader: R,
        machine_sender: mpsc::Sender<MachineServiceRequest<M>>,
        snapshot_sender: mpsc::UnboundedSender<MutationProposal<M::Mutation>>,
        request_receiver: mpsc::Receiver<JournalServiceRequest<M::Mutation>>,
        min_epoch_receiver: mpsc::UnboundedReceiver<u64>,
        batch_size: usize,
        snapshot_epoch: u64,
    ) -> Self {
        let base = JournalServiceBase {
            machine_sender,
            snapshot_sender,
            request_receiver,
            min_epoch_receiver,
            batch_size,
        };
        Self {
            reader,
            snapshot_epoch,
            base,
        }
    }

    pub async fn restore(mut self) -> JournalService<R::Writer, M> {
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
                "Recovered {} mutations from journal (persisted epoch: {})",
                mutation_count, last_epoch
            );
        }

        JournalService {
            writer: maybe_writer.unwrap(),
            persisted_epoch: last_epoch,
            base: self.base,
        }
    }

    fn decode_blob(blob: Vec<u8>) -> (M::Mutation, u64) {
        if blob.len() < 9 {
            panic!(
                "Journal blob is too short: expected at least 9 bytes, got {}",
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

pub struct JournalService<W: JournalWriter, M: Machine> {
    writer: W,
    persisted_epoch: u64,
    base: JournalServiceBase<M>,
}

impl<W: JournalWriter, M: Machine> JournalService<W, M> {
    fn write_mutation(&mut self, mutation: &M::Mutation, epoch: u64) {
        let mut blob = vec![0u8; 8 + mutation.encoded_len()];
        (&mut blob[..8]).write_u64::<LittleEndian>(epoch).unwrap();
        mutation
            .encode(&mut &mut blob[8..])
            .unwrap_or_else(|err| panic!("Failed to encode mutation: {}", err));
        self.writer
            .append_blob(&blob)
            .unwrap_or_else(|err| panic!("Failed to write to journal: {}", err));
    }

    pub async fn serve(&mut self) {
        loop {
            let BatchResult {
                mutations,
                notifiers,
                min_epoch,
            } = self.base.serve_batch(self.persisted_epoch).await;

            if let Some(min_epoch) = min_epoch {
                self.handle_new_min_epoch(min_epoch);
            }

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
                .unwrap_or_else(|err| panic!("Failed to persist journal: {}", err));
            self.persisted_epoch += proposals.len() as u64;

            debug!(
                "Wrote {} mutations to journal (persisted epoch: {})",
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

    fn handle_new_min_epoch(&mut self, min_epoch: u64) {
        assert!(min_epoch <= self.persisted_epoch + 1);

        let desired_len = (self.persisted_epoch + 1 - min_epoch) as usize;
        let actual_len = self.writer.get_blob_count();

        if actual_len > desired_len {
            debug!("Disposing log entries with epoch < {}", min_epoch);

            self.writer
                .dispose_oldest_blobs(actual_len - desired_len)
                .unwrap_or_else(|err| panic!("Failed to dispose blobs: {}", err));
        }
    }
}

use super::{
    logging_service::FastlogMessage,
    machine_service::{Machine, MachineServiceRequest},
    snapshot_service::MutationProposal,
};

use crate::{
    errors::*,
    fastlog,
    util::{
        ProfiledReceiver, ProfiledSender, ProfiledUnboundedReceiver, ProfiledUnboundedSender,
        Traced,
    },
};

use prost::Message;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use tokio::sync::oneshot;

use futures::{select, FutureExt};

use metrics::{gauge, timing, value};

use std::{
    fmt::{self, Debug},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

pub enum ReadResult<R, W> {
    Blob(Vec<u8>, R),
    End(W),
}

pub trait JournalReader: Sized + Send + 'static {
    type Writer: JournalWriter;

    fn read_blob(self) -> Result<ReadResult<Self, Self::Writer>>;
}

pub trait JournalWriter: Send + 'static {
    fn append_blob(&mut self, blob: &[u8]) -> Result<()>;
    fn persist(&mut self) -> Result<()>;
    fn get_blob_count(&self) -> usize;
    fn dispose_oldest_blobs(&mut self, blob_count: usize) -> Result<()>;
}

pub struct JournalServiceRequest<U: Message> {
    pub mutation: Traced<U>,
    pub notify: oneshot::Sender<()>,
}

// Only need Debug to make tokio::sync::mpsc::errors::SendError<_> implement Error.
impl<U: Message> Debug for JournalServiceRequest<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JournalServiceRequest")
    }
}

struct BatchResult<U> {
    mutations: Vec<Traced<U>>,
    notifiers: Vec<oneshot::Sender<()>>,
    min_epoch: Option<u64>,
}

struct JournalServiceBase<M: Machine> {
    machine_sender: ProfiledSender<MachineServiceRequest<M>>,
    snapshot_sender: ProfiledUnboundedSender<MutationProposal<M::Mutation>>,
    request_receiver: ProfiledReceiver<JournalServiceRequest<M::Mutation>>,
    min_epoch_receiver: ProfiledUnboundedReceiver<u64>,
    batch_size: usize,
    external_epoch: Arc<AtomicU64>,
}

impl<M: Machine> JournalServiceBase<M> {
    async fn send_proposal(&mut self, mutation: Traced<M::Mutation>, epoch: u64) -> Result<()> {
        self.snapshot_sender
            .send(MutationProposal {
                mutation: mutation.clone(),
                epoch,
            })
            .chain_err(|| "snapshot_sender failed")?;
        self.machine_sender
            .send(MachineServiceRequest::Proposal { mutation, epoch })
            .await
            .chain_err(|| "machine_sender failed")
    }

    async fn serve_batch(&mut self) -> Result<BatchResult<M::Mutation>> {
        gauge!(
            "rayd.journal_service.queue_size",
            self.request_receiver.approx_len(),
            "queue" => "request"
        );
        gauge!(
            "rayd.journal_service.queue_size",
            self.min_epoch_receiver.approx_len(),
            "queue" => "min_epoch"
        );

        select! {
            maybe_min_epoch = self.min_epoch_receiver.recv().fuse() => {
                let min_epoch = maybe_min_epoch.chain_err(|| "min_epoch_receiver failed")?;
                return Ok(BatchResult {
                    mutations: vec![],
                    notifiers: vec![],
                    min_epoch: Some(min_epoch),
                })
            },
            maybe_request = self.request_receiver.recv().fuse() => {
                let request = maybe_request.chain_err(|| "request_receiver failed")?;
                self.process_request_batch(request)
            },
        }
    }

    fn process_request_batch(
        &mut self,
        first: JournalServiceRequest<M::Mutation>,
    ) -> Result<BatchResult<M::Mutation>> {
        let mut mutations = vec![];
        let mut notifiers = vec![];
        let mut request = first;
        let mut processed_requests = 0;

        loop {
            mutations.push(request.mutation);
            notifiers.push(request.notify);
            processed_requests += 1;

            if processed_requests < self.batch_size {
                request = match self.request_receiver.try_recv() {
                    Ok(req) => req,
                    Err(_) => break,
                };
            } else {
                break;
            }
        }

        Ok(BatchResult {
            mutations,
            notifiers,
            min_epoch: None,
        })
    }

    fn update_persisted_epoch(&self, persisted_epoch: u64) {
        self.external_epoch
            .store(persisted_epoch, Ordering::Release);
    }
}

pub struct JournalServiceRestorer<R: JournalReader, M: Machine> {
    reader: R,
    snapshot_epoch: u64,
    base: JournalServiceBase<M>,
}

impl<R: JournalReader, M: Machine> JournalServiceRestorer<R, M> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reader: R,
        machine_sender: ProfiledSender<MachineServiceRequest<M>>,
        snapshot_sender: ProfiledUnboundedSender<MutationProposal<M::Mutation>>,
        request_receiver: ProfiledReceiver<JournalServiceRequest<M::Mutation>>,
        min_epoch_receiver: ProfiledUnboundedReceiver<u64>,
        batch_size: usize,
        snapshot_epoch: u64,
        external_epoch: Arc<AtomicU64>,
    ) -> Self {
        let base = JournalServiceBase {
            machine_sender,
            snapshot_sender,
            request_receiver,
            min_epoch_receiver,
            batch_size,
            external_epoch,
        };
        Self {
            reader,
            snapshot_epoch,
            base,
        }
    }

    pub async fn restore(mut self) -> Result<JournalService<R::Writer, M>> {
        info!("Starting journal recovery");

        let mut mutation_count = 0usize;
        let mut last_epoch = None;

        let mut maybe_reader = Some(self.reader);
        let mut maybe_writer = None;

        while let Some(reader) = maybe_reader {
            maybe_reader = match reader.read_blob().chain_err(|| "failed to read blob")? {
                ReadResult::Blob(data, reader) => {
                    let (mutation, epoch) = Self::decode_blob(data)?;
                    Self::validate_blob_epoch(epoch, self.snapshot_epoch, last_epoch)?;

                    if epoch > self.snapshot_epoch {
                        let traced = Traced::new(mutation);
                        fastlog!(FastlogMessage::RecoveredMutation {
                            id: traced.id,
                            epoch: epoch,
                        });
                        self.base.send_proposal(traced, epoch).await?;
                    }

                    last_epoch = Some(epoch);
                    mutation_count += 1;

                    Some(reader)
                }
                ReadResult::End(writer) => {
                    maybe_writer = Some(writer);
                    None
                }
            };
        }

        let last_epoch = last_epoch.unwrap_or(0);
        if last_epoch > 0 && last_epoch < self.snapshot_epoch {
            bail!(
                "Missing mutation(s): snapshot epoch {}, got mutations only up to epoch {}",
                self.snapshot_epoch,
                last_epoch
            );
        }

        if mutation_count > 0 {
            let first_epoch = last_epoch + 1 - mutation_count as u64;
            info!(
                "Recovered {} mutations from journal (epoch range: [{}, {}])",
                mutation_count, first_epoch, last_epoch
            );
        } else {
            info!("No mutations recovered from journal");
        }

        // Notice: before this point, the value of the external_epoch atomic was zero.
        // It is crucially important that no requests are served based on it's value before
        // the atomic is properly initialized. Otherwise expect stale reads.
        self.base.update_persisted_epoch(last_epoch);

        Ok(JournalService {
            writer: maybe_writer.unwrap(),
            persisted_epoch: last_epoch,
            base: self.base,
        })
    }

    fn decode_blob(blob: Vec<u8>) -> Result<(M::Mutation, u64)> {
        if blob.len() < 9 {
            bail!(
                "Journal blob is too short: expected at least 9 bytes, got {}",
                blob.len()
            );
        }

        let epoch = (&blob[..8]).read_u64::<LittleEndian>().unwrap();
        let mutation = M::Mutation::decode(&blob[8..]).chain_err(|| "failed to decode mutation")?;

        Ok((mutation, epoch))
    }

    fn validate_blob_epoch(epoch: u64, snapshot_epoch: u64, last_epoch: Option<u64>) -> Result<()> {
        if last_epoch
            .as_ref()
            .map(|last| last + 1 != epoch)
            .unwrap_or(false)
        {
            bail!(
                "Missing mutation(s): expected epoch {}, got {}",
                last_epoch.unwrap() + 1,
                epoch
            );
        }

        if last_epoch.is_none() && epoch > snapshot_epoch + 1 {
            bail!(
                "Missing mutation(s): expected epoch {}, got epoch {}",
                snapshot_epoch + 1,
                epoch
            );
        }

        Ok(())
    }
}

pub struct JournalService<W: JournalWriter, M: Machine> {
    writer: W,
    persisted_epoch: u64,
    base: JournalServiceBase<M>,
}

impl<W: JournalWriter, M: Machine> JournalService<W, M> {
    fn write_mutation(&mut self, mutation: &M::Mutation, epoch: u64) -> Result<()> {
        let mut blob = vec![0u8; 8 + mutation.encoded_len()];
        (&mut blob[..8]).write_u64::<LittleEndian>(epoch).unwrap();
        mutation
            .encode(&mut &mut blob[8..])
            .chain_err(|| "failed to encode mutation")?;
        self.writer
            .append_blob(&blob)
            .chain_err(|| "journal write failed")?;
        Ok(())
    }

    pub async fn serve(&mut self) -> Result<()> {
        loop {
            let BatchResult {
                mutations,
                notifiers,
                min_epoch,
            } = self.base.serve_batch().await?;

            if let Some(min_epoch) = min_epoch {
                self.handle_new_min_epoch(min_epoch)?;
            }

            if mutations.is_empty() {
                continue;
            }

            let proposals: Vec<_> = mutations
                .into_iter()
                .enumerate()
                .map(|(index, mutation)| (mutation, self.persisted_epoch + 1 + index as u64))
                .collect();

            value!("rayd.journal_service.batch_size", proposals.len() as u64);

            for (mutation, epoch) in proposals.iter() {
                self.write_mutation(&mutation.payload, *epoch)?;
            }

            let start = Instant::now();
            self.writer
                .persist()
                .chain_err(|| "failed to persist journal")?;
            timing!(
                "rayd.journal_service.persist_duration",
                start,
                Instant::now()
            );

            self.persisted_epoch += proposals.len() as u64;
            self.base.update_persisted_epoch(self.persisted_epoch);
            gauge!(
                "rayd.journal_service.persisted_epoch",
                self.persisted_epoch as i64
            );

            let now = chrono::Utc::now();
            for (mutation, epoch) in proposals.iter() {
                fastlog!(
                    now: now,
                    FastlogMessage::PersistedMutation {
                        epoch: *epoch,
                        id: mutation.id,
                    }
                );
            }

            for notify in notifiers.into_iter() {
                notify.send(()).ok(); // Ignore error
            }

            for (mutation, epoch) in proposals.into_iter() {
                self.base.send_proposal(mutation, epoch).await?;
            }
        }
    }

    fn handle_new_min_epoch(&mut self, min_epoch: u64) -> Result<()> {
        assert!(min_epoch <= self.persisted_epoch + 1);

        let desired_len = (self.persisted_epoch + 1 - min_epoch) as usize;
        let actual_len = self.writer.get_blob_count();

        if actual_len > desired_len {
            debug!("Disposing log entries with epoch < {}", min_epoch);

            if let Err(err) = self.writer.dispose_oldest_blobs(actual_len - desired_len) {
                warn!(
                    "Failed to dispose unneeded blobs (error chain below)\n{}",
                    err.display_fancy_chain()
                );
            }
        }

        Ok(())
    }
}

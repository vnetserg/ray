use super::machine_service::Machine;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use futures::{channel::mpsc, StreamExt};

use std::io::{self, Read, Write};

pub trait PersistentWrite: Write {
    fn persist(&mut self) -> io::Result<()>;
}

pub trait SnapshotStorage: Send + 'static {
    type Writer: PersistentWrite;
    type Reader: Read;

    fn create_snapshot(&mut self, name: &str) -> io::Result<Self::Writer>;
    fn open_last_snapshot(&self) -> io::Result<Option<Self::Reader>>;
}

pub struct MutationProposal<U> {
    pub mutation: U,
    pub epoch: u64,
}

pub fn read_snapshot<R: Read, M: Machine>(reader: &mut R) -> io::Result<(M, u64)> {
    let epoch = reader.read_u64::<LittleEndian>()?;
    M::from_snapshot(reader).map(|machine| (machine, epoch))
}

fn write_snapshot<W: Write, M: Machine>(writer: &mut W, machine: &M, epoch: u64) -> io::Result<()> {
    writer.write_u64::<LittleEndian>(epoch)?;
    machine.write_snapshot(writer)
}

pub struct SnapshotService<S: SnapshotStorage, M: Machine> {
    storage: S,
    machine: M,
    proposal_receiver: mpsc::UnboundedReceiver<MutationProposal<M::Mutation>>,
    epoch: u64,
    snapshot_interval: u64,
    batch_size: usize,
    last_snapshot_epoch: u64,
}

impl<S: SnapshotStorage, M: Machine> SnapshotService<S, M> {
    pub fn new(
        storage: S,
        machine: M,
        proposal_receiver: mpsc::UnboundedReceiver<MutationProposal<M::Mutation>>,
        epoch: u64,
        snapshot_interval: u64,
        batch_size: usize,
    ) -> Self {
        Self {
            storage,
            machine,
            proposal_receiver,
            epoch,
            snapshot_interval,
            batch_size,
            last_snapshot_epoch: epoch,
        }
    }

    pub async fn serve(&mut self) {
        loop {
            self.apply_mutation_batch().await;

            if self.epoch - self.last_snapshot_epoch >= self.snapshot_interval {
                self.make_snapshot();
            }
        }
    }

    pub async fn apply_mutation_batch(&mut self) {
        for i in 0..self.batch_size {
            let maybe_proposal = if i == 0 {
                self.proposal_receiver.next().await
            } else {
                match self.proposal_receiver.try_next() {
                    Ok(maybe_proposal) => maybe_proposal,
                    Err(_) => break,
                }
            };

            let MutationProposal { mutation, epoch } =
                maybe_proposal.expect("SnapshotService proposal_receiver failed");

            if epoch <= self.epoch {
                debug!(
                    "Rejected proposal: stale epoch (machine epoch: {}, proposal epoch: {})",
                    self.epoch, epoch,
                );
                continue;
            }

            assert_eq!(epoch, self.epoch + 1);
            self.machine.apply_mutation(mutation);
            self.epoch += 1;
        }
    }

    pub fn make_snapshot(&mut self) {
        info!("Snapshot initiated (epoch: {})", self.epoch);

        let mut writer = self
            .storage
            .create_snapshot(&self.epoch.to_string())
            .unwrap_or_else(|err| {
                panic!("Failed to create snapshot '{}': {}", self.epoch, err);
            });

        write_snapshot(&mut writer, &self.machine, self.epoch)
            .and_then(|_| writer.persist())
            .unwrap_or_else(|err| {
                panic!("Failed to write snapshot: {}", err);
            });

        self.last_snapshot_epoch = self.epoch;
        info!("Snapshot finished (epoch: {})", self.epoch);
    }
}

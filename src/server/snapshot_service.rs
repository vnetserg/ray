use super::machine_service::Machine;

use crate::errors::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use futures::{channel::mpsc, StreamExt};

use std::io::{Read, Write};

pub trait PersistentWrite: Write {
    fn persist(&mut self) -> Result<()>;
}

pub trait SnapshotStorage: Send + 'static {
    type Writer: PersistentWrite;
    type Reader: Read;

    fn create_snapshot(&mut self, name: &str) -> Result<Self::Writer>;
    fn open_last_snapshot(&self) -> Result<Option<Self::Reader>>;
}

pub struct MutationProposal<U> {
    pub mutation: U,
    pub epoch: u64,
}

pub fn read_snapshot<R: Read, M: Machine>(reader: &mut R) -> Result<(M, u64)> {
    let epoch = reader.read_u64::<LittleEndian>()?;
    let machine = M::from_snapshot(reader)?;
    Ok((machine, epoch))
}

fn write_snapshot<W: Write, M: Machine>(writer: &mut W, machine: &M, epoch: u64) -> Result<()> {
    writer.write_u64::<LittleEndian>(epoch)?;
    machine.write_snapshot(writer)
}

pub struct SnapshotService<S: SnapshotStorage, M: Machine> {
    storage: S,
    machine: M,
    proposal_receiver: mpsc::UnboundedReceiver<MutationProposal<M::Mutation>>,
    min_epoch_sender: mpsc::UnboundedSender<u64>,
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
        min_epoch_sender: mpsc::UnboundedSender<u64>,
        epoch: u64,
        snapshot_interval: u64,
        batch_size: usize,
    ) -> Self {
        Self {
            storage,
            machine,
            proposal_receiver,
            min_epoch_sender,
            epoch,
            snapshot_interval,
            batch_size,
            last_snapshot_epoch: epoch,
        }
    }

    pub async fn serve(&mut self) -> Result<()> {
        loop {
            self.apply_mutation_batch()
                .await
                .chain_err(|| "failed to apply mutation batch")?;

            if self.epoch - self.last_snapshot_epoch >= self.snapshot_interval {
                self.make_snapshot()
                    .chain_err(|| format!("failed to make snapshot for epoch {}", self.epoch))?;
            }
        }
    }

    pub async fn apply_mutation_batch(&mut self) -> Result<()> {
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
                maybe_proposal.chain_err(|| "proposal_receiver failed")?;

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
        Ok(())
    }

    pub fn make_snapshot(&mut self) -> Result<()> {
        info!("Snapshot initiated (epoch: {})", self.epoch);

        let mut writer = self
            .storage
            .create_snapshot(&self.epoch.to_string())
            .chain_err(|| "failed to create snapshot writer")?;

        write_snapshot(&mut writer, &self.machine, self.epoch)
            .and_then(|_| writer.persist())
            .chain_err(|| "snapshot write failed")?;

        self.min_epoch_sender
            .unbounded_send(self.epoch + 1)
            .chain_err(|| "min_epoch_sender failed")?;
        self.last_snapshot_epoch = self.epoch;

        info!("Snapshot finished (epoch: {})", self.epoch);

        Ok(())
    }
}

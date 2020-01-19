use super::machine_service::MutationProposal;

use prost::Message;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use futures::{
    channel::{mpsc, oneshot},
    sink::SinkExt,
    stream::StreamExt,
};

use std::io::{self, Read, Write};

pub trait PersistentLog: Read + Write + Send + 'static {
    fn persist(&mut self) -> io::Result<()>;
}

pub enum LogServiceRequest<U: Message> {
    PersistMutation {
        mutation: U,
        notify: oneshot::Sender<()>,
    },
    GetPersistedEpoch(oneshot::Sender<u64>),
}

pub struct LogService<L: PersistentLog, U: Message + Default> {
    log: L,
    proposal_sender: mpsc::Sender<MutationProposal<U>>,
    request_receiver: mpsc::Receiver<LogServiceRequest<U>>,
    batch_size: usize,
    persisted_epoch: u64,
}

impl<L: PersistentLog, U: Message + Default> LogService<L, U> {
    pub fn new(
        log: L,
        proposal_sender: mpsc::Sender<MutationProposal<U>>,
        request_receiver: mpsc::Receiver<LogServiceRequest<U>>,
        batch_size: usize,
    ) -> Self {
        Self {
            log,
            proposal_sender,
            request_receiver,
            batch_size,
            persisted_epoch: 0,
        }
    }

    pub async fn recover(&mut self) {
        let mut mutation_count = 0;
        while let Some((epoch, len)) = self.read_header() {
            if self.persisted_epoch > 0 && epoch != self.persisted_epoch + 1 {
                panic!(
                    "Missing mutation(s): expected epoch {}, got {}",
                    self.persisted_epoch + 1,
                    epoch
                );
            }
            self.persisted_epoch = epoch;

            let mutation = self.read_mutation(len as usize);
            let proposal = MutationProposal::new(mutation, epoch);

            self.proposal_sender
                .send(proposal)
                .await
                .expect("proposal receiver dropped");

            mutation_count += 1;
        }

        if self.persisted_epoch > 0 {
            info!(
                "Recovered {} mutations from log (persisted epoch: {})",
                mutation_count, self.persisted_epoch
            );
        }
    }

    fn read_header(&mut self) -> Option<(u64, usize)> {
        let mut buffer = [0u8; 12];
        if let Err(err) = self.log.read_exact(&mut buffer[..1]) {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            } else {
                panic!("Failed to read PersistentLog: {}", err);
            }
        }
        if let Err(err) = self.log.read_exact(&mut buffer[1..]) {
            panic!("Failed to read PersistentLog: {}", err);
        }

        let mut reader = &buffer[..];
        let epoch = reader.read_u64::<LittleEndian>().unwrap();
        let len = reader.read_u32::<LittleEndian>().unwrap();

        Some((epoch, len as usize))
    }

    fn read_mutation(&mut self, len: usize) -> U {
        let mut buf = vec![0; len];
        self.log
            .read_exact(&mut buf)
            .unwrap_or_else(|err| panic!("Failed to read PersistentLog: {}", err));
        U::decode(buf).unwrap_or_else(|err| panic!("Failed to parse mutation: {}", err))
    }

    pub async fn serve(&mut self) {
        loop {
            let mut proposals = vec![];
            let mut notifiers = vec![];
            let mut epoch = self.persisted_epoch;

            for i in 0..self.batch_size {
                let request = if i == 0 {
                    self.request_receiver
                        .next()
                        .await
                        .expect("all request senders dropped")
                } else {
                    match self.request_receiver.try_next() {
                        Ok(Some(req)) => req,
                        Ok(None) => panic!("PersistentLog request_receiver stream terminated"),
                        Err(_) => break,
                    }
                };
                match request {
                    LogServiceRequest::GetPersistedEpoch(response) => {
                        response.send(self.persisted_epoch).ok(); // Ignore error
                    }
                    LogServiceRequest::PersistMutation { mutation, notify } => {
                        epoch += 1;
                        proposals.push(MutationProposal::new(mutation, epoch));
                        notifiers.push(notify);
                    }
                }
            }

            for proposal in proposals.iter() {
                let mutation = proposal.get_mutation();
                let len = mutation.encoded_len();
                self.log
                    .write_u64::<LittleEndian>(proposal.get_epoch())
                    .and_then(|_| Ok(self.log.write_u32::<LittleEndian>(len as u32)?))
                    .unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
                self.write_mutation(mutation, len);
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

            for proposal in proposals.into_iter() {
                self.proposal_sender
                    .send(proposal)
                    .await
                    .expect("PersistentLog proposal_sender failed");
            }
        }
    }

    fn write_mutation(&mut self, mutation: &U, len: usize) {
        let mut buf = Vec::with_capacity(len);
        mutation
            .encode(&mut buf)
            .unwrap_or_else(|err| panic!("Failed to encode mutation: {}", err));
        if buf.len() != len {
            panic!(
                "write_mutation len mismatch: expected {}, actual {}",
                len,
                buf.len()
            );
        }
        self.log
            .write_all(&buf)
            .unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
    }
}

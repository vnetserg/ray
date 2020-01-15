use prost::Message;

use byteorder::{
    LittleEndian,
    ReadBytesExt,
    WriteBytesExt,
};

use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    sink::SinkExt,
    stream::StreamExt,
};

use std::{
    io::{
        self,
        Write,
        Read,
    },
};


pub trait PersistentLog : Read + Write + Send + 'static {
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
    mutation_sender: mpsc::Sender<U>,
    request_receiver: mpsc::Receiver<LogServiceRequest<U>>,
    persisted_epoch: u64,
}

impl<L: PersistentLog, U: Message + Default> LogService<L, U> {
    pub fn new(
        log: L,
        mutation_sender: mpsc::Sender<U>,
        request_receiver: mpsc::Receiver<LogServiceRequest<U>>
    ) -> Self {
        Self {
            log,
            mutation_sender,
            request_receiver,
            persisted_epoch: 0,
        }
    }

    pub async fn recover(&mut self) {
        loop {
            let len = match self.log.read_u32::<LittleEndian>() {
                Ok(n) => n,
                Err(err) => {
                    if err.kind() == io::ErrorKind::UnexpectedEof {
                        return;
                    } else {
                        panic!("Failed to read PersistentLog: {}", err);
                    }
                },
            };
            let mutation = self.read_mutation(len as usize);
            self.persisted_epoch += 1;
            self.mutation_sender.send(mutation).await.expect("mutation receiver dropped");
        }
    }

    fn read_mutation(&mut self, len: usize) -> U {
        let mut buf = vec![0; len];
        self.log.read_exact(&mut buf)
            .unwrap_or_else(|err| panic!("Failed to read PersistentLog: {}", err));
        U::decode(buf).unwrap_or_else(|err| panic!("Failed to parse mutation: {}", err))
    }

    pub async fn serve(&mut self) {
        loop {
            let mut mutations = vec![];
            let mut notifiers = vec![];

            for i in 0..100 {
                let request = if i == 0i32 {
                    self.request_receiver.next().await.expect("all request senders dropped")
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
                    },
                    LogServiceRequest::PersistMutation{ mutation, notify } => {
                        mutations.push(mutation);
                        notifiers.push(notify);
                    },
                }
            }

            for mutation in mutations.iter() {
                let len = mutation.encoded_len();
                self.log.write_u32::<LittleEndian>(len as u32)
                    .unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
                self.write_mutation(mutation, len);
            }

            if !mutations.is_empty() {
                self.log.persist().unwrap_or_else(|err| panic!("Failed to persist log: {}", err));
            }

            for notify in notifiers.into_iter() {
                notify.send(()).ok(); // Ignore error
            }

            for mutation in mutations.into_iter() {
                self.mutation_sender.send(mutation).await
                    .expect("PersistentLog mutation_sender failed");
            }
        }
    }

    fn write_mutation(&mut self, mutation: &U, len: usize) {
        let mut buf = Vec::with_capacity(len);
        mutation.encode(&mut buf).unwrap_or_else(|err| panic!("Failed to encode mutation: {}", err));
        if buf.len() != len {
            panic!("write_mutation len mismatch: expected {}, actual {}", len, buf.len());
        }
        self.log.write_all(&buf).unwrap_or_else(|err| panic!("Failed to write to log: {}", err));
    }
}

use byteorder::{LittleEndian, ReadBytesExt};

use metrics::{try_recorder, Key, Label};

use tokio::sync::mpsc::{Sender, Receiver, channel, error::{SendError, TrySendError, TryRecvError}};

use std::{sync::Arc, io::{self, Read}};

pub fn try_read_u32<T: Read>(reader: &mut T) -> io::Result<Option<usize>> {
    let mut buffer = [0u8; 4];
    if let Err(err) = reader.read_exact(&mut buffer[..1]) {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        } else {
            return Err(err);
        }
    }
    reader.read_exact(&mut buffer[1..])?;
    let len = (&buffer[..]).read_u32::<LittleEndian>().unwrap();
    Ok(Some(len as usize))
}

pub fn profiled_channel<T>(max_size: usize, name: &'static str, labels: Vec<Label>)
-> (ProfiledSender<T>, ProfiledReceiver<T>) {
    let (sender, receiver) = channel(max_size);
    (ProfiledSender::new(name, labels.clone(), sender), ProfiledReceiver::new(name, labels, receiver))
}

fn register_event(key: Key) {
    if let Some(recorder) = try_recorder() {
        recorder.increment_counter(key, 1);
    }
}

pub struct ProfiledSender<T> {
    key: Arc<Key>,
    inner: Sender<T>,
}

// Can't derive Clone since it puts Clone trait bound on T.
impl<T> Clone for ProfiledSender<T> {
    fn clone(&self) -> Self {
        Self { key: self.key.clone(), inner: self.inner.clone() }
    }
}

#[allow(dead_code)]
impl<T> ProfiledSender<T> {
    fn new(name: &'static str, mut labels: Vec<Label>, inner: Sender<T>) -> Self {
        labels.push(Label::new("event", "send"));
        let key = Arc::new(Key::from_name_and_labels(name, labels));
        Self { key, inner }
    }

    pub fn try_send(&mut self, message: T) -> Result<(), TrySendError<T>> {
        let result = self.inner.try_send(message);
        if result.is_ok() {
            register_event((*self.key).clone());
        }
        result
    }

    pub async fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        let result = self.inner.send(value).await;
        if result.is_ok() {
            register_event((*self.key).clone());
        }
        result
    }
}

pub struct ProfiledReceiver<T> {
    key: Key,
    inner: Receiver<T>,
}

#[allow(dead_code)]
impl<T> ProfiledReceiver<T> {
    fn new(name: &'static str, mut labels: Vec<Label>, inner: Receiver<T>) -> Self {
        labels.push(Label::new("event", "recv"));
        let key = Key::from_name_and_labels(name, labels);
        Self { key, inner }
    }

    pub fn close(&mut self) {
        self.inner.close()
    }

    pub async fn recv(&mut self) -> Option<T> {
        let result = self.inner.recv().await;
        if result.is_some() {
            register_event(self.key.clone());
        }
        result
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            register_event(self.key.clone());
        }
        result
    }
}

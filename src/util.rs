use byteorder::{LittleEndian, ReadBytesExt};

use tokio::sync::mpsc::{
    channel,
    error::{SendError, TryRecvError, TrySendError},
    unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};

use std::{
    io::{self, Read},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

pub fn try_read_u32<T: Read>(reader: &mut T) -> io::Result<Option<u32>> {
    let mut buffer = [0u8; 4];
    if let Err(err) = reader.read_exact(&mut buffer[..1]) {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        } else {
            return Err(err);
        }
    }
    reader.read_exact(&mut buffer[1..])?;
    let value = (&buffer[..]).read_u32::<LittleEndian>().unwrap();
    Ok(Some(value))
}

pub fn profiled_channel<T>(max_size: usize) -> (ProfiledSender<T>, ProfiledReceiver<T>) {
    let (sender, receiver) = channel(max_size);
    let size = Arc::new(AtomicI64::new(0));
    (
        ProfiledSender {
            size: size.clone(),
            inner: sender,
        },
        ProfiledReceiver {
            size,
            inner: receiver,
        },
    )
}

pub fn profiled_unbounded_channel<T>() -> (ProfiledUnboundedSender<T>, ProfiledUnboundedReceiver<T>)
{
    let (sender, receiver) = unbounded_channel();
    let size = Arc::new(AtomicI64::new(0));
    (
        ProfiledUnboundedSender {
            size: size.clone(),
            inner: sender,
        },
        ProfiledUnboundedReceiver {
            size,
            inner: receiver,
        },
    )
}

pub struct ProfiledSender<T> {
    // Use i64, not u64, as size counter because it's updates are racy with respect to the
    // actual queue size, so it can be negative at some points of time.
    size: Arc<AtomicI64>,
    inner: Sender<T>,
}

// Can't derive Clone since it puts Clone trait bound on T.
impl<T> Clone for ProfiledSender<T> {
    fn clone(&self) -> Self {
        Self {
            size: self.size.clone(),
            inner: self.inner.clone(),
        }
    }
}

#[allow(dead_code)]
impl<T> ProfiledSender<T> {
    pub fn try_send(&mut self, message: T) -> Result<(), TrySendError<T>> {
        let result = self.inner.try_send(message);
        if result.is_ok() {
            self.size.fetch_add(1, Ordering::Release);
        }
        result
    }

    pub async fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        let result = self.inner.send(value).await;
        if result.is_ok() {
            self.size.fetch_add(1, Ordering::Release);
        }
        result
    }
}

pub struct ProfiledReceiver<T> {
    size: Arc<AtomicI64>,
    inner: Receiver<T>,
}

#[allow(dead_code)]
impl<T> ProfiledReceiver<T> {
    pub fn close(&mut self) {
        self.inner.close()
    }

    pub async fn recv(&mut self) -> Option<T> {
        let result = self.inner.recv().await;
        if result.is_some() {
            self.size.fetch_sub(1, Ordering::Release);
        }
        result
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.size.fetch_sub(1, Ordering::Release);
        }
        result
    }

    pub fn approx_len(&self) -> i64 {
        self.size.load(Ordering::Acquire)
    }
}

pub struct ProfiledUnboundedSender<T> {
    // Use i64, not u64, as size counter because it's updates are racy with respect to the
    // actual queue size, so it can be negative at some points of time.
    size: Arc<AtomicI64>,
    inner: UnboundedSender<T>,
}

// Can't derive Clone since it puts Clone trait bound on T.
impl<T> Clone for ProfiledUnboundedSender<T> {
    fn clone(&self) -> Self {
        Self {
            size: self.size.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T> ProfiledUnboundedSender<T> {
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        let result = self.inner.send(value);
        if result.is_ok() {
            self.size.fetch_add(1, Ordering::Release);
        }
        result
    }
}

pub struct ProfiledUnboundedReceiver<T> {
    size: Arc<AtomicI64>,
    inner: UnboundedReceiver<T>,
}

#[allow(dead_code)]
impl<T> ProfiledUnboundedReceiver<T> {
    pub fn close(&mut self) {
        self.inner.close()
    }

    pub async fn recv(&mut self) -> Option<T> {
        let result = self.inner.recv().await;
        if result.is_some() {
            self.size.fetch_sub(1, Ordering::Release);
        }
        result
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let result = self.inner.try_recv();
        if result.is_ok() {
            self.size.fetch_sub(1, Ordering::Release);
        }
        result
    }

    pub fn approx_len(&self) -> i64 {
        self.size.load(Ordering::Acquire)
    }
}

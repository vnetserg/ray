use crate::errors::*;

use byteorder::{LittleEndian, ReadBytesExt};

use tokio::sync::mpsc::{
    channel,
    error::{SendError, TryRecvError, TrySendError},
    unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};

use uuid::Uuid;

use std::{
    io::{self, Read},
    panic::{catch_unwind, AssertUnwindSafe},
    process::Command,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

#[derive(Clone, Debug)]
pub struct Traced<T> {
    pub id: Uuid,
    pub payload: T,
}

impl<T> Traced<T> {
    pub fn new(payload: T) -> Self {
        let id = Uuid::new_v4();
        Self { id, payload }
    }

    pub fn with_id(id: Uuid, payload: T) -> Self {
        Self { id, payload }
    }

    pub fn into_payload(self) -> T {
        self.payload
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, func: F) -> Traced<U> {
        Traced::<U> {
            id: self.id,
            payload: func(self.payload),
        }
    }
}

pub fn do_and_die<F: FnOnce() -> Result<()>>(func: F) -> ! {
    let result = catch_unwind(AssertUnwindSafe(func));
    let thread_name = std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_string();

    match result {
        Ok(Ok(())) => error!("Thread '{}' finished unexpectedly", thread_name),
        Ok(Err(err)) => error!(
            "Thread '{}' failed (error chain below)\n{}",
            thread_name,
            err.display_fancy_chain()
        ),
        Err(_) => (), // panic occured, error is already logged by the panic hook.
    }

    std::process::exit(1);
}

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

fn run_shell_command(command: &str) -> Result<String> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .chain_err(|| format!("failed to run command '{}'", command))?;
    if !output.status.success() {
        bail!("command '{}' exited non-zero", command);
    }
    let text =
        std::str::from_utf8(&output.stdout).chain_err(|| "command output is not a valid utf-8")?;
    Ok(text.trim().to_string())
}

pub fn get_children_pids(parent_pid: u32) -> Result<Vec<u32>> {
    let cmd = format!("ls /proc/{}/task", parent_pid);
    let text = run_shell_command(&cmd)?;
    let pids = text
        .lines()
        .filter_map(|line| Some(line.parse().ok()?))
        .collect();
    Ok(pids)
}

pub fn get_process_name(pid: u32) -> Result<String> {
    let cmd = format!("cat /proc/{}/status | head -n1 | awk '{{print $2}}'", pid);
    let text = run_shell_command(&cmd)?;
    Ok(text)
}

pub fn get_process_cpu_time(pid: u32) -> Result<u64> {
    let cmd = format!(
        "cat /proc/{}/sched | grep se.sum_exec_runtime | awk '{{print $3}}'",
        pid
    );
    let text = run_shell_command(&cmd)?;
    let value: f64 = text
        .parse()
        .chain_err(|| format!("command output '{}' is not a float", text))?;
    Ok((value * 1000.) as u64)
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
    pub fn try_send(&mut self, message: T) -> std::result::Result<(), TrySendError<T>> {
        let result = self.inner.try_send(message);
        if result.is_ok() {
            self.size.fetch_add(1, Ordering::Release);
        }
        result
    }

    pub async fn send(&mut self, value: T) -> std::result::Result<(), SendError<T>> {
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

    pub fn try_recv(&mut self) -> std::result::Result<T, TryRecvError> {
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
    pub fn send(&self, value: T) -> std::result::Result<(), SendError<T>> {
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

    pub fn try_recv(&mut self) -> std::result::Result<T, TryRecvError> {
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

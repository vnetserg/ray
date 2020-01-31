use super::snapshot_service::{PersistentWrite, SnapshotStorage};

use crate::errors::*;

use chrono::Utc;

use std::{
    fs::{create_dir_all, read_dir, File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

pub struct SnapshotWriter {
    buffer: BufWriter<File>,
}

impl Write for SnapshotWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buffer.write(data)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }
}

impl PersistentWrite for SnapshotWriter {
    fn persist(&mut self) -> Result<()> {
        self.buffer.flush()?;
        self.buffer.get_ref().sync_data()?;
        Ok(())
    }
}

pub struct DirectorySnapshotStorage {
    path: PathBuf,
}

impl DirectorySnapshotStorage {
    pub fn new(path: &str) -> io::Result<Self> {
        let path = PathBuf::from(path);
        create_dir_all(path.as_path())?;
        Ok(Self { path })
    }
}

impl SnapshotStorage for DirectorySnapshotStorage {
    type Writer = SnapshotWriter;
    type Reader = BufReader<File>;

    fn create_snapshot(&mut self, name: &str) -> Result<Self::Writer> {
        let file_name = format!("{}_{}.snap", Utc::now().format("%+"), name);
        let path = Path::new(&self.path).join(file_name);
        debug!("Creating snapshot file: {:?}", path);

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .chain_err(|| format!("failed to open file for write: {:?}", path))?;
        let buffer = BufWriter::new(file);
        let writer = SnapshotWriter { buffer };

        Ok(writer)
    }

    fn open_last_snapshot(&self) -> Result<Option<Self::Reader>> {
        let mut latest = None;
        let dir_entries = read_dir(&self.path)
            .chain_err(|| format!("failed to read directory {:?}", self.path))?;
        for entry in dir_entries {
            let path = entry
                .chain_err(|| "failed to resolve directory entry")?
                .path();
            if path.is_file()
                && path.to_string_lossy().ends_with(".snap")
                && latest.as_ref().map(|prev| *prev < path).unwrap_or(true)
            {
                latest = Some(path.to_owned());
            }
        }
        if let Some(ref path) = latest {
            debug!("Latest snapshot found: {:?}", path);
            let file = OpenOptions::new()
                .read(true)
                .open(path)
                .chain_err(|| format!("failed to open file for read: {:?}", path))?;
            let reader = BufReader::new(file);
            Ok(Some(reader))
        } else {
            Ok(None)
        }
    }
}

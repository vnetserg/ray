use super::snapshot_service::{PersistentWrite, SnapshotStorage};

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
    fn persist(&mut self) -> io::Result<()> {
        self.buffer.flush()?;
        self.buffer.get_mut().sync_data()
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

    fn create_snapshot(&mut self, name: &str) -> io::Result<Self::Writer> {
        let file_name = format!("{}_{}.snap", Utc::now().format("%+"), name);
        let path = Path::new(&self.path).join(file_name);
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let buffer = BufWriter::new(file);
        let writer = SnapshotWriter { buffer };
        Ok(writer)
    }

    fn open_last_snapshot(&self) -> io::Result<Option<Self::Reader>> {
        let mut latest = None;
        for entry in read_dir(&self.path)? {
            let path = entry?.path();
            if path.is_file()
                && path.to_string_lossy().ends_with(".snap")
                && latest.as_ref().map(|prev| *prev < path).unwrap_or(true)
            {
                latest = Some(path.to_owned());
            }
        }
        if let Some(path) = latest {
            let file = OpenOptions::new().read(true).open(path)?;
            let reader = BufReader::new(file);
            Ok(Some(reader))
        } else {
            Ok(None)
        }
    }
}

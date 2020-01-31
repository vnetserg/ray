use super::{
    config::JournalStorageConfig,
    journal_service::{JournalReader, JournalWriter, ReadResult},
};

use crate::{errors::*, util::try_read_u32};

use chrono::Utc;

use byteorder::{LittleEndian, WriteBytesExt};

use std::{
    collections::VecDeque,
    fs::{create_dir_all, read_dir, remove_file, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

struct DirectoryJournalBase {
    directory_path: PathBuf,
    previous_files: VecDeque<(PathBuf, usize)>,
    total_blob_count: usize,
    file_size_soft_limit: usize,
}

impl DirectoryJournalBase {
    fn push_file(&mut self, path: PathBuf, blob_count: usize) {
        self.total_blob_count += blob_count;
        self.previous_files.push_back((path, blob_count));
    }

    fn dispose_oldest_blobs(&mut self, mut blob_count: usize) -> Result<()> {
        while !self.previous_files.is_empty() && blob_count >= self.previous_files[0].1 {
            let (ref path, file_blob_count) = self.previous_files[0];

            if let Err(err) = remove_file(path) {
                if err.kind() == io::ErrorKind::NotFound {
                    debug!("Journal file is already removed: {:?}", path);
                } else {
                    return Err(err).chain_err(|| format!("failed to remove {:?}", path));
                }
            } else {
                debug!("Removed journal file: {:?}", path);
            }

            self.total_blob_count -= file_blob_count;
            blob_count -= file_blob_count;
            self.previous_files.pop_front();
        }
        Ok(())
    }
}

pub struct DirectoryJournalReader {
    file_paths: VecDeque<PathBuf>,
    current_file: Option<BufReader<File>>,
    current_file_blob_count: usize,
    base: DirectoryJournalBase,
}

impl DirectoryJournalReader {
    pub fn new(config: &JournalStorageConfig) -> Result<Self> {
        let directory_path = PathBuf::from(&config.path);
        create_dir_all(directory_path.as_path())
            .chain_err(|| format!("failed to create directory {:?}", directory_path))?;

        let mut file_paths = vec![];
        let dir_entries = read_dir(&directory_path)
            .chain_err(|| format!("failed to read directory {:?}", directory_path))?;

        for entry in dir_entries {
            let file_path = entry.chain_err(|| "failed to resolve entry")?.path();
            if file_path.to_string_lossy().ends_with(".jnl") {
                file_paths.push(file_path.to_owned());
            }
        }

        file_paths.sort();

        let current_file = if file_paths.is_empty() {
            None
        } else {
            Some(Self::open_file(&file_paths[0])?)
        };

        let base = DirectoryJournalBase {
            directory_path,
            previous_files: VecDeque::new(),
            total_blob_count: 0,
            file_size_soft_limit: config.file_size_soft_limit,
        };

        let reader = Self {
            file_paths: file_paths.into(),
            current_file,
            current_file_blob_count: 0,
            base,
        };

        Ok(reader)
    }

    fn open_file(path: &Path) -> Result<BufReader<File>> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .chain_err(|| format!("failed to open file for read: {:?}", path))?;
        Ok(BufReader::new(file))
    }

    fn read_len(&mut self) -> Result<Option<usize>> {
        while let Some(ref mut file) = self.current_file {
            match try_read_u32(file)? {
                None => {
                    let path = self.file_paths.pop_front().unwrap();
                    self.base.push_file(path, self.current_file_blob_count);

                    if self.file_paths.is_empty() {
                        self.current_file = None;
                        break;
                    } else {
                        self.current_file = Some(Self::open_file(&self.file_paths[0])?);
                        self.current_file_blob_count = 0;
                    }
                }
                Some(len) => return Ok(Some(len)),
            }
        }

        Ok(None)
    }
}

impl JournalReader for DirectoryJournalReader {
    type Writer = DirectoryJournalWriter;

    fn read_blob(mut self) -> Result<ReadResult<Self, Self::Writer>> {
        let len = match self.read_len()? {
            Some(len) => len,
            None => {
                let writer = DirectoryJournalWriter::new(self.base)?;
                return Ok(ReadResult::End(writer));
            }
        };

        let mut blob = vec![0; len as usize];
        self.current_file.as_mut().unwrap().read_exact(&mut blob)?;

        self.current_file_blob_count += 1;

        Ok(ReadResult::Blob(blob, self))
    }
}

pub struct DirectoryJournalWriter {
    file: BufWriter<File>,
    file_path: PathBuf,
    current_file_size: usize,
    current_file_blob_count: usize,
    base: DirectoryJournalBase,
}

impl DirectoryJournalWriter {
    fn new(base: DirectoryJournalBase) -> Result<Self> {
        let (file, file_path) = Self::open_new_file(&base.directory_path)?;
        let writer = Self {
            file,
            file_path,
            current_file_size: 0,
            current_file_blob_count: 0,
            base,
        };
        Ok(writer)
    }

    fn open_new_file(directory_path: &Path) -> Result<(BufWriter<File>, PathBuf)> {
        let file_name = format!("{}.jnl", Utc::now().format("%+"));
        let path = Path::new(&directory_path).join(file_name);
        debug!("Starting new journal file: {:?}", path);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .chain_err(|| format!("failed to open file for write: {:?}", path))?;
        Ok((BufWriter::new(file), path))
    }
}

impl JournalWriter for DirectoryJournalWriter {
    fn append_blob(&mut self, blob: &[u8]) -> Result<()> {
        assert!(blob.len() >> 32 == 0);
        self.current_file_size += blob.len() + 4;
        self.current_file_blob_count += 1;
        self.file
            .write_u32::<LittleEndian>(blob.len() as u32)
            .and_then(|_| self.file.write_all(blob))
            .chain_err(|| format!("failed to write to {:?}", self.file_path))?;
        Ok(())
    }

    fn persist(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        if self.current_file_size >= self.base.file_size_soft_limit {
            let (new_file, new_file_path) = Self::open_new_file(&self.base.directory_path)?;
            self.base.push_file(
                std::mem::replace(&mut self.file_path, new_file_path),
                self.current_file_blob_count,
            );
            self.file = new_file;
            self.current_file_size = 0;
            self.current_file_blob_count = 0;
        }
        Ok(())
    }

    fn get_blob_count(&self) -> usize {
        self.base.total_blob_count + self.current_file_blob_count
    }

    fn dispose_oldest_blobs(&mut self, blob_count: usize) -> Result<()> {
        if blob_count > self.current_file_blob_count {
            self.base
                .dispose_oldest_blobs(blob_count - self.current_file_blob_count)
        } else {
            Ok(())
        }
    }
}

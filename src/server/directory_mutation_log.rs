use super::{
    config::MutationLogConfig,
    log_service::{PersistentLogReader, PersistentLogWriter, ReadResult},
};

use chrono::Utc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::{
    collections::VecDeque,
    fs::{create_dir_all, read_dir, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

pub struct DirectoryMutationLogReader {
    path: PathBuf,
    file_paths: VecDeque<PathBuf>,
    current_file: Option<BufReader<File>>,
    writer_size_limit: usize,
}

impl DirectoryMutationLogReader {
    pub fn new(config: &MutationLogConfig) -> io::Result<Self> {
        let path = PathBuf::from(&config.path);
        create_dir_all(path.as_path())?;

        let mut file_paths = vec![];
        for entry in read_dir(&path)? {
            let file_path = entry?.path();
            if file_path.to_string_lossy().ends_with(".jnl") {
                file_paths.push(file_path.to_owned());
            }
        }

        file_paths.sort();

        let mut reader = Self {
            path,
            file_paths: file_paths.into(),
            current_file: None,
            writer_size_limit: config.soft_file_size_limit,
        };

        reader.open_next_file()?;

        Ok(reader)
    }

    fn open_next_file(&mut self) -> io::Result<()> {
        if self.file_paths.is_empty() {
            self.current_file = None;
        } else {
            let file = OpenOptions::new().read(true).open(&self.file_paths[0])?;
            let reader = BufReader::new(file);
            self.current_file = Some(reader);
            self.file_paths.pop_front();
        }
        Ok(())
    }
}

impl PersistentLogReader for DirectoryMutationLogReader {
    type Writer = DirectoryMutationLogWriter;

    fn read_blob(mut self) -> io::Result<ReadResult<Self, Self::Writer>> {
        let mut buffer = [0u8; 4];
        while let Some(ref mut file) = self.current_file {
            if let Err(err) = file.read_exact(&mut buffer[..1]) {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    self.open_next_file()?;
                } else {
                    return Err(err);
                }
            } else {
                break;
            }
        }

        if self.current_file.is_none() {
            let writer = DirectoryMutationLogWriter::new(self.path, self.writer_size_limit)?;
            return Ok(ReadResult::End(writer));
        }

        let file = self.current_file.as_mut().unwrap();

        file.read_exact(&mut buffer[1..])?;
        let len = (&buffer[..]).read_u32::<LittleEndian>().unwrap();

        let mut blob = vec![0; len as usize];
        file.read_exact(&mut blob)?;

        Ok(ReadResult::Blob(blob, self))
    }
}

pub struct DirectoryMutationLogWriter {
    directory_path: PathBuf,
    file: BufWriter<File>,
    size: usize,
    size_limit: usize,
}

impl DirectoryMutationLogWriter {
    fn new(directory_path: PathBuf, size_limit: usize) -> io::Result<Self> {
        let file = Self::open_new_file(&directory_path)?;
        let writer = Self {
            directory_path,
            file,
            size_limit,
            size: 0,
        };
        Ok(writer)
    }

    fn open_new_file(directory_path: &Path) -> io::Result<BufWriter<File>> {
        let file_name = format!("{}.jnl", Utc::now().format("%+"));
        let path = Path::new(&directory_path).join(file_name);
        debug!("Opening new journal: {:?}", path);
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(BufWriter::new(file))
    }
}

impl PersistentLogWriter for DirectoryMutationLogWriter {
    fn append_blob(&mut self, blob: &[u8]) -> io::Result<()> {
        assert!(blob.len() >> 32 == 0);
        self.size += blob.len() + 4;
        self.file
            .write_u32::<LittleEndian>(blob.len() as u32)
            .and_then(|_| self.file.write_all(blob))
    }

    fn persist(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        if self.size >= self.size_limit {
            self.file = Self::open_new_file(&self.directory_path)?;
        }
        Ok(())
    }
}

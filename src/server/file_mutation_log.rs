use super::{
    config::MutationLogConfig,
    log_service::{PersistentLogReader, PersistentLogWriter, ReadResult},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::{
    fs,
    io::{self, BufReader, BufWriter, Read, Write},
};

pub struct FileMutationLogReader {
    reader: BufReader<fs::File>,
}

impl FileMutationLogReader {
    pub fn new(config: &MutationLogConfig) -> io::Result<Self> {
        let maybe_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&config.path);
        maybe_file.map(|file| {
            let reader = BufReader::new(file);
            Self { reader }
        })
    }
}

impl PersistentLogReader for FileMutationLogReader {
    type Writer = FileMutationLogWriter;

    fn read_blob(mut self) -> io::Result<ReadResult<Self, Self::Writer>> {
        let mut buffer = [0u8; 4];
        if let Err(err) = self.reader.read_exact(&mut buffer[..1]) {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                let writer = BufWriter::new(self.reader.into_inner());
                return Ok(ReadResult::End(FileMutationLogWriter { writer }));
            } else {
                return Err(err);
            }
        }

        self.reader.read_exact(&mut buffer[1..])?;
        let len = (&buffer[..]).read_u32::<LittleEndian>().unwrap();

        let mut blob = vec![0; len as usize];
        self.reader.read_exact(&mut blob)?;

        Ok(ReadResult::Blob(blob, self))
    }
}

pub struct FileMutationLogWriter {
    writer: BufWriter<fs::File>,
}

impl PersistentLogWriter for FileMutationLogWriter {
    fn append_blob(&mut self, blob: &[u8]) -> io::Result<()> {
        self.writer
            .write_u32::<LittleEndian>(blob.len() as u32)
            .and_then(|_| self.writer.write_all(blob))
    }

    fn persist(&mut self) -> io::Result<()> {
        self.writer
            .flush()
            .and_then(|_| self.writer.get_ref().sync_data())
    }
}

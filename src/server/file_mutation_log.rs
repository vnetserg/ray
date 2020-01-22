use super::{config::MutationLogConfig, log_service::PersistentLog};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::{
    fs,
    io::{self, Read, Write},
};

enum LogMode {
    Reading(io::BufReader<fs::File>),
    Writing(io::BufWriter<fs::File>),
}

pub struct FileMutationLog {
    mode: LogMode,
}

impl FileMutationLog {
    pub fn new(config: &MutationLogConfig) -> io::Result<Self> {
        let maybe_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&config.path);
        maybe_file.map(|file| {
            let reader = io::BufReader::new(file);
            Self {
                mode: LogMode::Reading(reader),
            }
        })
    }

    fn get_writer(&mut self) -> &mut io::BufWriter<fs::File> {
        if let LogMode::Reading(ref reader) = self.mode {
            let file = reader.get_ref().try_clone().expect("File clone failed");
            let writer = io::BufWriter::new(file);
            self.mode = LogMode::Writing(writer);
        }
        match self.mode {
            LogMode::Writing(ref mut writer) => writer,
            _ => unreachable!(),
        }
    }
}

impl PersistentLog for FileMutationLog {
    fn read_blob(&mut self) -> io::Result<Option<Vec<u8>>> {
        let reader = match self.mode {
            LogMode::Reading(ref mut reader) => reader,
            _ => panic!("Can not switch mutation log from writing back to reading"),
        };

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
        let mut blob = vec![0; len as usize];
        reader.read_exact(&mut blob)?;

        Ok(Some(blob))
    }

    fn append_blob(&mut self, blob: &[u8]) -> io::Result<()> {
        let writer = self.get_writer();
        writer
            .write_u32::<LittleEndian>(blob.len() as u32)
            .and_then(|_| writer.write_all(blob))
    }

    fn persist(&mut self) -> io::Result<()> {
        let writer = self.get_writer();
        writer.flush().and_then(|_| writer.get_ref().sync_data())
    }
}

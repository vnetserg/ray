use super::{
    config::MutationLogConfig,
    log_service::PersistentLog,
};

use std::{
    fs,
    io::{
        self,
        Read,
        Write,
    },
};

enum LogMode {
    Reading(io::BufReader<fs::File>),
    Writing(io::BufWriter<fs::File>),
}

pub struct FileMutationLog {
    mode: LogMode,
}

impl FileMutationLog {
    pub fn new(config: &MutationLogConfig) -> Self {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&config.path)
            .unwrap_or_else(|err| panic!("Failed to open '{}': {}", config.path, err));
        let reader = io::BufReader::new(file);
        Self {
            mode: LogMode::Reading(reader)
        }
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

impl Read for FileMutationLog {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.mode {
            LogMode::Reading(ref mut reader) => reader.read(buf),
            _ => panic!("Can not switch mutation log from writing back to reading"),
        }
    }
}

impl Write for FileMutationLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let writer = self.get_writer();
        writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let writer = self.get_writer();
        writer.flush()
    }
}

impl PersistentLog for FileMutationLog {
    fn persist(&mut self) -> io::Result<()> {
        let writer = self.get_writer();
        writer.flush().and_then(|_| writer.get_ref().sync_data())
    }
}

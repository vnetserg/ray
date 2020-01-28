use crate::{proto, server::machine_service::Machine};

use prost::Message;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::{
    collections::HashMap,
    io::{self, Read, Write},
};

#[derive(Default, Clone)]
pub struct StorageMachine {
    map: HashMap<Box<[u8]>, Box<[u8]>>,
}

impl Machine for StorageMachine {
    type Mutation = proto::SetRequest;
    type Query = Box<[u8]>;
    type Status = Box<[u8]>;

    fn apply_mutation(&mut self, mutation: Self::Mutation) {
        let key = mutation.key.into_boxed_slice();
        let value = mutation.value.into_boxed_slice();
        self.map.insert(key, value);
    }

    fn query_state(&self, query: Self::Query) -> Self::Status {
        self.map
            .get(&query)
            .cloned()
            .unwrap_or_else(|| Vec::new().into_boxed_slice())
    }

    fn write_snapshot<T: Write>(&self, writer: &mut T) -> io::Result<()> {
        for (key, value) in self.map.iter() {
            let set = proto::SetRequest {
                key: key.to_vec(),
                value: value.to_vec(),
            };

            let len = set.encoded_len();
            let mut buf = vec![0; len + 4];

            assert!(len >> 32 == 0);
            (&mut buf[..4])
                .write_u32::<LittleEndian>(len as u32)
                .unwrap();
            set.encode(&mut &mut buf[4..])
                .expect("Failed to encode proto message");

            writer.write_all(&buf)?;
        }

        Ok(())
    }

    fn from_snapshot<T: Read>(reader: &mut T) -> io::Result<Self> {
        let mut machine = Self::default();

        let read_length = |rd: &mut T| -> io::Result<Option<usize>> {
            let mut buffer = [0u8; 4];
            if let Err(err) = rd.read_exact(&mut buffer[..1]) {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(err);
                }
            }
            rd.read_exact(&mut buffer[1..])?;
            let len = (&buffer[..]).read_u32::<LittleEndian>().unwrap();
            Ok(Some(len as usize))
        };

        while let Some(len) = read_length(reader)? {
            let mut buffer = vec![0; len];
            reader.read_exact(&mut buffer)?;
            let set = proto::SetRequest::decode(&buffer[..])?;
            let key = set.key.into_boxed_slice();
            let value = set.value.into_boxed_slice();
            machine.map.insert(key, value);
        }

        Ok(machine)
    }
}

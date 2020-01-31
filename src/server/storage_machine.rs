use crate::{errors::*, proto, server::machine_service::Machine, util::try_read_u32};

use prost::Message;

use byteorder::{LittleEndian, WriteBytesExt};

use std::{
    collections::HashMap,
    io::{Read, Write},
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

    fn write_snapshot<T: Write>(&self, writer: &mut T) -> Result<()> {
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
            set.encode(&mut &mut buf[4..])?;

            writer.write_all(&buf)?;
        }

        Ok(())
    }

    fn from_snapshot<T: Read>(reader: &mut T) -> Result<Self> {
        let mut machine = Self::default();
        let mut index = 0;
        let mut offset = 0;

        while let Some(len) = try_read_u32(reader)? {
            let mut buffer = vec![0; len];
            reader.read_exact(&mut buffer)?;

            let set = proto::SetRequest::decode(&buffer[..]).chain_err(|| {
                format!(
                    "failed to decode mutation (index: {}, offset: {})",
                    index, offset
                )
            })?;

            let key = set.key.into_boxed_slice();
            let value = set.value.into_boxed_slice();
            machine.map.insert(key, value);

            index += 1;
            offset += 4 + buffer.len();
        }

        Ok(machine)
    }
}

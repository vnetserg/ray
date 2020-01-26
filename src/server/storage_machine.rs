use crate::{proto, server::machine_service::Machine};

use prost::Message;

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
            let mut buf = vec![0; len + 8];
            set.encode_length_delimited(&mut buf)
                .expect("Failed to encode proto message");

            writer.write_all(&buf)?;
        }

        Ok(())
    }

    fn from_snapshot<T: Read>(_reader: &mut T) -> io::Result<Self> {
        // TODO
        Ok(Self::default())
    }
}

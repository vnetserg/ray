use crate::{proto, server::machine_service::Machine};

use std::collections::HashMap;

#[derive(Default)]
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
}

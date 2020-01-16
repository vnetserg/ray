use super::{
    storage_machine::StorageMachine,
    machine_service::MachineServiceHandle,
};

use crate::proto::{
    storage_server::Storage,
    SetRequest,
    SetReply,
    GetRequest,
    GetReply,
};

use tonic::{
    Request,
    Response,
    Status
};


pub struct RayStorageService {
    handle: MachineServiceHandle<StorageMachine>,
}

impl RayStorageService {
    pub fn new(handle: MachineServiceHandle<StorageMachine>) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl Storage for RayStorageService {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetReply>, Status> {
        println!("Got a request: {:?}", request);

        self.handle.apply_mutation(request.into_inner()).await;

        let reply = SetReply {};
        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        println!("Got a request: {:?}", request);

        let request = request.into_inner();
        let key = request.key.into_boxed_slice();
        let value = self.handle.query_state(key).await;

        let reply = GetReply { value: value.to_vec() };
        Ok(Response::new(reply))
    }
}

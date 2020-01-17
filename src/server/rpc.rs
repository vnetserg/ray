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
    Status,
    Code,
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
        let remote_addr = request.remote_addr().ok_or(Status::new(Code::Aborted, "unknown IP"))?;

        debug!("New request: {} (remote: {})",
           request.get_ref(),
           remote_addr);

        self.handle.apply_mutation(request.into_inner()).await;

        let reply = SetReply {};
        debug!("Replying: {} (to: {})", reply, remote_addr);

        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let remote_addr = request.remote_addr().ok_or(Status::new(Code::Aborted, "unknown IP"))?;

        debug!("Request received: {} (remote: {})",
           request.get_ref(),
           remote_addr);

        let request = request.into_inner();
        let key = request.key.into_boxed_slice();
        let value = self.handle.query_state(key).await;

        let reply = GetReply { value: value.to_vec() };
        debug!("Replying: {} (to: {})", reply, remote_addr);

        Ok(Response::new(reply))
    }
}

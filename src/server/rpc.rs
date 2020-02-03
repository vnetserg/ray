use super::{machine_service::MachineServiceHandle, storage_machine::StorageMachine};

use metrics::{counter, timing};

use crate::proto::{storage_server::Storage, GetReply, GetRequest, SetReply, SetRequest};

use tonic::{Code, Request, Response, Status};

use std::{
    fmt::{Debug, Display},
    future::Future,
    pin::Pin,
    time::Instant,
};

pub struct RayStorageService {
    handle: MachineServiceHandle<StorageMachine>,
}

#[tonic::async_trait]
trait RequestHandler {
    type Request: Debug + Display;
    type Response: Debug + Display;
    const METHOD_NAME: &'static str;

    async fn handle_request(
        request: Self::Request,
        handle: MachineServiceHandle<StorageMachine>,
    ) -> Result<Self::Response, Status>;
}

struct SetRequestHandler {}

#[tonic::async_trait]
impl RequestHandler for SetRequestHandler {
    type Request = SetRequest;
    type Response = SetReply;
    const METHOD_NAME: &'static str = "set";

    async fn handle_request(
        request: Self::Request,
        mut handle: MachineServiceHandle<StorageMachine>,
    ) -> Result<Self::Response, Status> {
        handle.apply_mutation(request).await?;
        Ok(SetReply {})
    }
}

struct GetRequestHandler {}

#[tonic::async_trait]
impl RequestHandler for GetRequestHandler {
    type Request = GetRequest;
    type Response = GetReply;
    const METHOD_NAME: &'static str = "get";

    async fn handle_request(
        request: Self::Request,
        mut handle: MachineServiceHandle<StorageMachine>,
    ) -> Result<Self::Response, Status> {
        let key = request.key.into_boxed_slice();
        let value = handle.query_state(key).await?;

        Ok(GetReply {
            value: value.to_vec(),
        })
    }
}

impl RayStorageService {
    pub fn new(handle: MachineServiceHandle<StorageMachine>) -> Self {
        Self { handle }
    }

    async fn handle_request<T: RequestHandler>(
        &self,
        request: Request<T::Request>,
    ) -> Result<Response<T::Response>, Status> {
        let start = Instant::now();
        counter!("rayd.rpc.request_count", 1, "method" => T::METHOD_NAME);

        let inner = async {
            let remote_addr = request
                .remote_addr()
                .ok_or_else(|| Status::new(Code::Aborted, "unknown IP"))?;

            debug!(
                "New request: {} (remote: {})",
                request.get_ref(),
                remote_addr
            );

            let response = T::handle_request(request.into_inner(), self.handle.clone())
                .await
                .map(Response::new);

            debug!("Replying: {:?} (to: {})", response, remote_addr);

            response
        };

        let response = inner.await;
        if response.is_err() {
            counter!("rayd.rpc.error_count", 1, "method" => T::METHOD_NAME);
        }

        timing!("rayd.rpc.request_duration", start, Instant::now(), "method" => T::METHOD_NAME);

        response
    }
}

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Don't use async_trait macro to avoid one excessive heap allocation.
impl Storage for RayStorageService {
    fn set<'a, 'b>(
        &'a self,
        request: Request<SetRequest>,
    ) -> BoxedFuture<'b, Result<Response<SetReply>, Status>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.handle_request::<SetRequestHandler>(request))
    }

    fn get<'a, 'b>(
        &'a self,
        request: Request<GetRequest>,
    ) -> BoxedFuture<'a, Result<Response<GetReply>, Status>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.handle_request::<GetRequestHandler>(request))
    }
}

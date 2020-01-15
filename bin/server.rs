use ray::{
    config::DEFAULT_PORT,
    proto::{
        storage_server::{
            Storage,
            StorageServer
        },
        SetRequest,
        SetReply,
        GetRequest,
        GetReply,
    },
    server::{
        run_storage,
        StorageHandle,
        FileMutationLog,
    }
};

use tonic::{
    transport::Server,
    Request,
    Response,
    Status
};

use std::net::SocketAddr;

struct RayStorageService {
    handle: StorageHandle,
}

#[tonic::async_trait]
impl Storage for RayStorageService {
    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetReply>, Status> {
        println!("Got a request: {:?}", request);

        self.handle.apply_mutation(request.into_inner()).await;

        let reply = SetReply {};
        Ok(Response::new(reply))
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        println!("Got a request: {:?}", request);

        let request = request.into_inner();
        let key = request.key.into_boxed_slice();
        let value = self.handle.query_state(key).await;

        let reply = GetReply { value: value.to_vec() };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), DEFAULT_PORT);
    let log = FileMutationLog::new("rayd.log");
    let storage = RayStorageService {
        handle: run_storage(log),
    };

    Server::builder()
        .add_service(StorageServer::new(storage))
        .serve(addr)
        .await?;

    Ok(())
}

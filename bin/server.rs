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
};

use tonic::{
    transport::Server,
    Request,
    Response,
    Status
};

use futures::lock::Mutex;

use std::{
	collections::HashMap,
	net::SocketAddr,
};

type Data = Box<[u8]>;

#[derive(Default)]
struct RayStorage {
    map: Mutex<HashMap<Data, Data>>,
}

#[tonic::async_trait]
impl Storage for RayStorage {
    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetReply>, Status> {
        println!("Got a request: {:?}", request);

        let request = request.into_inner();
        let key = request.key.into_boxed_slice();

        let previous = {
            let mut map = self.map.lock().await;
            match request.value {
                Some(value) => map.insert(key, value.into()),
                None => map.remove(&key),
            }
        };

        let reply = SetReply { previous: previous.map(|value| value.into()) };
        Ok(Response::new(reply))
    }

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        println!("Got a request: {:?}", request);

        let request = request.into_inner();
        let key = request.key.into_boxed_slice();
        let value = self.map.lock().await.get(&key).cloned();

        let reply = GetReply { value: value.map(|value| value.into()) };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), DEFAULT_PORT);
    let storage = RayStorage::default();

    Server::builder()
        .add_service(StorageServer::new(storage))
        .serve(addr)
        .await?;

    Ok(())
}

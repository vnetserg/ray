use super::proto;

use tonic::{
    Request,
    Status,
    transport::{
        Channel,
        Error,
    },
};

pub struct RayClient {
    client: proto::storage_client::StorageClient<Channel>,
}

impl RayClient {
    pub async fn connect(address: &str, port: u16) -> Result<Self, Error> {
        let url = format!("http://{}:{}", address, port);
        proto::storage_client::StorageClient::connect(url).await
            .map(|client| RayClient { client } )
    }

    pub async fn get(&mut self, key: Vec<u8>) -> Result<Vec<u8>, Status> {
        let request = Request::new(proto::GetRequest { key });
        let response = self.client.get(request).await;
        response.map(|resp| resp.into_inner().value)
    }

    pub async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<Vec<u8>, Status> {
        let request = Request::new(proto::SetRequest { key, value });
        let response = self.client.set(request).await;
        response.map(|resp| resp.into_inner().previous)
    }
}

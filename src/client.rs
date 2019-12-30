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

    pub async fn get(&mut self, key: Box<[u8]>) -> Result<Option<Box<[u8]>>, Status> {
        let request = Request::new(proto::GetRequest { key: key.to_vec() });
        let response = self.client.get(request).await;
        response.map(|resp| resp.into_inner().value.map(|value| value.into()))
    }

    pub async fn set(&mut self, key: Box<[u8]>, value: Box<[u8]>) -> Result<Option<Box<[u8]>>, Status> {
        let request = Request::new(proto::SetRequest {
            key: key.to_vec(),
            value: Some(value.into())
        });
        let response = self.client.set(request).await;
        response.map(|resp| resp.into_inner().previous.map(|value| value.into()))
    }
}

use std::{io, mem::take};

use async_trait::async_trait;
use bincode::Options;
use futures::{AsyncRead, AsyncWrite};
use libp2p::{
    core::upgrade::{read_length_prefixed, write_length_prefixed},
    request_response::{ProtocolName, RequestResponseCodec},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum Command {
    Put(Vec<u8>, oneshot::Sender<[u8; 32]>),
    Get([u8; 32], oneshot::Sender<Vec<u8>>),
}

pub struct PeerHandle(pub mpsc::Sender<Command>);
impl PeerHandle {
    pub async fn put(&self, object: Vec<u8>) -> [u8; 32] {
        let wait_put = oneshot::channel();
        self.0.send(Command::Put(object, wait_put.0)).await.unwrap();
        wait_put.1.await.unwrap()
    }

    pub async fn get(&self, id: [u8; 32]) -> Vec<u8> {
        let wait_get = oneshot::channel();
        self.0.send(Command::Get(id, wait_get.0)).await.unwrap();
        wait_get.1.await.unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FileExchangeProtocol;
#[derive(Debug, Clone, Copy)]
pub struct FileExchangeCodec;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileRequest {
    Push(Vec<u8>),
    Pull([u8; 32]),
    PushFrag([u8; 32], u32, Vec<u8>),
    PullFrag([u8; 32], u32), // append proof from VRF
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileResponse {
    PushOk([u8; 32]),
    PullOk(Vec<u8>),
    PushFragOk,
    PullFragOk([u8; 32], u32, Vec<u8>),
}

impl ProtocolName for FileExchangeProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/exchange/0.1.0".as_bytes()
    }
}

#[async_trait]
impl RequestResponseCodec for FileExchangeCodec {
    type Protocol = FileExchangeProtocol;
    type Request = FileRequest;
    type Response = FileResponse;

    async fn read_request<T: AsyncRead + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request> {
        let mut request = bincode::options()
            .deserialize(&read_length_prefixed(io, 1024).await.unwrap())
            .unwrap();
        if let FileRequest::Push(object) = &mut request {
            *object = read_length_prefixed(io, 2 << 30).await.unwrap()
        }
        Ok(request)
    }

    async fn read_response<T: AsyncRead + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response> {
        let mut response = bincode::options()
            .deserialize(&read_length_prefixed(io, 1024).await.unwrap())
            .unwrap();
        if let FileResponse::PullOk(object) = &mut response {
            *object = read_length_prefixed(io, 2 << 30).await.unwrap()
        }
        Ok(response)
    }

    async fn write_request<T: AsyncWrite + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        mut data: Self::Request,
    ) -> io::Result<()> {
        match &mut data {
            FileRequest::Push(object) => {
                let object = take(object);
                write_length_prefixed(io, &bincode::options().serialize(&data).unwrap())
                    .await
                    .unwrap();
                write_length_prefixed(io, object).await.unwrap()
            }
            data => write_length_prefixed(io, &bincode::options().serialize(data).unwrap())
                .await
                .unwrap(),
        }
        Ok(())
    }

    async fn write_response<T: AsyncWrite + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        mut data: Self::Response,
    ) -> io::Result<()> {
        match &mut data {
            FileResponse::PullOk(object) => {
                let object = take(object);
                write_length_prefixed(io, &bincode::options().serialize(&data).unwrap())
                    .await
                    .unwrap();
                write_length_prefixed(io, object).await.unwrap()
            }
            data => write_length_prefixed(io, &bincode::options().serialize(data).unwrap())
                .await
                .unwrap(),
        }
        Ok(())
    }
}

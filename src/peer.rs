use std::{io, mem::take};

use async_trait::async_trait;
use bincode::Options;
use futures::{AsyncRead, AsyncWrite};
use libp2p::{
    core::upgrade::{read_length_prefixed, write_length_prefixed},
    identity::{self, ed25519::SecretKey, Keypair},
    request_response::{ProtocolName, RequestResponseCodec},
    Multiaddr,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum Command {
    Put(Vec<u8>, oneshot::Sender<[u8; 32]>),
    Get([u8; 32], usize, oneshot::Sender<Vec<u8>>),
}

pub struct PeerHandle(pub mpsc::Sender<Command>);
impl PeerHandle {
    pub async fn put(&self, object: Vec<u8>) -> [u8; 32] {
        let wait_put = oneshot::channel();
        self.0.send(Command::Put(object, wait_put.0)).await.unwrap();
        wait_put.1.await.unwrap()
    }

    pub async fn get(&self, size: usize, id: [u8; 32]) -> Vec<u8> {
        let wait_get = oneshot::channel();
        self.0
            .send(Command::Get(id, size, wait_get.0))
            .await
            .unwrap();
        wait_get.1.await.unwrap()
    }
}

pub fn addr_to_keypair(addr: &Multiaddr) -> Keypair {
    let mut bytes = addr.to_vec();
    if bytes.len() > 32 {
        bytes = bytes[bytes.len() - 32..].to_vec();
    } else if bytes.len() < 32 {
        bytes = [vec![0; 32 - bytes.len()], bytes].concat();
    }
    identity::Keypair::Ed25519(From::from(SecretKey::from_bytes(&mut bytes).unwrap()))
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
            .deserialize(&read_length_prefixed(io, 1024).await?)
            .unwrap();
        if let FileRequest::Push(object) = &mut request {
            *object = read_length_prefixed(io, 2 << 30).await?
        } else if let FileRequest::PushFrag(_, _, frag) = &mut request {
            *frag = read_length_prefixed(io, 2 << 30).await?
        }
        Ok(request)
    }

    async fn read_response<T: AsyncRead + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response> {
        let mut response = bincode::options()
            .deserialize(&read_length_prefixed(io, 1024).await?)
            .unwrap();
        if let FileResponse::PullOk(object) = &mut response {
            *object = read_length_prefixed(io, 2 << 30).await?
        } else if let FileResponse::PullFragOk(_, _, frag) = &mut response {
            *frag = read_length_prefixed(io, 2 << 30).await?
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
                write_length_prefixed(io, &bincode::options().serialize(&data).unwrap()).await?;
                write_length_prefixed(io, object).await?
            }
            FileRequest::PushFrag(_, _, frag) => {
                let frag = take(frag);
                write_length_prefixed(io, &bincode::options().serialize(&data).unwrap()).await?;
                write_length_prefixed(io, frag).await?
            }
            data => write_length_prefixed(io, &bincode::options().serialize(data).unwrap()).await?,
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
                write_length_prefixed(io, &bincode::options().serialize(&data).unwrap()).await?;
                write_length_prefixed(io, object).await?;
            }
            FileResponse::PullFragOk(_, _, frag) => {
                let frag = take(frag);
                write_length_prefixed(io, &bincode::options().serialize(&data).unwrap()).await?;
                write_length_prefixed(io, frag).await?;
            }
            data => write_length_prefixed(io, &bincode::options().serialize(data).unwrap()).await?,
        }
        Ok(())
    }
}

use std::{collections::HashMap, io, mem::take};

use async_trait::async_trait;
use bincode::Options;
use futures::{AsyncRead, AsyncWrite, StreamExt};
use libp2p::{
    core::upgrade::{read_length_prefixed, write_length_prefixed, Version},
    identity,
    kad::{
        record::Key, store::MemoryStore, GetProvidersOk, Kademlia, KademliaConfig, KademliaEvent,
        QueryId, QueryResult,
    },
    mdns,
    mplex::MplexConfig,
    noise::NoiseAuthenticated,
    request_response::{
        ProtocolName, ProtocolSupport, RequestResponse, RequestResponseCodec, RequestResponseEvent,
        RequestResponseMessage, ResponseChannel,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, PeerId, Swarm, Transport,
};
use rand::{seq::SliceRandom, thread_rng};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};

#[derive(NetworkBehaviour)]
pub struct KadFsBehaviour {
    kademlia: Kademlia<MemoryStore>,
    exchange: RequestResponse<FileExchangeCodec>,
    mdns: mdns::tokio::Behaviour,
}
type Event = <KadFsBehaviour as NetworkBehaviour>::OutEvent;

pub struct KadFs {
    swarm: Swarm<KadFsBehaviour>,
    peers: Vec<PeerId>,
    wait_put: Option<oneshot::Sender<()>>,
    wait_get: Option<oneshot::Sender<Vec<u8>>>,
    objects: Option<([u8; 32], Vec<u8>)>,
    // StartProviding query => response channel
    push_peers: HashMap<QueryId, ResponseChannel<FileResponse>>,
    command: mpsc::Receiver<Command>,
    command_sender: mpsc::Sender<Command>,
    // on client side whether a pull request is already sent to a provider
    is_pulling: bool,
}

impl KadFs {
    pub fn new() -> Self {
        let id_keys = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(id_keys.public());
        let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
            .upgrade(Version::V1)
            .authenticate(NoiseAuthenticated::xx(&id_keys).unwrap())
            .multiplex(MplexConfig::new())
            .boxed();
        let kademlia = Kademlia::with_config(
            peer_id,
            MemoryStore::new(peer_id),
            KademliaConfig::default(),
        );
        let exchange = RequestResponse::new(
            FileExchangeCodec,
            [(FileExchangeProtocol, ProtocolSupport::Full)],
            Default::default(),
        );
        let mut swarm = Swarm::with_tokio_executor(
            transport,
            KadFsBehaviour {
                kademlia,
                exchange,
                mdns: mdns::Behaviour::new(Default::default()).unwrap(),
            },
            peer_id,
        );
        swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .unwrap();
        let (command_sender, command) = mpsc::channel(1);
        Self {
            swarm,
            peers: Default::default(),
            wait_put: None,
            wait_get: None,
            push_peers: Default::default(),
            objects: None,
            command,
            command_sender,
            is_pulling: false,
        }
    }

    pub fn handle(&self) -> KadFsHandle {
        KadFsHandle(self.command_sender.clone())
    }

    fn put(&mut self, id: [u8; 32], object: Vec<u8>, wait_put: oneshot::Sender<()>) {
        assert!(self.wait_put.is_none());
        self.wait_put = Some(wait_put);
        let peer_id = self.peers.choose(&mut thread_rng()).unwrap();
        println!("Choose {peer_id} to put");
        // PUT step 1: push object
        self.swarm
            .behaviour_mut()
            .exchange
            .send_request(peer_id, FileRequest::Push(id, object));
    }

    fn get(&mut self, id: [u8; 32], wait_get: oneshot::Sender<Vec<u8>>) {
        assert!(self.wait_get.is_none());
        self.wait_get = Some(wait_get);
        // GET step 1: find providers (should only find one)
        self.swarm
            .behaviour_mut()
            .kademlia
            .get_providers(Key::new(&id));
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Mdns(mdns::Event::Discovered(list)) => {
                for (peer, addr) in list {
                    println!("Discover peer: {peer}");
                    self.peers.push(peer);
                    self.swarm.behaviour_mut().kademlia.add_address(&peer, addr);
                }
            }
            Event::Mdns(mdns::Event::Expired(list)) => {
                for (peer, addr) in list {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .remove_address(&peer, &addr);
                }
            }
            Event::Kademlia(KademliaEvent::OutboundQueryProgressed {
                id,
                result: QueryResult::StartProviding(Ok(_)),
                ..
            }) => {
                // PUT step 3: response pushing peer
                let channel = self.push_peers.remove(&id).unwrap();
                self.swarm
                    .behaviour_mut()
                    .exchange
                    .send_response(channel, FileResponse::PushOk)
                    .unwrap();
            }
            Event::Kademlia(KademliaEvent::OutboundQueryProgressed {
                result:
                    QueryResult::GetProviders(Ok(GetProvidersOk::FoundProviders { key, providers })),
                ..
            }) if !self.is_pulling => {
                assert!(self.wait_get.is_some());
                // GET step 2: pull object
                self.swarm.behaviour_mut().exchange.send_request(
                    &providers.into_iter().next().unwrap(),
                    FileRequest::Pull(key.to_vec().try_into().unwrap()),
                );
                self.is_pulling = true;
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Request {
                        request: FileRequest::Push(id, object),
                        channel,
                        ..
                    },
                ..
            }) => {
                // PUT step 2: insert object and publish provider record
                self.objects = Some((id, object));
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .start_providing(Key::new(&id))
                    .unwrap();
                self.push_peers.insert(query_id, channel);
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Request {
                        request: FileRequest::Pull(id),
                        channel,
                        ..
                    },
                ..
            }) => {
                // GET step 3: response pulling
                if let Some((object_id, object)) = &self.objects {
                    if id == *object_id {
                        self.swarm
                            .behaviour_mut()
                            .exchange
                            .send_response(channel, FileResponse::PullOk(object.clone()))
                            .unwrap()
                    }
                }
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Response {
                        response: FileResponse::PushOk,
                        ..
                    },
                ..
            }) => {
                // PUT step 4: done
                self.wait_put.take().unwrap().send(()).unwrap()
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Response {
                        response: FileResponse::PullOk(object),
                        ..
                    },
                ..
            }) => {
                // Get step 4: done
                self.wait_get.take().unwrap().send(object).unwrap()
            }
            event => println!("Behaviour event: {event:?}"),
        }
    }
}

#[derive(Debug)]
enum Command {
    Put([u8; 32], Vec<u8>, oneshot::Sender<()>),
    Get([u8; 32], oneshot::Sender<Vec<u8>>),
}

pub struct KadFsHandle(mpsc::Sender<Command>);
impl KadFsHandle {
    pub async fn put(&self, id: [u8; 32], object: Vec<u8>) {
        let wait_put = oneshot::channel();
        self.0
            .send(Command::Put(id, object, wait_put.0))
            .await
            .unwrap();
        wait_put.1.await.unwrap()
    }

    pub async fn get(&self, id: [u8; 32]) -> Vec<u8> {
        let wait_get = oneshot::channel();
        self.0.send(Command::Get(id, wait_get.0)).await.unwrap();
        wait_get.1.await.unwrap()
    }
}

impl KadFs {
    pub async fn run_event_loop(&mut self) {
        loop {
            select! {
                command = self.command.recv() => self.handle_command(command.unwrap()),
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::NewListenAddr { address, .. } => println!("Listen: {address:?}"),
                    SwarmEvent::Behaviour(event) => self.handle_event(event),
                    event => eprintln!("Swarm event: {event:?}"),
                }
            }
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Put(id, object, wait_put) => self.put(id, object, wait_put),
            Command::Get(id, wait_get) => self.get(id, wait_get),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FileExchangeProtocol;
#[derive(Debug, Clone, Copy)]
pub struct FileExchangeCodec;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileRequest {
    Push([u8; 32], Vec<u8>),
    Pull([u8; 32]),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileResponse {
    PushOk,
    PullOk(Vec<u8>),
}

impl ProtocolName for FileExchangeProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/exchange/1".as_bytes()
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
        if let FileRequest::Push(_, object) = &mut request {
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
            FileRequest::Push(_, object) => {
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

use std::{collections::HashMap, time::Duration};

use futures::StreamExt;
use libp2p::{
    core::upgrade::Version,
    kad::{
        record::Key, store::MemoryStore, GetProvidersOk, Kademlia, KademliaConfig, KademliaEvent,
        QueryId, QueryResult,
    },
    mplex::MplexConfig,
    multihash::{Hasher, Sha2_256},
    noise::NoiseAuthenticated,
    request_response::{
        ProtocolSupport, RequestResponse, RequestResponseConfig, RequestResponseEvent,
        RequestResponseMessage, ResponseChannel,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, Multiaddr, PeerId, Swarm, Transport,
};
use rand::{seq::SliceRandom, thread_rng};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::spawn_blocking,
};
use tracing::{info, info_span};

use crate::peer::{
    addr_to_keypair, Command, FileExchangeCodec, FileExchangeProtocol, FileRequest, FileResponse,
    PeerHandle,
};

#[derive(NetworkBehaviour)]
pub struct KadBehaviour {
    kademlia: Kademlia<MemoryStore>,
    exchange: RequestResponse<FileExchangeCodec>,
    // mdns: mdns::tokio::Behaviour,
}
type Event = <KadBehaviour as NetworkBehaviour>::OutEvent;

pub struct KadPeer {
    swarm: Swarm<KadBehaviour>,
    #[allow(unused)]
    n_peer: usize,
    is_ready: bool,

    // active peer (client side) state
    peers: Vec<PeerId>,
    wait_put: Option<oneshot::Sender<[u8; 32]>>,
    wait_get: Option<oneshot::Sender<Vec<u8>>>,
    // whether a pull request is already sent to a provider
    is_pulling: bool,
    command: mpsc::Receiver<Command>,
    command_sender: mpsc::Sender<Command>,

    // passive peer state
    // 1-capacity store, equivalent to a real store with strict eviction rule
    // there's no concurrent storing, so there's no false positive eviction
    objects: Option<([u8; 32], Vec<u8>)>,
    // StartProviding query => response channel
    push_peers: HashMap<QueryId, ResponseChannel<FileResponse>>,
}

impl KadPeer {
    pub fn new(n_peer: usize, addr: Multiaddr) -> Self {
        let id_keys = addr_to_keypair(&addr);
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
        let mut exchange_config = RequestResponseConfig::default();
        exchange_config.set_request_timeout(Duration::from_secs(120));
        let exchange = RequestResponse::new(
            FileExchangeCodec,
            [(FileExchangeProtocol, ProtocolSupport::Full)],
            exchange_config,
        );
        let mut swarm = Swarm::with_tokio_executor(
            transport,
            KadBehaviour {
                kademlia,
                exchange,
                // mdns: mdns::Behaviour::new(Default::default()).unwrap(),
            },
            peer_id,
        );
        swarm.listen_on(addr).unwrap();
        let (command_sender, command) = mpsc::channel(1);
        Self {
            swarm,
            n_peer,
            is_ready: false,
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

    pub fn add_peer(&mut self, addr: Multiaddr) {
        let peer_id = addr_to_keypair(&addr).public().into();
        self.swarm
            .behaviour_mut()
            .kademlia
            .add_address(&peer_id, addr);
        self.peers.push(peer_id);
    }

    pub fn handle(&self) -> PeerHandle {
        PeerHandle(self.command_sender.clone())
    }

    fn put(&mut self, object: Vec<u8>, wait_put: oneshot::Sender<[u8; 32]>) {
        assert!(self.wait_put.is_none());
        self.wait_put = Some(wait_put);
        let peer_id = self.peers.choose(&mut thread_rng()).unwrap();
        info!("Choose {peer_id} to put");
        // PUT step 1: push object
        self.swarm
            .behaviour_mut()
            .exchange
            .send_request(peer_id, FileRequest::Push(object));
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

    async fn handle_event(&mut self, event: Event) {
        match event {
            // Event::Mdns(mdns::Event::Discovered(list)) => {
            //     for (peer, addr) in list {
            //         self.peers.insert(peer);
            //         self.swarm.behaviour_mut().kademlia.add_address(&peer, addr);
            //     }
            //     if !self.is_ready && self.peers.len() >= self.n_peer - 1 {
            //         println!("Ready");
            //         self.is_ready = true;
            //     }
            // }
            // Event::Mdns(mdns::Event::Expired(list)) => {
            //     for (peer, addr) in list {
            //         self.swarm
            //             .behaviour_mut()
            //             .kademlia
            //             .remove_address(&peer, &addr);
            //     }
            // }
            Event::Kademlia(KademliaEvent::RoutingUpdated { .. }) => {}
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
                    .send_response(
                        channel,
                        FileResponse::PushOk(self.objects.as_ref().unwrap().0),
                    )
                    .unwrap();
            }
            Event::Kademlia(KademliaEvent::OutboundQueryProgressed {
                result:
                    QueryResult::GetProviders(Ok(GetProvidersOk::FoundProviders { key, providers })),
                ..
            }) if !self.is_pulling => {
                assert!(self.wait_get.is_some());
                if providers.is_empty() {
                    // println!("! Provider set is empty");
                    return;
                }
                // GET step 2: pull object
                self.swarm.behaviour_mut().exchange.send_request(
                    &providers.into_iter().next().unwrap(),
                    FileRequest::Pull(key.to_vec().try_into().unwrap()),
                );
                self.is_pulling = true;
            }
            Event::Kademlia(KademliaEvent::OutboundQueryProgressed {
                result:
                    QueryResult::GetProviders(Ok(GetProvidersOk::FinishedWithNoAdditionalRecord {
                        ..
                    })),
                ..
            }) => {
                assert!(self.is_pulling)
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Request {
                        request: FileRequest::Push(object),
                        channel,
                        ..
                    },
                ..
            }) => {
                // PUT step 2: insert object and publish provider record
                let (id, object) = spawn_blocking(move || {
                    let _span = info_span!("Compute SHA256 for pushed object").entered();
                    let mut hasher = Sha2_256::default();
                    hasher.update(&object);
                    (hasher.finalize().try_into().unwrap(), object)
                })
                .await
                .unwrap();
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
                        response: FileResponse::PushOk(id),
                        ..
                    },
                ..
            }) => {
                // PUT step 4: done
                self.wait_put.take().unwrap().send(id).unwrap()
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
            Event::Exchange(RequestResponseEvent::OutboundFailure { .. }) => panic!(),
            event => info!("{event:?}"),
        }
    }

    pub async fn run_event_loop(&mut self) {
        loop {
            select! {
                command = self.command.recv() => self.handle_command(command.unwrap()),
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(event) => self.handle_event(event).await,
                    event => info!("{event:?}"),
                }
            }
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Put(object, wait_put) => self.put(object, wait_put),
            Command::Get(id, wait_get) => self.get(id, wait_get),
        }
    }
}

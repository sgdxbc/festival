use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    task::Poll,
};

use bincode::Options;
use futures::StreamExt;
use libp2p::{
    core::upgrade::Version,
    floodsub::{Floodsub, FloodsubEvent, FloodsubMessage, Topic},
    mplex::MplexConfig,
    multihash::{Hasher, Sha2_256},
    noise::NoiseAuthenticated,
    request_response::{
        ProtocolSupport, RequestResponse, RequestResponseEvent, RequestResponseMessage,
    },
    swarm::{dummy, NetworkBehaviour, SwarmEvent},
    tcp, Multiaddr, PeerId, Swarm, Transport,
};
use rand::{random, thread_rng, Rng};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::spawn_blocking,
};
use tracing::{info, info_span};

use crate::{
    peer::{
        addr_to_keypair, Command, FileExchangeCodec, FileExchangeProtocol, FileRequest,
        FileResponse, PeerHandle,
    },
    WirehairDecoder, WirehairEncoder,
};

#[derive(NetworkBehaviour)]
pub struct EntropyBehaviour {
    gossip: Floodsub,
    exchange: RequestResponse<FileExchangeCodec>,
    peer_address: PeerAddress,
    // mdns: mdns::tokio::Behaviour,
}
type Event = <EntropyBehaviour as NetworkBehaviour>::OutEvent;

pub struct PeerAddress(HashMap<PeerId, Multiaddr>);
impl NetworkBehaviour for PeerAddress {
    type ConnectionHandler = dummy::ConnectionHandler;
    type OutEvent = ();
    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        vec![self.0[peer_id].clone()]
    }
    fn new_handler(&mut self) -> Self::ConnectionHandler {
        dummy::ConnectionHandler
    }
    fn poll(
        &mut self,
        _cx: &mut std::task::Context<'_>,
        _params: &mut impl libp2p::swarm::PollParameters,
    ) -> Poll<libp2p::swarm::NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>> {
        Poll::Pending
    }
}

pub struct EntropyPeer {
    swarm: Swarm<EntropyBehaviour>,
    n_peer: usize,
    k: usize,
    // k_repair: usize,
    k_put: usize, // with high probability, the min number of honest peers per epoch
    k_select: usize,

    command_sender: mpsc::Sender<Command>,
    command: mpsc::Receiver<Command>,
    wait_put: Option<WaitPut>,
    wait_get: Option<WaitGet>,

    fragments: Option<([u8; 32], u32, Vec<u8>)>,
}

struct WaitPut {
    sender: oneshot::Sender<[u8; 32]>,
    id: [u8; 32],
    encoder: Arc<Mutex<WirehairEncoder>>,
    show_peers: HashSet<PeerId>,
}

struct WaitGet {
    sender: oneshot::Sender<Vec<u8>>,
    id: [u8; 32],
    decoder: Arc<Mutex<WirehairDecoder>>,
}

impl EntropyPeer {
    pub fn random_identity(n_peer: usize, k: usize, addr: Multiaddr) -> Self {
        let id_keys = addr_to_keypair(&addr);
        let peer_id = PeerId::from(id_keys.public());
        let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
            .upgrade(Version::V1)
            .authenticate(NoiseAuthenticated::xx(&id_keys).unwrap())
            .multiplex(MplexConfig::new())
            .boxed();
        let mut gossip = Floodsub::new(peer_id);
        gossip.subscribe(Topic::new("put"));
        gossip.subscribe(Topic::new("get"));
        gossip.subscribe(Topic::new("show"));
        let exchange = RequestResponse::new(
            FileExchangeCodec,
            [(FileExchangeProtocol, ProtocolSupport::Full)],
            Default::default(),
        );
        let mut swarm = Swarm::with_tokio_executor(
            transport,
            EntropyBehaviour {
                gossip,
                exchange,
                peer_address: PeerAddress(Default::default()),
                // mdns: mdns::Behaviour::new(Default::default()).unwrap(),
            },
            peer_id,
        );
        swarm.listen_on(addr).unwrap();
        let (command_sender, command) = mpsc::channel(1);
        Self {
            swarm,
            n_peer,
            k,
            k_put: k * 8 / 5,
            k_select: k * 2, //
            command,
            command_sender,
            wait_put: None,
            wait_get: None,
            fragments: Default::default(),
        }
    }

    pub fn handle(&self) -> PeerHandle {
        PeerHandle(self.command_sender.clone())
    }

    pub fn add_peer(&mut self, addr: Multiaddr) {
        let peer_id = addr_to_keypair(&addr).public().into();
        self.swarm
            .behaviour_mut()
            .peer_address
            .0
            .insert(peer_id, addr);
        if thread_rng().gen_bool(10. / self.n_peer as f64) {
            self.swarm
                .behaviour_mut()
                .gossip
                .add_node_to_partial_view(peer_id);
        }
    }

    async fn put(&mut self, object: Vec<u8>, wait_put: oneshot::Sender<[u8; 32]>) {
        assert!(self.wait_put.is_none());
        // PUT step 1: gossip PUT
        let (id, object) = spawn_blocking(move || {
            let _span = info_span!("Compute SHA256 for pushed object").entered();
            let mut hasher = Sha2_256::default();
            hasher.update(&object);
            (<[u8; 32]>::try_from(hasher.finalize()).unwrap(), object)
        })
        .await
        .unwrap();
        self.swarm
            .behaviour_mut()
            .gossip
            .publish(Topic::new("put"), id);
        println!("Published");
        self.wait_put = Some(WaitPut {
            sender: wait_put,
            id,
            encoder: Arc::new(Mutex::new(WirehairEncoder::new(
                &object,
                (object.len() / self.k) as _,
            ))),
            show_peers: Default::default(),
        });
    }

    fn get(&mut self, id: [u8; 32], wait_get: oneshot::Sender<Vec<u8>>) {
        assert!(self.wait_get.is_none());
        // GET step 1: gossip GET
        self.swarm
            .behaviour_mut()
            .gossip
            .publish(Topic::new("get"), id);
        self.wait_get = Some(WaitGet {
            sender: wait_get,
            id,
            decoder: Arc::new(Mutex::new(WirehairDecoder::new(1 << 30, (1 << 30) / 16))),
        })
    }

    async fn handle_event(&mut self, event: Event) {
        if matches!(event, Event::Gossip(FloodsubEvent::Message(_))) {
            println!("{event:?}");
        }
        match event {
            Event::Gossip(FloodsubEvent::Subscribed { .. }) => {}
            Event::Gossip(FloodsubEvent::Message(FloodsubMessage {
                source: peer,
                data,
                topics,
                ..
            })) if topics[0].id() == "put" => {
                println!("Receive gossip PUT");
                // select with probability k/N
                // first generate a random number uniformly in [0, N/k)
                // if it is inside [0, 1), further scale it to [0, u32::MAX) as final frag id
                let frag_id = random::<f32>() / (self.k_select as f32 / self.n_peer as f32);
                let frag_id = if frag_id > 1. {
                    return;
                } else {
                    (frag_id * u32::MAX as f32) as _
                };
                // PUT step 2: pull fragment
                self.swarm.behaviour_mut().exchange.send_request(
                    &peer,
                    FileRequest::PullFrag(data.try_into().unwrap(), frag_id),
                );
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Request {
                        request: FileRequest::PullFrag(id, frag_id),
                        channel,
                        ..
                    },
                ..
            }) if self.wait_put.as_ref().map(|wait_put| wait_put.id) == Some(id) => {
                // PUT step 3: response pull fragment
                let Some(wait_put) = self.wait_put.as_mut() else {
                    // already responded `k_put` peers and finialize put
                    return; // is it ok to never respond?
                };
                let mut encoder = wait_put.encoder.lock().unwrap();
                let mut frag = vec![0; encoder.block_bytes as _];
                encoder.encode(frag_id, &mut frag).unwrap();
                self.swarm
                    .behaviour_mut()
                    .exchange
                    .send_response(channel, FileResponse::PullFragOk(id, frag_id, frag))
                    .unwrap();
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Response {
                        response: FileResponse::PullFragOk(id, frag_id, frag),
                        ..
                    },
                ..
            }) => {
                // PUT step 4: gossip SHOW
                self.fragments = Some((id, frag_id, frag));
                self.swarm.behaviour_mut().gossip.publish(
                    Topic::new("show"),
                    bincode::options().serialize(&(id, frag_id)).unwrap(),
                )
            }
            Event::Gossip(FloodsubEvent::Message(FloodsubMessage {
                source: peer,
                data,
                topics,
                ..
            })) if topics[0].id() == "show" => {
                // PUT step 5: collect gossip SHOW
                let Some(wait_put) = self.wait_put.as_mut() else {
                    return; // TODO other cases that want to handle SHOW gossip
                };
                let (id, _) = bincode::options()
                    .deserialize::<([u8; 32], u32)>(&data)
                    .unwrap();
                if id != wait_put.id {
                    return;
                }
                wait_put.show_peers.insert(peer);
                if wait_put.show_peers.len() >= self.k_put {
                    let wait_put = self.wait_put.take().unwrap();
                    wait_put.sender.send(wait_put.id).unwrap()
                }
            }
            Event::Gossip(FloodsubEvent::Message(FloodsubMessage {
                source: peer,
                data,
                topics,
                ..
            })) if topics[0].id() == "get" => {
                // GET step 2: push fragment
                if let Some((id, frag_id, frag)) = self.fragments.as_ref() {
                    if <[u8; 32]>::try_from(data).unwrap() == *id {
                        self.swarm.behaviour_mut().exchange.send_request(
                            &peer,
                            FileRequest::PushFrag(*id, *frag_id, frag.clone()),
                        );
                    }
                }
            }
            Event::Exchange(RequestResponseEvent::Message {
                message:
                    RequestResponseMessage::Request {
                        request: FileRequest::PushFrag(id, frag_id, frag),
                        channel,
                        ..
                    },
                ..
            }) if self.wait_get.as_ref().map(|wait_get| wait_get.id) == Some(id) => {
                // not rery meaningful by now
                self.swarm
                    .behaviour_mut()
                    .exchange
                    .send_response(channel, FileResponse::PushFragOk)
                    .unwrap();
                // GET step 3: collect push fragment
                let Some(wait_get) = self.wait_get.as_mut() else {unreachable!()};
                let mut decoder = wait_get.decoder.lock().unwrap();
                if decoder.decode(frag_id, &frag).unwrap() {
                    let mut object = vec![0; decoder.message_bytes as _];
                    decoder.recover(&mut object).unwrap();
                    drop(decoder);
                    let wait_get = self.wait_get.take().unwrap();
                    wait_get.sender.send(object).unwrap();
                }
            }
            event => info!("{event:?}"),
        }
    }

    pub async fn run_event_loop(&mut self) {
        loop {
            select! {
                command = self.command.recv() => self.handle_command(command.unwrap()).await,
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(event) => self.handle_event(event).await,
                    event => info!("{event:?}"),
                }
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Put(object, wait_put) => self.put(object, wait_put).await,
            Command::Get(id, wait_get) => self.get(id, wait_get),
        }
    }
}

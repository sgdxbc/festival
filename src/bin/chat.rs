use futures::StreamExt;
use libp2p::{
    core::upgrade,
    floodsub::{self, Floodsub, FloodsubEvent},
    identity, mdns, mplex, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, Multiaddr, PeerId, Transport,
};
use std::error::Error;
use tokio::io::{self, AsyncBufReadExt};

/// The `tokio::main` attribute sets up a tokio runtime.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    // Create a random PeerId
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    println!("Local peer id: {peer_id:?}");

    // Create a tokio-based TCP transport use noise for authenticated
    // encryption and Mplex for multiplexing of substreams on a TCP stream.
    let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1)
        .authenticate(
            noise::NoiseAuthenticated::xx(&id_keys)
                .expect("Signing libp2p-noise static DH keypair failed."),
        )
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    // Create a Floodsub topic
    let floodsub_topic = floodsub::Topic::new("chat");

    // We create a custom  behaviour that combines floodsub and mDNS.
    // The derive generates a delegating `NetworkBehaviour` impl.
    #[derive(NetworkBehaviour)]
    struct MyBehaviour {
        floodsub: Floodsub,
        mdns: mdns::tokio::Behaviour,
    }

    // Create a Swarm to manage peers and events.
    let mdns_behaviour = mdns::Behaviour::new(Default::default())?;
    let mut behaviour = MyBehaviour {
        floodsub: Floodsub::new(peer_id),
        mdns: mdns_behaviour,
    };
    behaviour.floodsub.subscribe(floodsub_topic.clone());
    let mut swarm = libp2p::swarm::Swarm::with_tokio_executor(transport, behaviour, peer_id);

    // Reach out to another node if specified
    if let Some(to_dial) = std::env::args().nth(1) {
        let addr: Multiaddr = to_dial.parse()?;
        swarm.dial(addr)?;
        println!("Dialed {to_dial:?}");
    }

    // Read full lines from stdin
    let mut stdin = io::BufReader::new(io::stdin()).lines();

    // Listen on all interfaces and whatever port the OS assigns
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Kick it off
    loop {
        tokio::select! {
            line = stdin.next_line() => {
                let line = line?.expect("stdin closed");
                swarm.behaviour_mut().floodsub.publish(floodsub_topic.clone(), line.as_bytes());
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("Listening on {address:?}");
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Floodsub(FloodsubEvent::Message(message))) => {
                        println!(
                                "Received: '{:?}' from {:?}",
                                String::from_utf8_lossy(&message.data),
                                message.source
                            );
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(event)) => {
                        match event {
                            mdns::Event::Discovered(list) => {
                                for (peer, _) in list {
                                    swarm.behaviour_mut().floodsub.add_node_to_partial_view(peer);
                                }
                            }
                            mdns::Event::Expired(list) => {
                                for (peer, _) in list {
                                    if !swarm.behaviour().mdns.has_node(&peer) {
                                        swarm.behaviour_mut().floodsub.remove_node_from_partial_view(&peer);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

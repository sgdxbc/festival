use std::{env::args, time::Duration};

use festival::{peer::PeerHandle, EntropyPeer, KadPeer};
use libp2p::Multiaddr;
use rand::{seq::SliceRandom, thread_rng, Rng};
use tokio::{
    spawn,
    task::JoinHandle,
    time::{sleep, Instant},
};
use tracing::metadata::LevelFilter;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter, FmtSubscriber};

const HOSTS: &[&str] = &[
    // "/ip4/127.0.0.1",

    // "/ip4/172.31.9.51",
    // "/ip4/172.30.6.243",
    // "/ip4/172.29.7.238",
    // "/ip4/172.28.1.107",
    // "/ip4/172.27.3.164",
    "/ip4/172.31.19.153",
    "/ip4/172.30.15.180",
    "/ip4/172.29.7.84",
    "/ip4/172.28.10.12",
    "/ip4/172.27.5.133",
];

const ENTROPY_K: usize = 64;

#[tokio::main(worker_threads = 1)]
// #[tokio::main]
async fn main() {
    tracing_log::LogTracer::init().unwrap();
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::WARN.into())
                .from_env_lossy(),
        )
        .with_span_events(FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // args:
    // [1]: kad | entropy
    // [2]: host index
    // [3]: peer per host
    // [4]: peer index start from 1
    // [5]: [putget]
    // [6]: [object size]
    let host_i = args().nth(2).unwrap().parse::<usize>().unwrap();
    let peer_per_host = args().nth(3).unwrap().parse::<usize>().unwrap();
    let n_peer = peer_per_host * HOSTS.len();
    let peer_i = args().nth(4).unwrap().parse::<u16>().unwrap();
    let (put_port, get_port) = (
        thread_rng().gen_range(20000..20100),
        thread_rng().gen_range(30000..30100),
    );
    match (args().nth(1).as_deref(), args().nth(5).as_deref()) {
        (Some("entropy"), None) => {
            let mut peer = EntropyPeer::new(
                n_peer,
                ENTROPY_K,
                format!("{}/tcp/{}", HOSTS[host_i], peer_i + 10000)
                    .parse()
                    .unwrap(),
            );
            for host in HOSTS {
                for i in 1..=peer_per_host {
                    peer.add_pending_peer(format!("{host}/tcp/{}", 10000 + i).parse().unwrap());
                }

                for port in 20000..20100 {
                    peer.add_peer(format!("{host}/tcp/{port}").parse().unwrap(), true);
                }
                for port in 30000..30100 {
                    peer.add_peer(format!("{host}/tcp/{port}").parse().unwrap(), true);
                }
            }
            peer.run_event_loop().await
        }
        (Some("kad"), None) => {
            let mut peer = KadPeer::new(
                n_peer,
                format!("{}/tcp/{}", HOSTS[host_i], peer_i + 10000)
                    .parse()
                    .unwrap(),
            );
            let mut peers = HOSTS
                .iter()
                .flat_map(|host| {
                    (1..=peer_per_host)
                        .map(move |i| format!("{host}/tcp/{}", 10000 + i).parse().unwrap())
                })
                .collect::<Vec<Multiaddr>>();
            peers.shuffle(&mut thread_rng());
            for other_peer in peers {
                peer.add_peer(other_peer);
            }
            println!("READY");
            peer.run_event_loop().await
        }
        (Some(protocol), Some("putget")) => {
            let size = args()
                .nth(6)
                .map(|arg| arg.parse().unwrap())
                .unwrap_or(1 << 26); // 64MB
            let mut object = vec![0; size];
            thread_rng().fill(&mut object[..]);

            let (handle, peer_thread) = opertion_peer(
                protocol,
                format!("{}/tcp/{put_port}", HOSTS[host_i]).parse().unwrap(),
                n_peer,
                peer_per_host,
            );
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            let id = handle.put(object.clone()).await;
            println!("{:.2?} Put done", Instant::now() - instant);
            peer_thread.abort();

            // sleep(Duration::from_secs(5)).await;

            let (handle, peer_thread) = opertion_peer(
                protocol,
                format!("{}/tcp/{get_port}", HOSTS[host_i]).parse().unwrap(),
                n_peer,
                peer_per_host,
            );
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            let object_ = handle.get(size, id).await;
            println!("{:.2?} Get done", Instant::now() - instant);
            assert_eq!(object_, object);
            peer_thread.abort();
        }
        _ => panic!(),
    }
}

fn opertion_peer(
    protocol: &str,
    addr: Multiaddr,
    n_peer: usize,
    peer_per_host: usize,
) -> (PeerHandle, JoinHandle<()>) {
    match protocol {
        "kad" => {
            let mut peer = KadPeer::new(n_peer, addr);
            let mut peers = HOSTS
                .iter()
                .flat_map(|host| {
                    (1..=peer_per_host)
                        .map(move |i| format!("{host}/tcp/{}", 10000 + i).parse().unwrap())
                })
                .collect::<Vec<Multiaddr>>();
            peers.shuffle(&mut thread_rng());
            for other_peer in peers {
                peer.add_peer(other_peer);
            }
            (
                peer.handle(),
                spawn(async move { peer.run_event_loop().await }),
            )
        }
        "entropy" => {
            let mut peer = EntropyPeer::new(n_peer, ENTROPY_K, addr);
            peer.subscribe_topics();
            for host in HOSTS {
                for i in 1..=peer_per_host {
                    peer.add_peer(format!("{host}/tcp/{}", 10000 + i).parse().unwrap(), false);
                }
            }
            (
                peer.handle(),
                spawn(async move { peer.run_event_loop().await }),
            )
        }
        _ => unreachable!(),
    }
}

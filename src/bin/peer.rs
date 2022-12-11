use std::{env::args, time::Duration};

use festival::{peer::PeerHandle, EntropyPeer, KadPeer};
use libp2p::Multiaddr;
use rand::{thread_rng, Rng};
use tokio::{
    spawn,
    task::JoinHandle,
    time::{sleep, Instant},
};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter, FmtSubscriber};

const HOSTS: &[&str] = &[
    "/ip4/127.0.0.1",
    // "/ip4/172.31.9.51",
    // "/ip4/172.30.6.243",
    // "/ip4/172.29.7.238",
    // "/ip4/172.28.1.107",
    // "/ip4/172.27.3.164",
];

#[tokio::main(worker_threads = 1)]
async fn main() {
    tracing_log::LogTracer::init().unwrap();
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
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
    match (args().nth(1).as_deref(), args().nth(5).as_deref()) {
        (Some("entropy"), None) => {
            let mut peer = EntropyPeer::random_identity(
                n_peer,
                16,
                format!("{}/tcp/{}", HOSTS[host_i], peer_i + 10000)
                    .parse()
                    .unwrap(),
            );
            for host in HOSTS {
                for i in 1..=peer_per_host {
                    peer.add_peer(format!("{host}/tcp/{}", 10000 + i).parse().unwrap());
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
            for host in HOSTS {
                for i in 1..=peer_per_host {
                    peer.add_peer(format!("{host}/tcp/{}", 10000 + i).parse().unwrap());
                }
            }
            peer.run_event_loop().await
        }
        (Some(protocol), Some("putget")) => {
            let mut object = vec![
                0;
                args()
                    .nth(6)
                    .map(|arg| arg.parse().unwrap())
                    .unwrap_or(1 << 30)
            ];
            thread_rng().fill(&mut object[..]);

            let (handle, peer_thread) = opertion_peer(
                protocol,
                format!("{}/tcp/10000", HOSTS[host_i]).parse().unwrap(),
                n_peer,
                peer_per_host,
            );
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            let id = handle.put(object.clone()).await;
            println!("{:.2?} Put done", Instant::now() - instant);
            peer_thread.abort();

            let (handle, peer_thread) = opertion_peer(
                protocol,
                format!("{}/tcp/20000", HOSTS[host_i]).parse().unwrap(),
                n_peer,
                peer_per_host,
            );
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            let object_ = handle.get(id).await;
            println!("{:.2?} Get done", Instant::now() - instant);
            peer_thread.abort();
            assert_eq!(object_, object);
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
            for host in HOSTS {
                for i in 1..=peer_per_host {
                    peer.add_peer(format!("{host}/tcp/{}", 10000 + i).parse().unwrap());
                }
            }
            (
                peer.handle(),
                spawn(async move { peer.run_event_loop().await }),
            )
        }
        "entropy" => {
            let mut peer = EntropyPeer::random_identity(n_peer, 16, addr);
            for host in HOSTS {
                for i in 1..=peer_per_host {
                    peer.add_peer(format!("{host}/tcp/{}", 10000 + i).parse().unwrap());
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

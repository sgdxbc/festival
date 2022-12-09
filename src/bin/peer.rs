use std::{env::args, time::Duration};

use festival::{peer::PeerHandle, EntropyPeer, KadPeer};
use rand::{thread_rng, Rng};
use tokio::{
    spawn,
    task::JoinHandle,
    time::{sleep, Instant},
};
use tracing::{info, Level};
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

#[tokio::main]
async fn main() {
    tracing_log::LogTracer::init().unwrap();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_span_events(FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    match (args().nth(1).as_deref(), args().nth(2).as_deref()) {
        (Some("entropy"), None) => {
            EntropyPeer::random_identity(1000, 16)
                .run_event_loop()
                .await
        }
        (Some("kad"), None) => KadPeer::random_identity().run_event_loop().await,
        (Some(protocol), Some("putget")) => {
            let mut object = vec![
                0;
                args()
                    .nth(3)
                    .map(|arg| arg.parse().unwrap())
                    .unwrap_or(1 << 30)
            ];
            thread_rng().fill(&mut object[..]);

            let (handle, peer_thread) = opertion_peer(protocol);
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            let id = handle.put(object.clone()).await;
            info!("{:.2?} Put done", Instant::now() - instant);
            peer_thread.abort();

            let (handle, peer_thread) = opertion_peer(protocol);
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            let object_ = handle.get(id).await;
            info!("{:.2?} Get done", Instant::now() - instant);
            peer_thread.abort();
            assert_eq!(object_, object);
        }
        _ => panic!(),
    }
}

fn opertion_peer(protocol: &str) -> (PeerHandle, JoinHandle<()>) {
    match protocol {
        "kad" => {
            let mut peer = KadPeer::random_identity();
            (
                peer.handle(),
                spawn(async move { peer.run_event_loop().await }),
            )
        }
        "entropy" => {
            let mut peer = EntropyPeer::random_identity(1000, 16);
            (
                peer.handle(),
                spawn(async move { peer.run_event_loop().await }),
            )
        }
        _ => unreachable!(),
    }
}

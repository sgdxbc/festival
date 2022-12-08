use std::{env::args, time::Duration};

use festival::kad::KadFs;
use rand::{thread_rng, Rng};
use tokio::{
    spawn,
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

    match args().nth(1).as_deref() {
        None => KadFs::new().run_event_loop().await,
        Some("putget") => {
            let mut object = vec![0; 1 << 30];
            thread_rng().fill(&mut object[..]);

            let mut peer = KadFs::new();
            let handle = peer.handle();
            let peer_thread = spawn(async move { peer.run_event_loop().await });
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            let id = handle.put(object.clone()).await;
            info!("{:.2?} Put done", Instant::now() - instant);
            peer_thread.abort();

            let mut peer = KadFs::new();
            let handle = peer.handle();
            let peer_thread = spawn(async move { peer.run_event_loop().await });
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

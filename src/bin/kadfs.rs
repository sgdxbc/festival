use std::{env::args, time::Duration};

use festival::kad::KadFs;
use rand::{thread_rng, Rng};
use tokio::{
    spawn,
    time::{sleep, Instant},
};

#[tokio::main]
async fn main() {
    let mut peer = KadFs::new();
    let handle = peer.handle();
    let peer_thread = spawn(async move { peer.run_event_loop().await });
    match args().nth(1).as_deref() {
        Some("putget") => {
            let mut object = vec![0; 1 << 30];
            thread_rng().fill(&mut object[..]);
            let id = thread_rng().gen();
            sleep(Duration::from_secs(1)).await;
            let instant = Instant::now();
            println!("{:.2?} Start put", Instant::now() - instant);
            handle.put(id, object.clone()).await;
            println!("{:.2?} Put done", Instant::now() - instant);
            let object_ = handle.get(id).await;
            println!("{:.2?} Get done", Instant::now() - instant);
            assert_eq!(object_, object);
        }
        None => peer_thread.await.unwrap(),
        _ => panic!(),
    }
}

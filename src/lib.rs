pub mod wirehair;

pub use wirehair::{WirehairDecoder, WirehairEncoder, WirehairResult};

use std::{cmp::Reverse, collections::BinaryHeap};

use rand::Rng;
use rand_distr::{Distribution, Poisson};
use rustc_hash::FxHashMap;

pub struct System<R> {
    now_sec: u32,
    peers: FxHashMap<[u8; 32], Peer>,
    events: BinaryHeap<(Reverse<u32>, Event)>,
    rng: R,

    config: SystemConfig,
    pub stats: SystemStats,
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub n_peer: u32,
    pub failure_rate: f32,
}

#[derive(Debug, Default)]
pub struct SystemStats {
    n_failure: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    PeerFailure([u8; 32]),
}

impl<R: Rng> System<R> {
    pub fn new(rng: R, config: SystemConfig) -> Self {
        let mut system = Self {
            now_sec: 0,
            peers: Default::default(),
            events: Default::default(),
            rng,
            config,
            stats: Default::default(),
        };
        for _ in 0..system.config.n_peer {
            system.insert_peer();
        }
        system
    }

    pub fn run(&mut self, until_sec: u32) {
        let mut event;
        while {
            let event_sec;
            (Reverse(event_sec), event) = self.events.pop().unwrap();
            assert!(event_sec >= self.now_sec);
            self.now_sec = event_sec;
            self.now_sec <= until_sec
        } {
            match event {
                Event::PeerFailure(peer_id) => {
                    self.stats.n_failure += 1;
                    let _peer = self.peers.remove(&peer_id).unwrap();
                    // peer on failure
                    self.insert_peer();
                }
            }
        }
    }

    fn insert_peer(&mut self) {
        let peer_id = self.rng.gen();
        let present = self.peers.insert(peer_id, Peer {});
        assert!(present.is_none());
        self.events.push((
            Reverse(
                self.now_sec
                    + Poisson::new(self.config.failure_rate)
                        .unwrap()
                        .sample(&mut self.rng) as u32,
            ),
            Event::PeerFailure(peer_id),
        ))
    }
}

struct Peer {
    //
}

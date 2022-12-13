use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use rand::{rngs::SmallRng, seq::IteratorRandom, Rng, SeedableRng};
use rand_distr::{Distribution, Poisson};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

pub struct System<R> {
    oracle: SystemOracle,
    peers: FxHashMap<[u8; 32], Peer>,
    object_peers: FxHashMap<[u8; 32], FxHashSet<[u8; 32]>>,
    rng: R,
    config: SystemConfig,
    pub stats: SystemStats,
}

struct SystemOracle {
    now_sec: u64,
    events: BinaryHeap<(Reverse<u64>, Event)>,
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub n_peer: usize,
    pub failure_rate: f32,
    pub faulty_rate: f32,
    pub n_object: usize,
    pub protocol: ProtocolConfig,
}

#[derive(Debug, Clone)]
pub enum ProtocolConfig {
    Festival {
        k_select: usize,
        k: usize,
        k_repair: usize,
        cache_hit_rate: f32,
        // check_celebration_sec: u32,
        // gossip_sec: u32,
    },
    Replicated {
        n: usize,
    },
}

#[derive(Debug, Default)]
pub struct SystemStats {
    pub n_failure: u64,
    pub n_repair: f32,
    pub n_store: f32,
    pub n_lost: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    PeerFailure,
    Checkpoint,
    // festival
    // GossipFragment {
    //     peer_id: [u8; 32],
    //     object_id: [u8; 32],
    //     age: u32,
    // },
    // CheckCelebration {
    //     peer_id: [u8; 32],
    //     object_id: [u8; 32],
    //     age: u32,
    // },
}

#[derive(Default)]
struct Peer {
    is_faulty: bool,
    fragments: FxHashMap<[u8; 32], u64>,
    // responsible_objects: FxHashMap<[u8; 32], FxHashMap<[u8; 32], u32>>,
}

impl SystemOracle {
    fn push_event(&mut self, after_sec: u64, event: Event) {
        self.events.push((Reverse(self.now_sec + after_sec), event));
    }
}

impl<R: Rng> System<R> {
    pub fn new(rng: R, config: SystemConfig) -> Self {
        let mut system = Self {
            oracle: SystemOracle {
                now_sec: 0,
                events: Default::default(),
            },
            peers: Default::default(),
            object_peers: Default::default(),
            rng,
            config,
            stats: Default::default(),
        };
        for _ in 0..system.config.n_peer {
            system.insert_peer();
        }
        for _ in 0..system.config.n_object {
            let id = system.rng.gen();
            system.insert_object(id);
        }
        system.oracle.push_event(1, Event::PeerFailure);
        // system.oracle.push_event(86400, Event::Checkpoint);
        system
    }

    pub fn run(&mut self, until_sec: u64) {
        let mut instant = Instant::now();
        let mut event;
        while {
            let event_sec;
            (Reverse(event_sec), event) = self.oracle.events.pop().unwrap();
            assert!(event_sec >= self.oracle.now_sec);
            self.oracle.now_sec = event_sec;
            self.oracle.now_sec <= until_sec
        } {
            match event {
                Event::PeerFailure => self.on_peer_failure(),
                Event::Checkpoint => {
                    println!(
                        "entropyF,{},{},{}",
                        self.oracle.now_sec, self.stats.n_repair, self.stats.n_store
                    );
                    self.oracle.push_event(86400, Event::Checkpoint);
                }
            }

            let now = Instant::now();
            if now - instant >= Duration::from_secs(1) {
                eprint!(
                    "{:.2}% {:?}{:16}\r",
                    self.oracle.now_sec as f32 / until_sec as f32 * 100.,
                    self.stats,
                    ""
                );
                instant = now;
            }
        }
        eprintln!("Done{:100}", "");
    }

    fn on_peer_failure(&mut self) {
        self.stats.n_failure += 1;
        let peer_id = *self.peers.keys().choose(&mut self.rng).unwrap();
        let peer = self.peers.remove(&peer_id).unwrap();
        match self.config.protocol {
            ProtocolConfig::Festival {
                k,
                k_repair,
                cache_hit_rate,
                ..
            } => {
                assert!(!peer.is_faulty || peer.fragments.is_empty());
                self.stats.n_store -= peer.fragments.len() as f32 / k as f32;

                for (object_id, age) in peer.fragments {
                    if !self.object_peers.contains_key(&object_id) {
                        // this object already lost
                        continue;
                    }

                    let removed = self
                        .object_peers
                        .get_mut(&object_id)
                        .unwrap()
                        .remove(&peer_id);
                    assert!(removed);
                    // assert!(self.object_peers[&object_id].len() >= k);
                    if self.object_peers[&object_id].len() < k {
                        self.stats.n_failure += 1;
                        self.object_peers.remove(&object_id);
                        continue;
                    }

                    if self.object_peers[&object_id].len() < k_repair {
                        for (&peer_id, peer) in &mut self.peers {
                            if peer.is_faulty {
                                continue;
                            }
                            if self.object_peers[&object_id].contains(&peer_id) {
                                continue;
                            }
                            if Self::check_responsible(peer_id, object_id, age + 1, &self.config)
                                .is_some()
                            {
                                peer.fragments.insert(object_id, age + 1);
                                self.object_peers
                                    .get_mut(&object_id)
                                    .unwrap()
                                    .insert(peer_id);
                                self.stats.n_repair += if self.rng.gen_bool(cache_hit_rate as _) {
                                    1. / k as f32
                                } else {
                                    1.
                                };
                                self.stats.n_store += 1. / k as f32;
                            }
                        }
                    }
                }
            }
            ProtocolConfig::Replicated { .. } => {
                self.stats.n_store -= peer.fragments.len() as f32;

                for object_id in peer.fragments.keys() {
                    if !self.object_peers.contains_key(object_id) {
                        continue;
                    }

                    let removed = self
                        .object_peers
                        .get_mut(object_id)
                        .unwrap()
                        .remove(&peer_id);
                    assert!(removed);

                    // choose next peer here bring an issue that the replacing
                    // peer will never be selected
                    // however, that may be what we exactly want, since here we
                    // have a failure
                    let mut peer_id;
                    while {
                        peer_id = *self.peers.keys().choose(&mut self.rng).unwrap();
                        self.object_peers[object_id].contains(&peer_id)
                    } {}
                    if peer.is_faulty {
                        continue;
                    }

                    self.object_peers
                        .get_mut(object_id)
                        .unwrap()
                        .insert(peer_id);
                    self.peers
                        .get_mut(&peer_id)
                        .unwrap()
                        .fragments
                        .insert(*object_id, 0);
                    self.stats.n_repair += 1.;
                    self.stats.n_store += 1.;
                }
            }
        }

        self.insert_peer();
        self.oracle.push_event(
            Poisson::new(365. * 86400. / self.config.failure_rate / self.config.n_peer as f32)
                .unwrap()
                .sample(&mut self.rng) as _,
            Event::PeerFailure,
        );
    }

    fn insert_peer(&mut self) {
        let peer_id = self.rng.gen();
        let present = self.peers.insert(
            peer_id,
            Peer {
                is_faulty: self.rng.gen_bool(self.config.faulty_rate as _),
                ..Default::default()
            },
        );
        assert!(present.is_none());
    }

    fn check_responsible(
        peer_id: [u8; 32],
        object_id: [u8; 32],
        age: u64,
        config: &SystemConfig,
    ) -> Option<u32> {
        let mut hasher = FxHasher::default();
        (peer_id, object_id, age).hash(&mut hasher);
        let mut rng = SmallRng::seed_from_u64(hasher.finish());
        let ProtocolConfig::Festival {k_select: n_peer_per_age, ..} = config.protocol else {
            unreachable!()
        };
        if rng.gen_ratio(n_peer_per_age as _, config.n_peer as _) {
            Some(rng.gen())
        } else {
            None
        }
    }

    fn insert_object(&mut self, object_id: [u8; 32]) {
        match self.config.protocol {
            ProtocolConfig::Festival { k, .. } => {
                let mut peers = FxHashSet::default();
                self.peers
                    .iter_mut()
                    .filter(|(&peer_id, peer)| {
                        !peer.is_faulty
                            && Self::check_responsible(peer_id, object_id, 1, &self.config)
                                .is_some()
                    })
                    .for_each(|(&peer_id, peer)| {
                        peer.fragments.insert(object_id, 1);
                        peers.insert(peer_id);
                    });

                self.stats.n_store += peers.len() as f32 / k as f32;
                if peers.len() < k {
                    self.stats.n_lost += 1;
                } else {
                    self.object_peers.insert(object_id, peers);
                }
            }
            ProtocolConfig::Replicated { n, .. } => {
                let peers = self
                    .peers
                    .keys()
                    .cloned()
                    .choose_multiple(&mut self.rng, n)
                    .into_iter()
                    .filter(|peer| !self.peers[peer].is_faulty)
                    .collect::<FxHashSet<_>>();
                if peers.is_empty() {
                    self.stats.n_lost += 1;
                } else {
                    self.stats.n_store += peers.len() as f32;
                    self.object_peers.insert(object_id, peers);
                    for peer_id in &self.object_peers[&object_id] {
                        let peer = self.peers.get_mut(peer_id).unwrap();
                        peer.fragments.insert(object_id, 0);
                    }
                }
            }
        }
    }
}

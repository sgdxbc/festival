pub mod wirehair;

pub use wirehair::{WirehairDecoder, WirehairEncoder, WirehairResult};

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    hash::{Hash, Hasher},
};

use rand::{rngs::SmallRng, seq::IteratorRandom, Rng, SeedableRng};
use rand_distr::{Distribution, Poisson};
use rustc_hash::{FxHashMap, FxHasher};

pub struct System<R> {
    oracle: SystemOracle,
    peers: FxHashMap<[u8; 32], Peer>,
    rng: R,
    config: SystemConfig,
    pub stats: SystemStats,
}

struct SystemOracle {
    now_sec: u32,
    events: BinaryHeap<(Reverse<u32>, Event)>,
}

#[derive(Debug, Clone)]
pub struct SystemConfig {
    pub n_peer: usize,
    pub failure_rate: f32,
    pub n_object: usize,
    pub protocol: ProtocolConfig,
}

#[derive(Debug, Clone)]
pub enum ProtocolConfig {
    Festival {
        n_peer_per_age: usize,
        k: usize,
        k_repair: usize,
        check_celebration_sec: u32,
        gossip_sec: u32,
    },
    Kedemlia {},
}

#[derive(Debug, Default)]
pub struct SystemStats {
    n_failure: u32,
    n_gossip: u32,
    n_repair: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    PeerFailure,
    GossipFragment {
        peer_id: [u8; 32],
        object_id: [u8; 32],
        age: u32,
    },
    CheckCelebration {
        peer_id: [u8; 32],
        object_id: [u8; 32],
        age: u32,
    },
}

#[derive(Default)]
struct Peer {
    fragments: FxHashMap<[u8; 32], ()>,
    eligible_objects: FxHashMap<[u8; 32], FxHashMap<[u8; 32], u32>>,
}

// enum Fragment {
//     Festival { age: u32 },
//     // Kedemlia
// }

impl SystemOracle {
    fn push_event(&mut self, after_sec: u32, event: Event) {
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
        system
    }

    pub fn run(&mut self, until_sec: u32) {
        let mut event;
        while {
            let event_sec;
            (Reverse(event_sec), event) = self.oracle.events.pop().unwrap();
            assert!(event_sec >= self.oracle.now_sec);
            self.oracle.now_sec = event_sec;
            self.oracle.now_sec <= until_sec
        } {
            match event {
                Event::PeerFailure => {
                    self.stats.n_failure += 1;
                    let peer_id = *self.peers.keys().choose(&mut self.rng).unwrap();
                    let _peer = self.peers.remove(&peer_id).unwrap();
                    // peer on failure
                    self.insert_peer();
                    self.oracle.push_event(
                        Poisson::new(self.config.failure_rate as f32 / self.config.n_peer as f32)
                            .unwrap()
                            .sample(&mut self.rng) as _,
                        Event::PeerFailure,
                    );
                }
                Event::GossipFragment {
                    peer_id,
                    object_id,
                    age,
                } => {
                    let ProtocolConfig::Festival {check_celebration_sec, gossip_sec, ..} = self.config.protocol else {
                                unreachable!()
                            };

                    if !self.peers.contains_key(&peer_id) {
                        continue;
                    }
                    self.stats.n_gossip += 1;
                    for (&peer2_id, peer2) in &mut self.peers {
                        if peer2_id == peer_id {
                            continue;
                        }
                        if peer2.fragments.contains_key(&object_id) {
                            continue; // TODO garbage collection
                        }
                        if let Some(alive_peers) = peer2.eligible_objects.get_mut(&object_id) {
                            alive_peers.insert(peer_id, age);
                        } else if let Some(_) =
                            Self::eligible(peer2_id, object_id, age + 1, &self.config)
                        {
                            // println!(
                            //     "{:02x?} {:8} start tracking for age {}",
                            //     &peer2_id[..4],
                            //     self.oracle.now_sec,
                            //     age + 1
                            // );
                            peer2
                                .eligible_objects
                                .insert(object_id, [(peer_id, age)].into_iter().collect());
                            self.oracle.push_event(
                                check_celebration_sec,
                                Event::CheckCelebration {
                                    peer_id: peer2_id,
                                    object_id,
                                    age: age + 1,
                                },
                            )
                        }
                    }
                    self.oracle.push_event(
                        gossip_sec,
                        Event::GossipFragment {
                            peer_id,
                            object_id,
                            age,
                        },
                    );
                }
                Event::CheckCelebration {
                    peer_id,
                    object_id,
                    age,
                } => {
                    let ProtocolConfig::Festival {k, k_repair, check_celebration_sec, ..} = self.config.protocol else {
                        unreachable!()
                    };
                    let Some(peer) = self.peers.get_mut(&peer_id) else {
                        continue;
                    };
                    let alive_peers = peer.eligible_objects.remove(&object_id).unwrap();
                    let local_alive_len = alive_peers.len();
                    let mut global_alive_len = 0;
                    let mut later_len = 0;
                    for (peer_id, peer_age) in alive_peers {
                        if self.peers.contains_key(&peer_id) {
                            global_alive_len += 1;
                        }
                        if peer_age >= age {
                            later_len += 1;
                        }
                    }
                    // println!(
                    //     "{:8} check local {local_alive_len} global {global_alive_len}",
                    //     self.oracle.now_sec
                    // );
                    assert!(global_alive_len >= k, "object lost");
                    let Some(peer) = self.peers.get_mut(&peer_id) else {
                        unreachable!();
                    };
                    if local_alive_len >= k_repair {
                        if later_len < k_repair {
                            peer.eligible_objects.insert(object_id, Default::default());
                            self.oracle.push_event(
                                check_celebration_sec,
                                Event::CheckCelebration {
                                    peer_id,
                                    object_id,
                                    age,
                                },
                            );
                        }
                    } else {
                        // start repairing
                        // println!(
                        //     "{:02x?} {:8} repair into age {age}",
                        //     &peer_id[..4],
                        //     self.oracle.now_sec
                        // );
                        // it does not matter who to fetch fragments, as long as
                        // we can find enough peers (which is asserted above),
                        // and correctly record traffic overhead
                        // the total amount of fetching equals to the size of
                        // original data object (strictly speaking it should be
                        // slightly larger, but wirehair works well enough to
                        //  ignore that)
                        self.stats.n_repair += 1;
                        // peer.fragments.insert(object_id, Fragment::Festival { age });
                        peer.fragments.insert(object_id, ());
                        self.oracle.push_event(
                            1,
                            Event::GossipFragment {
                                peer_id,
                                object_id,
                                age,
                            },
                        );
                    }
                }
            }
        }
    }

    fn insert_peer(&mut self) {
        let peer_id = self.rng.gen();
        let present = self.peers.insert(peer_id, Peer::default());
        assert!(present.is_none());
    }

    fn eligible(
        peer_id: [u8; 32],
        object_id: [u8; 32],
        age: u32,
        config: &SystemConfig,
    ) -> Option<u32> {
        let mut hasher = FxHasher::default();
        (peer_id, object_id, age).hash(&mut hasher);
        let mut rng = SmallRng::seed_from_u64(hasher.finish());
        let ProtocolConfig::Festival {n_peer_per_age, ..} = config.protocol else {
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
            ProtocolConfig::Festival {
                k_repair,
                gossip_sec,
                ..
            } => {
                // this is probably not the `n_repair` peers that suppose to
                // store the fragments for age = 1
                // however as soon as age moves on things should get correct
                self.peers
                    .iter_mut()
                    .choose_multiple(&mut self.rng, k_repair)
                    .into_iter()
                    .for_each(|(&peer_id, peer)| {
                        // peer.fragments
                        //     .insert(object_id, Fragment::Festival { age: 1 });
                        peer.fragments.insert(object_id, ());
                        self.oracle.push_event(
                            (0..gossip_sec).choose(&mut self.rng).unwrap(),
                            Event::GossipFragment {
                                peer_id,
                                object_id,
                                age: 1,
                            },
                        )
                    });
            }
            ProtocolConfig::Kedemlia { .. } => todo!(),
        }
    }
}

use std::{
    cmp::Reverse,
    collections::{hash_map::Entry::Occupied, BinaryHeap},
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use rand::{rngs::SmallRng, seq::IteratorRandom, Rng, SeedableRng};
use rand_distr::{Distribution, Poisson};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};

pub struct System<R> {
    oracle: SystemOracle,
    peers: FxHashMap<[u8; 32], Peer>,
    peer_buckets: FxHashMap<Box<[u8]>, FxHashSet<[u8; 32]>>,
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
    Kademlia {
        n: usize,
        republish_sec: u32,
    },
}

#[derive(Debug, Default)]
pub struct SystemStats {
    n_failure: u32,
    n_gossip: u32,
    n_repair: u32,
    n_store: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    PeerFailure,

    // festival
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

    // kademlia
    Republish([u8; 32]),
}

#[derive(Default)]
struct Peer {
    fragments: FxHashMap<[u8; 32], ()>,
    eligible_objects: FxHashMap<[u8; 32], FxHashMap<[u8; 32], u32>>,
}

// enum Fragment {
//     Festival { age: u32 },
//     // Kademlia
// }

impl SystemOracle {
    fn push_event(&mut self, after_sec: u32, event: Event) {
        self.events.push((Reverse(self.now_sec + after_sec), event));
    }
}

impl<R: Rng> System<R> {
    const BUCKET_KEY_LEN: usize = 1;
    pub fn new(rng: R, config: SystemConfig) -> Self {
        let mut system = Self {
            oracle: SystemOracle {
                now_sec: 0,
                events: Default::default(),
            },
            peers: Default::default(),
            peer_buckets: Default::default(),
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
                Event::GossipFragment {
                    peer_id,
                    object_id,
                    age,
                } => self.on_gossip_fragment(peer_id, object_id, age),
                Event::CheckCelebration {
                    peer_id,
                    object_id,
                    age,
                } => self.on_check_celebration(peer_id, object_id, age),
                Event::Republish(object_id) => self.on_republish(object_id),
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
            ProtocolConfig::Festival { k, .. } => {
                self.stats.n_store -= peer.fragments.len() as f32 / k as f32
            }
            ProtocolConfig::Kademlia { .. } => self.stats.n_store -= peer.fragments.len() as f32,
        }
        self.peer_buckets
            .get_mut(&peer_id[..Self::BUCKET_KEY_LEN])
            .unwrap()
            .remove(&peer_id);
        self.insert_peer();
        self.oracle.push_event(
            Poisson::new(365. * 86400. / self.config.failure_rate / self.config.n_peer as f32)
                .unwrap()
                .sample(&mut self.rng) as _,
            Event::PeerFailure,
        );
    }

    fn on_gossip_fragment(&mut self, peer_id: [u8; 32], object_id: [u8; 32], age: u32) {
        let ProtocolConfig::Festival {check_celebration_sec, gossip_sec, ..} = self.config.protocol else {
                                unreachable!()
                            };

        if !self.peers.contains_key(&peer_id) {
            return;
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
            } else if let Some(_i) = Self::eligible(peer2_id, object_id, age + 1, &self.config) {
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
                    (gossip_sec..check_celebration_sec)
                        .choose(&mut self.rng)
                        .unwrap(),
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

    fn on_check_celebration(&mut self, peer_id: [u8; 32], object_id: [u8; 32], age: u32) {
        let ProtocolConfig::Festival {k, k_repair, check_celebration_sec, gossip_sec, ..} = self.config.protocol else {
            unreachable!()
        };
        let Some(peer) = self.peers.get_mut(&peer_id) else {
                        return;
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
        assert!(
            global_alive_len >= k,
            "object lost: {global_alive_len} < {k}"
        );
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
            return;
        }
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
        self.stats.n_store += 1. / k as f32;
        self.oracle.push_event(
            (0..gossip_sec).choose(&mut self.rng).unwrap(),
            Event::GossipFragment {
                peer_id,
                object_id,
                age,
            },
        );
    }

    fn on_republish(&mut self, object_id: [u8; 32]) {
        let ProtocolConfig::Kademlia { republish_sec, ..} = self.config.protocol else {
            unreachable!()
        };
        let mut lost = true;
        for peer_id in self.get_closest(object_id) {
            let peer = self.peers.get_mut(&peer_id).unwrap();
            let entry = peer.fragments.entry(object_id);
            if matches!(entry, Occupied(_)) {
                lost = false;
            } else {
                entry.or_insert(());
                self.stats.n_repair += 1;
                self.stats.n_store += 1.;
            }
        }
        assert!(!lost, "object lost");
        self.oracle
            .push_event(republish_sec, Event::Republish(object_id));
    }

    fn insert_peer(&mut self) {
        let peer_id = self.rng.gen();
        let present = self.peers.insert(peer_id, Peer::default());
        assert!(present.is_none());
        self.peer_buckets
            .entry(peer_id[..Self::BUCKET_KEY_LEN].into())
            .or_default()
            .insert(peer_id);
    }

    fn get_closest(&self, id: [u8; 32]) -> impl Iterator<Item = [u8; 32]> {
        let ProtocolConfig::Kademlia { n: k, .. } = self.config.protocol else {
            unreachable!()
        };
        let mut bucket = self.peer_buckets[&id[..Self::BUCKET_KEY_LEN]]
            .clone()
            .into_iter()
            .collect::<Vec<_>>();
        assert!(bucket.len() >= k); //
        bucket.sort_unstable_by_key(|id_low| {
            id.iter()
                .zip(id_low.iter())
                .map(|(&a, &b)| a ^ b)
                .collect::<Vec<_>>()
        });
        bucket.into_iter().take(k)
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
                k,
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
                self.stats.n_store += k_repair as f32 / k as f32;
            }
            ProtocolConfig::Kademlia {
                republish_sec, n, ..
            } => {
                for peer_id in self.get_closest(object_id) {
                    let peer = self.peers.get_mut(&peer_id).unwrap();
                    peer.fragments.insert(object_id, ());
                    self.oracle.push_event(
                        (0..republish_sec).choose(&mut self.rng).unwrap(),
                        Event::Republish(object_id),
                    )
                }
                self.stats.n_store += n as f32;
            }
        }
    }
}

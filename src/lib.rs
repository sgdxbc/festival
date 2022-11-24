pub mod encode;
pub mod network;

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
};

use encode::{DecodeContext, FragmentMeta};
use network::Route;
use rand::{seq::IteratorRandom, Rng};
use rustc_hash::FxHashMap;

pub type Thumbnail = [u8; 32];
pub type FragmentKey = [u8; 32];
pub type PeerAddress = [u8; 32];
pub type VirtualInstant = u32;
pub type VirtualDuration = u32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StorageObject {
    pub thumbnail: Thumbnail,
    pub size: u32,
}

pub struct Peer {
    fragments: FxHashMap<FragmentKey, FragmentMeta>,
    faulty_deadline: Option<VirtualInstant>,
}

pub struct System {
    virtual_time: VirtualInstant,
    events: BinaryHeap<(Reverse<VirtualInstant>, SystemEvent)>,
    peers: FxHashMap<PeerAddress, Peer>,
    route: Route,
    fragment_cache: FxHashMap<Thumbnail, BTreeMap<u32, PeerAddress>>,
    config: SystemConfig,
    stats: SystemStats,
}

pub struct SystemConfig {
    pub n_peer: u32,
    pub n_faulty_peer: u32,
    pub n_object: u32,
    pub object_size: u32,
    pub n_fragment: u32,
    pub age_duration: VirtualDuration,
    pub fault_switch_duration: VirtualDuration,
    pub checkpoint_duration: VirtualDuration,
}

#[derive(Default)]
struct SystemStats {
    n_recover: u32,
    n_fast_recover: u32,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            n_peer: 100000,
            n_faulty_peer: 10000,
            n_object: 1,
            object_size: 5000,
            n_fragment: 6400,
            age_duration: 86400,
            fault_switch_duration: 10 * 86400,
            checkpoint_duration: 86400,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SystemEvent {
    Checkpoint,
    Celebrate(StorageObject, u32),
    FaultSwitch, //
    ResetPeer(PeerAddress),
}

impl System {
    pub fn new(rng: &mut impl Rng, config: SystemConfig) -> Self {
        let mut peers = FxHashMap::default();
        let mut route = Route::Empty;
        for _ in 0..config.n_peer {
            let address = rng.gen();
            peers.insert(
                address,
                Peer {
                    fragments: FxHashMap::default(),
                    faulty_deadline: None,
                },
            );
            route.insert(address);
        }
        println!("Route depth {}", route.depth());
        let mut system = Self {
            virtual_time: 0,
            events: Default::default(),
            peers,
            route,
            fragment_cache: Default::default(),
            config,
            stats: Default::default(),
        };
        system.insert_event(system.config.checkpoint_duration, SystemEvent::Checkpoint);
        for _ in 0..system.config.n_object {
            let object = StorageObject {
                thumbnail: rng.gen(),
                size: system.config.object_size,
            };
            system.insert_object(&object, 0);
            system.insert_event(
                (0..system.config.age_duration).choose(rng).unwrap(),
                SystemEvent::Celebrate(object, 1),
            );
        }
        for address in system
            .peers
            .keys()
            .copied()
            .choose_multiple(rng, system.config.n_faulty_peer as _)
        {
            system.compromise(&address);
        }
        system.insert_event(
            system.config.fault_switch_duration,
            SystemEvent::FaultSwitch,
        );
        system
    }

    fn insert_event(&mut self, duration: VirtualDuration, event: SystemEvent) {
        self.events
            .push((Reverse(self.virtual_time + duration), event));
    }

    fn insert_object(&mut self, object: &StorageObject, age: u32) {
        let cache = self.fragment_cache.entry(object.thumbnail).or_default();
        for index in self.config.n_fragment * age..self.config.n_fragment * (age + 1) {
            let fragment = FragmentMeta::new(object, index);
            let key = fragment.key();
            let address = self.route.find(&key);
            let peer = self.peers.get_mut(address).unwrap();
            if peer.faulty_deadline.is_none() {
                peer.fragments.insert(key, fragment);
                cache.insert(index, *address);
            }
        }
    }

    fn check_object(&mut self, object: &StorageObject, age: u32) {
        let mut fragments = Vec::new();
        for (&index, address) in self.fragment_cache[&object.thumbnail]
            .range(self.config.n_fragment * age..self.config.n_fragment * (age + 1))
        {
            let fragment = FragmentMeta::new(object, index);
            // let key = fragment.key();
            // if self.peers[self.route.find(&key)]
            //     .fragments
            //     .contains_key(&key)
            // {
            //     fragments.push(fragment);
            // }
            assert!(self.peers[address].fragments.contains_key(&fragment.key()));
            fragments.push(fragment);
        }
        if fragments.len() as u32 >= self.config.object_size * 108 / 100 {
            self.stats.n_fast_recover += 1;
            return;
        }
        let mut context = DecodeContext::new(object);
        context.push_fragments(fragments.iter());
        assert!(context.is_recovered());
        self.stats.n_recover += 1;
    }

    fn compromise(&mut self, address: &PeerAddress) {
        let peer = self.peers.get_mut(address).unwrap();
        if peer.faulty_deadline.is_some() {
            assert!(peer.fragments.is_empty());
        }
        for (_, fragment) in peer.fragments.drain() {
            // it is ok to have no cache, maybe already cleaned during celebration
            if let Some(cache_address) = self
                .fragment_cache
                .get_mut(&fragment.object_thumbnail)
                .unwrap()
                .remove(&fragment.index)
            {
                assert_eq!(&cache_address, address);
            }
        }
        peer.faulty_deadline = Some(self.virtual_time + self.config.fault_switch_duration); //
        self.insert_event(
            self.config.fault_switch_duration,
            SystemEvent::ResetPeer(*address),
        );
    }

    fn handle_event(&mut self, event: SystemEvent, rng: &mut impl Rng) {
        match event {
            SystemEvent::Checkpoint => {
                println!("Checkpoint");
                println!("   Time {}days", self.virtual_time / 86400);
                println!("   Fast recovery {}", self.stats.n_fast_recover);
                println!("   Recovery {}", self.stats.n_recover);
                self.insert_event(self.config.checkpoint_duration, SystemEvent::Checkpoint);
            }
            SystemEvent::Celebrate(object, age) => {
                self.check_object(&object, age - 1);
                self.fragment_cache
                    .get_mut(&object.thumbnail)
                    .unwrap()
                    .clear(); // careful
                self.insert_object(&object, age);
                // garbage collection on peers?
                self.insert_event(
                    self.config.age_duration,
                    SystemEvent::Celebrate(object, age + 1),
                );
            }
            SystemEvent::FaultSwitch => {
                for address in self
                    .peers
                    .keys()
                    .copied()
                    .choose_multiple(rng, self.config.n_faulty_peer as _)
                {
                    self.compromise(&address);
                }
                self.insert_event(self.config.fault_switch_duration, SystemEvent::FaultSwitch);
            }
            SystemEvent::ResetPeer(address) => {
                let faulty_deadline = self.peers[&address].faulty_deadline.unwrap();
                if faulty_deadline <= self.virtual_time {
                    self.peers.get_mut(&address).unwrap().faulty_deadline = None;
                }
            }
        }
    }

    pub fn run(&mut self, rng: &mut impl Rng, stop_instant: VirtualInstant) {
        loop {
            let event;
            (Reverse(self.virtual_time), event) = self.events.pop().unwrap();
            if self.virtual_time > stop_instant {
                return;
            }
            self.handle_event(event, rng);
        }
    }
}

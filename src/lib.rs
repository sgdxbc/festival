pub mod encode;
pub mod network;

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
};

use encode::{DecodeContext, FragmentMeta};
use network::Route;
use rand::{seq::IteratorRandom, Rng};

pub type Thumbnail = [u8; 32];
pub type FragmentKey = [u8; 32];
pub type PeerAddress = [u8; 32];
pub type VirtualInstant = u32;
pub type VirtualDuration = u32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StorageObject {
    thumbnail: Thumbnail,
    size: u32,
}

pub struct Peer {
    fragments: HashMap<FragmentKey, FragmentMeta>,
    faulty_deadline: Option<VirtualInstant>,
}

pub struct System {
    virtual_time: VirtualInstant,
    peers: HashMap<PeerAddress, Peer>,
    route: Route,
    events: BinaryHeap<(Reverse<VirtualInstant>, SystemEvent)>,

    n_faulty: u32,
    n_fragment: u32,
    age_duration: VirtualDuration,
    fault_switch_duration: VirtualDuration,
    checkpoint_duration: VirtualDuration,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SystemEvent {
    Checkpoint,
    Celebrate(StorageObject, u32),
    FaultSwitch, //
    ResetPeer(PeerAddress),
}

impl System {
    pub fn new(
        rng: &mut impl Rng,
        n_peer: u32,
        n_faulty: u32,
        n_object: u32,
        object_size: u32,
        n_fragment: u32,
        age_duration: VirtualDuration,
        fault_switch_duration: VirtualDuration,
        checkpoint_duration: VirtualDuration,
    ) -> Self {
        let mut peers = HashMap::new();
        let mut route = Route::new();
        for _ in 0..n_peer {
            let address = rng.gen();
            peers.insert(
                address,
                Peer {
                    fragments: HashMap::new(),
                    faulty_deadline: None,
                },
            );
            route.insert(address);
        }
        let mut system = Self {
            virtual_time: 0,
            peers,
            route,
            events: Default::default(),

            n_faulty,
            n_fragment,
            age_duration,
            fault_switch_duration,
            checkpoint_duration,
        };
        system.insert_event(checkpoint_duration, SystemEvent::Checkpoint);
        for _ in 0..n_object {
            let object = StorageObject {
                thumbnail: rng.gen(),
                size: object_size,
            };
            system.insert_object(&object, 0);
            system.insert_event(
                (0..age_duration).choose(rng).unwrap(),
                SystemEvent::Celebrate(object, 1),
            );
        }
        for address in system
            .peers
            .keys()
            .copied()
            .choose_multiple(rng, n_faulty as _)
        {
            system.compromise(&address);
        }
        system.insert_event(fault_switch_duration, SystemEvent::FaultSwitch);
        system
    }

    fn insert_event(&mut self, duration: VirtualDuration, event: SystemEvent) {
        self.events
            .push((Reverse(self.virtual_time + duration), event));
    }

    fn insert_object(&mut self, object: &StorageObject, age: u32) {
        for index in self.n_fragment * age..self.n_fragment * (age + 1) {
            let fragment = FragmentMeta::new(object, index);
            let key = fragment.key();
            let peer = self.peers.get_mut(self.route.find(&key)).unwrap();
            if peer.faulty_deadline.is_none() {
                peer.fragments.insert(key, fragment);
            }
        }
    }

    fn check_object(&self, object: &StorageObject, age: u32) {
        let mut fragments = Vec::new();
        for index in self.n_fragment * age..self.n_fragment * (age + 1) {
            let fragment = FragmentMeta::new(object, index);
            let key = fragment.key();
            if self.peers[self.route.find(&key)]
                .fragments
                .contains_key(&key)
            {
                fragments.push(fragment);
            }
        }
        let mut context = DecodeContext::new(object);
        context.push_fragments(fragments.iter());
        assert!(context.is_recovered());
    }

    fn compromise(&mut self, address: &PeerAddress) {
        let peer = self.peers.get_mut(address).unwrap();
        if peer.faulty_deadline.is_some() {
            assert!(peer.fragments.is_empty());
        }
        peer.fragments.clear();
        peer.faulty_deadline = Some(self.virtual_time + self.fault_switch_duration); //
        self.insert_event(self.fault_switch_duration, SystemEvent::ResetPeer(*address));
    }

    fn handle_event(&mut self, event: SystemEvent, rng: &mut impl Rng) {
        match event {
            SystemEvent::Checkpoint => {
                println!(
                    "Checkpoint Time {:?}days",
                    self.virtual_time as f32 / 86400.
                );
                self.insert_event(self.checkpoint_duration, SystemEvent::Checkpoint);
            }
            SystemEvent::Celebrate(object, age) => {
                self.check_object(&object, age - 1);
                self.insert_object(&object, age);
                // garbage collection?
                self.insert_event(self.age_duration, SystemEvent::Celebrate(object, age + 1));
            }
            SystemEvent::FaultSwitch => {
                for address in self
                    .peers
                    .keys()
                    .copied()
                    .choose_multiple(rng, self.n_faulty as _)
                {
                    self.compromise(&address);
                }
                self.insert_event(self.fault_switch_duration, SystemEvent::FaultSwitch);
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
            if self.virtual_time >= stop_instant {
                return;
            }
            self.handle_event(event, rng);
        }
    }
}

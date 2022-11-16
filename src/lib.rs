pub mod encode;

use std::collections::{BTreeSet, HashMap};

use encode::FragmentMeta;

pub type Thumbnail = [u8; 32];
pub type FragmentKey = [u8; 32];
pub type PeerAddress = [u8; 32];
pub type VirtualInstant = u32;
pub type VirtualDuration = u32;

pub struct StorageObject {
    thumbnail: Thumbnail,
    size: u32,
}

pub struct Peer {
    fragments: HashMap<FragmentKey, FragmentMeta>,
    is_faulty: bool,
}

pub struct System {
    virtual_time: VirtualInstant,
    peers: HashMap<PeerAddress, Peer>,
    events: BTreeSet<(VirtualInstant, SystemEvent)>,
}

pub enum SystemEvent {
    //
}

use std::env;

use festival_sim::{ProtocolConfig, System, SystemConfig};
use rand::{rngs::SmallRng, SeedableRng};

fn main() {
    let rng = SmallRng::seed_from_u64(
        env::var("FESTIVAL_SEED")
            .map(|arg| arg.parse().unwrap())
            .unwrap_or_default(),
    );

    let config = SystemConfig {
        n_peer: 20000,
        failure_rate: 4.,
        n_object: 10,
        // protocol: ProtocolConfig::Festival {
        //     n_peer_per_age: 26,
        //     k: 256,
        //     k_repair: 400,
        //     check_celebration_sec: 24 * 3600,
        //     gossip_sec: 18 * 3600,
        // },
        protocol: ProtocolConfig::Kademlia {
            n: 20,
            republish_sec: 3600,
        },
    };
    println!("{config:?}");

    let mut system = System::new(rng, config);
    system.run(365 * 86400);
    println!("{:?}", system.stats);
}

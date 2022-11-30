use std::env;

use festival::{ProtocolConfig::Festival, System, SystemConfig};
use rand::{rngs::SmallRng, SeedableRng};

fn main() {
    let rng = SmallRng::seed_from_u64(
        env::var("FESTIVAL_SEED")
            .map(|arg| arg.parse().unwrap())
            .unwrap_or_default(),
    );
    let mut system = System::new(
        rng,
        SystemConfig {
            n_peer: 10000,
            failure_rate: 10. * 86400.,
            n_object: 1,
            protocol: Festival {
                n_peer_per_age: 50,
                k: 256,
                k_repair: 300,
                check_celebration_sec: 86400,
                gossip_sec: 6 * 3600,
            },
        },
    );
    system.run(365 * 86400);
    println!("{:?}", system.stats);
}

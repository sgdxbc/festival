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
        n_peer: 100000,
        failure_rate: 1.,
        n_object: 1,
        protocol: ProtocolConfig::Festival {
            k: 64,
            k_select: 64 * 2,
            k_repair: 64 * 8 / 5,
            cache_hit_rate: 0.1,
        },
        // protocol: ProtocolConfig::Replicated { n: 3 },
    };
    println!("{config:?}");

    let mut system = System::new(rng, config);
    system.run(365 * 86400);
    println!("{:?}", system.stats);
}

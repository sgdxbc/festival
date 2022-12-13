use std::thread::spawn;

use rand::{rngs::SmallRng, SeedableRng};
use sim::{ProtocolConfig, System, SystemConfig};

fn main() {
    let cache_hit_rate = 1.;
    let config = SystemConfig {
        n_peer: 100000,
        failure_rate: 1.,
        n_object: 1,
        protocol: ProtocolConfig::Festival {
            k: 64,
            k_select: 64 * 2,
            k_repair: 64 * 8 / 5,
            cache_hit_rate,
        },
        // protocol: ProtocolConfig::Replicated { n: 3 },
    };
    println!("{config:?}");

    let mut threads = Vec::new();
    for i in 0..10 {
        let config = config.clone();
        threads.push(spawn(move || {
            let rng = SmallRng::seed_from_u64(
                // env::var("FESTIVAL_SEED")
                //     .map(|arg| arg.parse().unwrap())
                //     .unwrap_or_default(),
                i,
            );

            let mut system = System::new(rng, config.clone());
            system.run(10 * 365 * 86400);
            println!("{:?}", system.stats);

            println!(
                "entropy-{cache_hit_rate},{},{},{},{}",
                config.n_peer, config.failure_rate, config.n_object, system.stats.n_repair
            );
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
}

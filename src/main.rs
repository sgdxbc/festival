use festival::{System, SystemConfig};
use rand::{rngs::SmallRng, SeedableRng};

fn main() {
    let rng = SmallRng::seed_from_u64(0);
    let mut system = System::new(
        rng,
        SystemConfig {
            n_peer: 10000,
            failure_rate: 10. * 86400.,
        },
    );
    system.run(10 * 365 * 86400);
    println!("{:?}", system.stats);
}

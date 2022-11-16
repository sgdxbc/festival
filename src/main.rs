use festival::System;
use rand::{rngs::StdRng, SeedableRng};

fn main() {
    let mut rng = StdRng::seed_from_u64(0);
    let mut system = System::new(
        &mut rng,
        1000,
        100,
        10,
        5000,
        6000,
        86400,
        86400 * 10,
        86400,
    );
    system.run(&mut rng, 86400 * 365 * 10);
}

use festival::{System, SystemConfig};
use rand::{rngs::StdRng, SeedableRng};

fn main() {
    let mut rng = StdRng::seed_from_u64(0);
    let mut system = System::new(
        &mut rng,
        SystemConfig {
            n_fragment: 6800,
            n_object: 100,
            // checkpoint_duration: 7 * 86400,
            ..Default::default()
        },
    );
    system.run(&mut rng, 86400 * 100);
}

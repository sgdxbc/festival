use std::{env, fs::File, io::Write, path::Path};

fn main() {
    let eps = 0.01;
    const F: f32 = 2114.;
    let mut rho = Vec::new();
    rho.push(0.);
    rho.push(1. - (1. + 1. / F) / (1. + eps));
    for i in 2..=F as _ {
        let i = i as f32;
        rho.push((1. - rho[1]) / ((1. - 1. / F) * i * (i - 1.)));
    }
    assert!((rho.iter().sum::<f32>() - 1.).abs() <= f32::EPSILON * 4.);

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("rho.rs");
    let mut dest = File::create(dest_path).unwrap();
    writeln!(dest, "[").unwrap();
    for value in rho {
        writeln!(dest, "    {value:.32},").unwrap();
    }
    writeln!(dest, "]").unwrap();

    println!("cargo:rerun-if-changed=build.rs");
}

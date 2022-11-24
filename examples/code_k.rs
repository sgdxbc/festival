use festival::{
    encode::{DecodeContext, FragmentMeta},
    StorageObject,
};
use plotters::prelude::*;
use rand::random;

fn main() {
    let x = [
        100, 200, 300, 400, 500, 600, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2400, 2800, 3200,
        3400, 3800, 4200, 4600, 5000,
    ];
    let mut y = Vec::new();
    for k in x {
        let object = StorageObject {
            thumbnail: [0; 32],
            size: k,
        };
        let n = (0..100).map(|_| n_fragment(&object, k)).max().unwrap();
        println!("{k} {n}",);
        y.push(n);
    }
    let root = BitMapBackend::new("0.png", (640, 480)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .margin(5)
        .x_label_area_size(30)
        .y_label_area_size(30)
        .build_cartesian_2d(0..5000, 0..5400)
        .unwrap();
    chart.configure_mesh().draw().unwrap();
    chart
        .draw_series(LineSeries::new(
            x.into_iter()
                .zip(y.into_iter())
                .map(|(x, y)| (x as i32, y as i32)),
            &RED,
        ))
        .unwrap();
    chart
        .draw_series(LineSeries::new(
            x.into_iter().map(|x| (x as i32, x as i32)),
            &BLUE,
        ))
        .unwrap();
    root.present().unwrap();
}

fn n_fragment(object: &StorageObject, k: u32) -> u32 {
    let mut context = DecodeContext::new(object);
    let base_index = random();
    context.push_fragments(
        (base_index..base_index + k)
            .map(|index| FragmentMeta::new(object, index))
            .collect::<Vec<_>>()
            .iter(),
    );
    for index in base_index + k.. {
        context.push_fragments([FragmentMeta::new(object, index)].iter());
        if context.is_recovered() {
            return index - base_index + 1;
        }
    }
    unreachable!()
}

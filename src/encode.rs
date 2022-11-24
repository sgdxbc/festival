use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    hash::{Hash, Hasher},
};

use rand::{
    distributions::WeightedIndex, prelude::Distribution, rngs::StdRng, seq::IteratorRandom, Rng,
    SeedableRng,
};

use crate::{FragmentKey, StorageObject, Thumbnail};

pub struct FragmentMeta {
    pub object_thumbnail: Thumbnail,
    pub index: u32,
    size: u32,
}

fn auxiliary_size(size: u32) -> u32 {
    let k = 3.;
    let delta = 0.005;
    (delta * k * size as f32).ceil() as _
}

fn block_choose_auxiliary(size: u32, block_index: u32) -> impl Iterator<Item = u32> {
    let k = 3;
    let mut rng = StdRng::seed_from_u64(block_index as _);
    (0..auxiliary_size(size))
        .choose_multiple(&mut rng, k)
        .into_iter()
}

impl FragmentMeta {
    pub fn new(object: &StorageObject, index: u32) -> Self {
        Self {
            object_thumbnail: object.thumbnail,
            index,
            size: object.size,
        }
    }

    pub fn choose_composite(&self) -> impl Iterator<Item = u32> {
        let mut rng = StdRng::seed_from_u64(self.index as _);
        let degree = WeightedIndex::new(include!(concat!(env!("OUT_DIR"), "/rho.rs")))
            .unwrap()
            .sample(&mut rng);
        (0..(self.size + auxiliary_size(self.size)))
            .choose_multiple(&mut rng, degree)
            .into_iter()
    }

    pub fn key(&self) -> FragmentKey {
        let mut hasher = DefaultHasher::new();
        (self.object_thumbnail, self.index).hash(&mut hasher);
        StdRng::seed_from_u64(hasher.finish()).gen()
    }
}

pub struct DecodeContext {
    object_thumbnail: Thumbnail,
    size: u32,
    recovered_composites: HashSet<u32>, // in range 0..(size + auxiliary size)
    pending_blocks: Vec<u32>,
    buffered_fragments: Vec<Vec<u32>>,
}

impl DecodeContext {
    pub fn new(object: &StorageObject) -> Self {
        Self {
            object_thumbnail: object.thumbnail,
            size: object.size,
            recovered_composites: Default::default(),
            pending_blocks: (0..object.size).collect(),
            buffered_fragments: Default::default(),
        }
    }

    pub fn push_fragments<'a>(&mut self, fragments: impl Iterator<Item = &'a FragmentMeta>) {
        self.buffered_fragments.extend(fragments.map(|fragment| {
            assert_eq!(fragment.object_thumbnail, self.object_thumbnail);
            assert_eq!(fragment.size, self.size);
            fragment.choose_composite().collect()
        }));
        loop {
            let mut composites = HashSet::new();
            self.buffered_fragments.retain_mut(|fragment| {
                fragment.retain(|composite| !self.recovered_composites.contains(composite));
                if fragment.is_empty() {
                    false
                } else if fragment.len() == 1 {
                    composites.insert(fragment[0]);
                    false
                } else {
                    true
                }
            });
            if composites.is_empty() {
                break;
            }
            self.recovered_composites.extend(composites);

            loop {
                let mut recovered_blocks = HashSet::new();
                self.pending_blocks.retain(|&block| {
                    let recovered = self.recovered_composites.contains(&block)
                        || block_choose_auxiliary(self.size, block)
                            .all(|composite| self.recovered_composites.contains(&composite));
                    if recovered {
                        recovered_blocks.insert(block);
                    }
                    !recovered
                });
                if recovered_blocks.is_empty() {
                    break;
                }
                self.recovered_composites.extend(recovered_blocks);
            }
        }
    }

    pub fn is_recovered(&self) -> bool {
        self.pending_blocks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::random;

    use super::{DecodeContext, FragmentMeta};

    #[test]
    fn reproducible_fragment() {
        let size = 5000;
        let object = crate::StorageObject {
            thumbnail: [0; 32],
            size,
        };
        let composites = (0..size + 500)
            .map(|index| {
                FragmentMeta::new(&object, index)
                    .choose_composite()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for index in 0..composites.len() as u32 {
            assert_eq!(
                FragmentMeta::new(&object, index)
                    .choose_composite()
                    .collect::<Vec<_>>(),
                composites[index as usize]
            );
        }
    }

    fn unique_fragment_with_base_index(base_index: u32) {
        let size = 5000;
        let object = crate::StorageObject {
            thumbnail: [0; 32],
            size,
        };
        let composites = (0..size + 400)
            .map(|index| {
                FragmentMeta::new(&object, base_index + index)
                    .choose_composite()
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert!(composites.len() >= (size + 397) as _);
    }

    #[test]
    fn unique_fragment() {
        for _ in 0..10 {
            unique_fragment_with_base_index(random());
        }
    }

    fn recover_with_base_index(base_index: u32) {
        let size = 5000;
        let object = crate::StorageObject {
            thumbnail: [0; 32],
            size,
        };
        let fragments = (0..size + 400)
            .map(|index| FragmentMeta::new(&object, base_index + index))
            .collect::<Vec<_>>();
        let mut context = DecodeContext::new(&object);
        context.push_fragments(fragments.iter());
        context.push_fragments([].iter()); // no progress but minimize buffer
        println!("Pending blocks {:?}", context.pending_blocks);
        println!("Buffered fragments {:?}", context.buffered_fragments);
        assert!(context.is_recovered());
    }

    #[test]
    fn recover() {
        for _ in 0..10 {
            recover_with_base_index(random());
        }
    }
}

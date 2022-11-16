use crate::{FragmentKey, PeerAddress};

#[derive(Debug)]
pub struct Route {
    addresses: Vec<PeerAddress>,
}

fn distance_level(k: &[u8; 32], other_k: &[u8; 32]) -> usize {
    let mut last_level = 0;
    let level = (k
        .iter()
        .zip(other_k.iter())
        .map(|(&byte, &other_byte)| (byte ^ other_byte).leading_zeros())
        .take_while(|&common| {
            last_level = common;
            common == 8
        })
        .sum::<u32>()
        + last_level) as usize;
    assert!(level < k.len() * 8);
    level
}

impl Route {
    pub fn new() -> Self {
        Self {
            addresses: Default::default(),
        }
    }

    pub fn find(&self, key: &FragmentKey) -> &PeerAddress {
        self.addresses
            .iter()
            .min_by_key(|&address| distance_level(address, key))
            .unwrap()
    }

    pub fn insert(&mut self, address: PeerAddress) {
        self.addresses.push(address);
    }
}

#[cfg(test)]
mod tests {
    use rand::random;

    use crate::{FragmentKey, PeerAddress};

    use super::{distance_level, Route};

    fn find_closest_with_route_and_key(
        route: &Route,
        addresses: &[PeerAddress],
        key: &FragmentKey,
    ) {
        let address = route.find(key);
        let level = distance_level(address, key);
        for address in addresses {
            assert!(distance_level(address, key) >= level);
        }
    }

    // not very necessary for current implementation
    // mostly to prevent regression if we move on to a more sophisticated implementation
    #[test]
    fn find_closest() {
        for _ in 0..10 {
            let mut route = Route::new();
            let mut addresses = Vec::new();
            for _ in 0..1000 {
                let address = random();
                route.insert(address);
                addresses.push(address);
            }
            for _ in 0..100 {
                find_closest_with_route_and_key(&route, &addresses, &random());
            }
        }
    }

    #[test]
    fn reproducible_find() {
        let mut route = Route::new();
        for _ in 0..1000 {
            route.insert(random());
        }
        let key = random();
        let address = route.find(&key);
        for _ in 0..10 {
            assert_eq!(route.find(&key), address);
        }
    }
}

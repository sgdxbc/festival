use crate::{FragmentKey, PeerAddress};

#[derive(Debug)]
pub enum Route {
    Empty,
    Peer(PeerAddress),
    Divide(Box<Route>, Box<Route>),
}

fn level_bit(k: &[u8; 32], level: usize) -> u8 {
    k[level / 8].reverse_bits() >> (level % 8) & 0x1
}

impl Route {
    pub fn find(&self, key: &FragmentKey) -> &PeerAddress {
        self.find_level(key, 0)
    }

    fn find_level(&self, key: &FragmentKey, level: usize) -> &PeerAddress {
        match self {
            Self::Empty => unreachable!(),
            Self::Peer(address) => address,
            Self::Divide(route0, route1) => match (&**route0, &**route1, level_bit(key, level)) {
                (route, Self::Empty, _)
                | (Self::Empty, route, _)
                | (route, _, 0)
                | (_, route, 1) => route.find_level(key, level + 1),
                _ => unreachable!(),
            },
        }
    }

    pub fn insert(&mut self, address: PeerAddress) {
        self.insert_level(address, 0)
    }

    fn insert_level(&mut self, address: PeerAddress, level: usize) {
        if level == address.len() * 8 {
            assert!(matches!(self, Self::Empty));
            *self = Self::Peer(address);
            return;
        }

        if let Self::Empty = self {
            *self = Self::Divide(Box::new(Self::Empty), Box::new(Self::Empty));
        }
        match (self, level_bit(&address, level)) {
            (Self::Divide(route, _), 0) | (Self::Divide(_, route), 1) => {
                route.insert_level(address, level + 1)
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::random;

    use crate::{FragmentKey, PeerAddress};

    use super::Route;

    fn distance_level(k: &[u8; 32], other_k: &[u8; 32]) -> f32 {
        let mut distance = 0.;
        for diff in k
            .iter()
            .zip(other_k.iter())
            .map(|(&byte, &other_byte)| byte ^ other_byte)
        {
            distance = distance * 256. + diff as f32;
        }
        distance
    }

    fn find_closest_with_route_and_key(
        route: &Route,
        addresses: &[PeerAddress],
        key: &FragmentKey,
    ) {
        let address = route.find(key);
        let level = distance_level(address, key);
        for other_address in addresses {
            assert!(distance_level(other_address, key) >= level);
        }
    }

    #[test]
    fn find_closest() {
        for _ in 0..10 {
            let mut route = Route::Empty;
            let mut addresses = Vec::new();
            for _ in 0..1000 {
                let address = random();
                route.insert(address);
                addresses.push(address);
            }
            for _ in 0..10000 {
                find_closest_with_route_and_key(&route, &addresses, &random());
            }
        }
    }

    #[test]
    fn reproducible_find() {
        let mut route = Route::Empty;
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

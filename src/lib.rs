use std::{io, marker, str};
use std::borrow::Cow;
use zerocopy::{LayoutVerified, AsBytes, FromBytes};

mod traits;
mod types;

pub use self::traits::{EPAsBytes, EPFromBytes};
pub use self::types::{Type, Slice, Str, Ignore};

pub struct Database<KC, DC> {
    marker: marker::PhantomData<(KC, DC)>,
}

impl<KC, DC> Database<KC, DC> {
    pub fn new() -> Database<KC, DC> {
        Database { marker: marker::PhantomData }
    }
}

const STATIC_BYTES: [u8; 21] = [0; 21];

impl<KC, DC> Database<KC, DC>
where
    KC: EPFromBytes,
    KC::Output: EPAsBytes,
    DC: EPFromBytes,
    DC::Output: EPAsBytes,
{
    pub fn get(&self, key: &KC::Output) -> io::Result<Option<Cow<DC::Output>>> {
        let key_bytes: Cow<[u8]> = key.as_bytes();

        Ok(DC::from_bytes(&STATIC_BYTES))
    }

    pub fn put(&self, key: &KC::Output, data: &DC::Output) -> io::Result<Option<Cow<DC::Output>>> {
        let key_bytes: Cow<[u8]> = key.as_bytes();
        let data_bytes: Cow<[u8]> = data.as_bytes();

        Ok(DC::from_bytes(&STATIC_BYTES))
    }
}

use std::error::Error as StdError;
use std::marker;
use std::result::Result as StdResult;

use crate::{Result, Error};

/// Lazily decode the data bytes, it can be used to avoid CPU intensive decoding
/// before making sure we really need to decode it (e.g. based on the key).
#[derive(Default)]
pub struct LazyDecode<C>(marker::PhantomData<C>);

impl<'a, C: 'static> heed_traits::BytesDecode<'a> for LazyDecode<C> {
    type DItem = Lazy<'a, C>;

    fn bytes_decode(bytes: &'a [u8]) -> StdResult<Self::DItem, Box<dyn StdError>> {
        Ok(Lazy { data: bytes, _phantom: marker::PhantomData })
    }
}

/// Owns bytes that can be decoded on demand.
#[derive(Copy, Clone)]
pub struct Lazy<'a, C> {
    data: &'a [u8],
    _phantom: marker::PhantomData<C>,
}

impl<'a, C: heed_traits::BytesDecode<'a>> Lazy<'a, C> {
    pub fn decode(&self) -> Result<C::DItem> {
        C::bytes_decode(self.data).map_err(Error::Decoding)
    }
}

use std::borrow::Cow;

use heed_traits::{BytesDecode, BytesEncode};
use serde::{Deserialize, Serialize};

/// Describes a type that is [`Serialize`]/[`Deserialize`] and uses `bincode` to do so.
///
/// It can borrow bytes from the original slice.
pub struct SerdeBincode<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> BytesEncode<'a> for SerdeBincode<T>
where
    T: Serialize,
{
    type EItem = T;
    type Err = bincode::Error;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        bincode::serialize(item).map(Cow::Owned)
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeBincode<T>
where
    T: Deserialize<'a>,
{
    type DItem = T;
    type Err = bincode::Error;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err> {
        bincode::deserialize(bytes)
    }
}

unsafe impl<T> Send for SerdeBincode<T> {}

unsafe impl<T> Sync for SerdeBincode<T> {}

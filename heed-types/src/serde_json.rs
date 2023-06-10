use std::borrow::Cow;

use heed_traits::{BytesDecode, BytesEncode};
use serde::{Deserialize, Serialize};

/// Describes a type that is [`Serialize`]/[`Deserialize`] and uses `serde_json` to do so.
///
/// It can borrow bytes from the original slice.
pub struct SerdeJson<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> BytesEncode<'a> for SerdeJson<T>
where
    T: Serialize,
{
    type EItem = T;
    type Err = serde_json::Error;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        serde_json::to_vec(item).map(Cow::Owned).map_err(Into::into)
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeJson<T>
where
    T: Deserialize<'a>,
{
    type DItem = T;
    type Err = serde_json::Error;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err> {
        serde_json::from_slice(bytes)
    }
}

unsafe impl<T> Send for SerdeJson<T> {}

unsafe impl<T> Sync for SerdeJson<T> {}

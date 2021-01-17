use heed_traits::{BytesDecode, BytesEncode};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Describes a type that is [`Serialize`]/[`Deserialize`] and uses `serde_json` to do so.
///
/// It can borrow bytes from the original slice.
pub struct SerdeJson<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for SerdeJson<T>
where
    T: Serialize,
{
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        serde_json::to_vec(item).map(Cow::Owned).ok()
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeJson<T>
where
    T: Deserialize<'a>,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        serde_json::from_slice(bytes).ok()
    }
}

unsafe impl<T> Send for SerdeJson<T> {}

unsafe impl<T> Sync for SerdeJson<T> {}

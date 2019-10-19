use std::borrow::Cow;
use serde::{Serialize, Deserialize};
use crate::{BytesEncode, BytesDecode};

/// Describes a type that is [`Serialize`]/[`Deserialize`] and uses bincode to do that.
///
/// It can borrow bytes from the original slice.
pub struct SerdeJson<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for SerdeJson<T> where T: Serialize {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        serde_json::to_vec(item).map(Cow::Owned).ok()
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeJson<T> where T: Deserialize<'a> + Clone {
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        serde_json::from_slice(bytes).ok()
    }
}

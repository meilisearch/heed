use heed_traits::{BoxedError, BytesDecode, ToBytes};
use serde::{Deserialize, Serialize};

/// Describes a type that is [`Serialize`]/[`Deserialize`] and uses `rmp_serde` to do so.
///
/// It can borrow bytes from the original slice.
pub struct SerdeRmp<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> ToBytes<'a> for SerdeRmp<T>
where
    T: Serialize,
{
    type SelfType = T;

    type ReturnBytes = Vec<u8>;

    type Error = rmp_serde::encode::Error;

    fn to_bytes(item: &'a Self::SelfType) -> Result<Self::ReturnBytes, Self::Error> {
        rmp_serde::to_vec(item)
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeRmp<T>
where
    T: Deserialize<'a>,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        rmp_serde::from_slice(bytes).map_err(Into::into)
    }
}

unsafe impl<T> Send for SerdeRmp<T> {}

unsafe impl<T> Sync for SerdeRmp<T> {}

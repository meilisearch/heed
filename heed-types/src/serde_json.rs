use heed_traits::{BoxedError, BytesDecode, BytesEncode};
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

    type ReturnBytes = Vec<u8>;

    type Error = serde_json::Error;

    fn zero_copy(_item: &Self::EItem) -> bool {
        false
    }

    fn bytes_encode(item: &Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        serde_json::to_vec(item)
    }

    fn bytes_encode_into_writer<W: std::io::Write>(
        item: &'a Self::EItem,
        writer: W,
    ) -> Result<(), BoxedError> {
        serde_json::to_writer(writer, item)?;
        Ok(())
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeJson<T>
where
    T: Deserialize<'a>,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}

unsafe impl<T> Send for SerdeJson<T> {}

unsafe impl<T> Sync for SerdeJson<T> {}

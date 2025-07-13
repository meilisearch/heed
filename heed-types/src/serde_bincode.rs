use heed_traits::{BoxedError, BytesDecode, BytesEncode};
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

    type ReturnBytes = Vec<u8>;

    type Error = bincode::Error;

    fn zero_copy(_item: &Self::EItem) -> bool {
        false
    }

    fn bytes_encode(item: &Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        bincode::serialize(item)
    }

    fn bytes_encode_into_writer<W: std::io::Write>(
        item: &'a Self::EItem,
        writer: W,
    ) -> Result<(), BoxedError> {
        bincode::serialize_into(writer, item)?;
        Ok(())
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeBincode<T>
where
    T: Deserialize<'a>,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        bincode::deserialize(bytes).map_err(Into::into)
    }
}

unsafe impl<T> Send for SerdeBincode<T> {}

unsafe impl<T> Sync for SerdeBincode<T> {}

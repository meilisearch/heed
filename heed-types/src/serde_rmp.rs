use heed_traits::{BoxedError, BytesDecode, BytesEncode};
use serde::{Deserialize, Serialize};

/// Describes a type that is [`Serialize`]/[`Deserialize`] and uses `rmp_serde` to do so.
///
/// It can borrow bytes from the original slice.
pub struct SerdeRmp<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> BytesEncode<'a> for SerdeRmp<T>
where
    T: Serialize,
{
    type EItem = T;

    type ReturnBytes = Vec<u8>;

    type Error = rmp_serde::encode::Error;

    fn zero_copy(_item: &Self::EItem) -> bool {
        false
    }

    fn bytes_encode(item: &Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        rmp_serde::to_vec(item)
    }

    fn bytes_encode_into_writer<W: std::io::Write>(
        item: &'a Self::EItem,
        mut writer: W,
    ) -> Result<(), BoxedError> {
        rmp_serde::encode::write(&mut writer, item)?;
        Ok(())
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

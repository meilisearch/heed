use std::borrow::Cow;

use heed_traits::{BoxedError, BytesDecode, BytesEncode};
use serde::{Deserialize, Serialize};
use serde_wincode::SerdeCompat;

/// Describes a type that is [`Serialize`]/[`Deserialize`] and uses `wincode` to do so.
///
/// It can borrow bytes from the original slice.
pub struct SerdeWincode<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> BytesEncode<'a> for SerdeWincode<T>
where
    T: Serialize,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        use serde_wincode::wincode::Serialize;
        match SerdeCompat::<T>::serialize(item) {
            Ok(bytes) => Ok(Cow::Owned(bytes)),
            Err(err) => Err(err.into()),
        }
    }
}

impl<'a, T: 'a> BytesDecode<'a> for SerdeWincode<T>
where
    T: Deserialize<'a>,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        use serde_wincode::wincode::Deserialize;
        SerdeCompat::<T>::deserialize(bytes).map_err(BoxedError::from)
    }
}

unsafe impl<T> Send for SerdeWincode<T> {}

unsafe impl<T> Sync for SerdeWincode<T> {}

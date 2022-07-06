use std::borrow::Cow;

use bytemuck::PodCastError;
use heed_traits::{BoxedError, BytesDecode, BytesEncode};

/// Describes the `()` type.
pub struct Unit;

impl BytesEncode<'_> for Unit {
    type EItem = ();

    fn bytes_encode(_item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        Ok(Cow::Borrowed(&[]))
    }
}

impl BytesDecode<'_> for Unit {
    type DItem = ();

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(PodCastError::SizeMismatch.into())
        }
    }
}

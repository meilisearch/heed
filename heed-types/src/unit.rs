use std::borrow::Cow;
use std::convert::Infallible;

use bytemuck::PodCastError;
use heed_traits::{BytesDecode, BytesEncode};

/// Describes the `()` type.
pub struct Unit;

impl BytesEncode<'_> for Unit {
    type EItem = ();
    type Err = Infallible;

    fn bytes_encode(_item: &Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        Ok(Cow::Borrowed(&[]))
    }
}

impl BytesDecode<'_> for Unit {
    type DItem = ();
    type Err = PodCastError;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, Self::Err> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(PodCastError::SizeMismatch)
        }
    }
}

use std::convert::Infallible;

use heed_traits::{BoxedError, BytesDecode, BytesEncode};

/// Describes a [`str`].
pub enum Str {}

impl<'a> BytesEncode<'a> for Str {
    type EItem = str;

    type ReturnBytes = &'a [u8];

    type Error = Infallible;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        Ok(item.as_bytes())
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        std::str::from_utf8(bytes).map_err(Into::into)
    }
}

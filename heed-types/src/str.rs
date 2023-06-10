use std::borrow::Cow;
use std::convert::Infallible;
use std::str::Utf8Error;

use heed_traits::{BytesDecode, BytesEncode};

/// Describes an [`prim@str`].
pub struct Str;

impl BytesEncode<'_> for Str {
    type EItem = str;
    type Err = Infallible;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        Ok(Cow::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;
    type Err = Utf8Error;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err> {
        std::str::from_utf8(bytes)
    }
}

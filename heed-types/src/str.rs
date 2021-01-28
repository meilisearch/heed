use std::borrow::Cow;
use std::error::Error;
use std::str;

use heed_traits::{BytesDecode, BytesEncode};

use crate::UnalignedSlice;

/// Describes an [`prim@str`].
pub struct Str;

impl<'a> BytesEncode<'a> for Str {
    type EItem = str;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error>> {
        UnalignedSlice::bytes_encode(item.as_bytes())
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        str::from_utf8(bytes).map_err(Into::into)
    }
}

use std::borrow::Cow;
use std::error::Error;

use crate::UnalignedSlice;
use heed_traits::{BytesDecode, BytesEncode};

/// Describes an [`str`].
pub struct Str;

impl BytesEncode<'_> for Str {
    type EItem = str;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn Error>> {
        UnalignedSlice::<u8>::bytes_encode(item.as_bytes())
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        Ok(std::str::from_utf8(bytes)?)
    }
}

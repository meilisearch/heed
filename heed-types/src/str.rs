use std::borrow::Cow;
use std::str;

use heed_traits::{BytesDecode, BytesEncode};

use crate::UnalignedSlice;

/// Describes an [`str`].
pub struct Str;

impl BytesEncode for Str {
    type EItem<'a> = &'a str;

    fn bytes_encode<'a, 'b>(item: &'b Self::EItem<'a>) -> Option<Cow<'a, [u8]>> {
        UnalignedSlice::<u8>::bytes_encode(&item.as_bytes())
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        str::from_utf8(bytes).ok()
    }
}

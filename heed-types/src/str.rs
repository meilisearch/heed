use std::borrow::Cow;

use heed_traits::{BytesDecode, BytesEncode};

/// Describes an [`prim@str`].
pub struct Str;

impl BytesEncode<'_> for Str {
    type EItem = str;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        std::str::from_utf8(bytes).ok()
    }
}

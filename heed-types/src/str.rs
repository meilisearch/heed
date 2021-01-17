use std::borrow::Cow;
use std::{str, marker};

use heed_traits::{BytesDecode, BytesEncode};
use bytemuck::try_cast_slice;

/// Describes an [`str`].
pub struct Str<'a> {
    _phantom: marker::PhantomData<&'a ()>,
}

impl<'a> BytesEncode for Str<'a> {
    type EItem = &'a str;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        try_cast_slice(item.as_bytes()).map(Cow::Borrowed).ok()
    }
}

impl<'a> BytesDecode<'a> for Str<'_> {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        str::from_utf8(bytes).ok()
    }
}

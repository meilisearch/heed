use std::borrow::Cow;
use std::error::Error;
use std::{str, marker};

use heed_traits::{BytesDecode, BytesEncode};
use bytemuck::try_cast_slice;

/// Describes an [`prim@str`].
pub struct Str<'a> {
    _phantom: marker::PhantomData<&'a ()>,
}

impl<'a> BytesEncode for Str<'a> {
    type EItem = &'a str;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn Error>> {
        try_cast_slice(item.as_bytes()).map(Cow::Borrowed).map_err(Into::into)
    }
}

impl<'a> BytesDecode<'a> for Str<'_> {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        str::from_utf8(bytes).map_err(Into::into)
    }
}

use std::borrow::Cow;
use std::error::Error;

pub trait BytesEncode<'a> {
    type EItem: ?Sized + 'a;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error>>;
}

pub trait BytesDecode<'a> {
    type DItem: 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>>;
}

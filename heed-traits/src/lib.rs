use std::borrow::Cow;
use std::error::Error;

pub trait BytesEncode {
    type EItem: ?Sized;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn Error>>;
}

pub trait BytesDecode<'a> {
    type DItem: 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>>;
}

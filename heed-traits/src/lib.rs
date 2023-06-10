use std::borrow::Cow;

/// A trait that represents an encoding structure.
pub trait BytesEncode<'a> {
    type EItem: ?Sized + 'a;
    type Err;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Self::Err>;
}

/// A trait that represents a decoding structure.
pub trait BytesDecode<'a> {
    type DItem: 'a;
    type Err;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err>;
}

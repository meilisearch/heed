use std::borrow::Cow;
use std::error::Error as StdError;

/// A boxed `Send + Sync + 'static` error.
pub type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// A trait that represents an encoding structure.
pub trait BytesEncode<'a> {
    type EItem: ?Sized + 'a;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError>;
}

/// A trait that represents a decoding structure.
pub trait BytesDecode<'a> {
    type DItem: 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError>;
}

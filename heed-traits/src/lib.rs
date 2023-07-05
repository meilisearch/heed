//! Crate `heed-traits` contains the traits used to encode and decode database content.

#![warn(missing_docs)]

use std::borrow::Cow;
use std::error::Error as StdError;

/// A boxed `Send + Sync + 'static` error.
pub type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// A trait that represents an encoding structure.
pub trait BytesEncode<'a> {
    /// The type to encode
    type EItem: ?Sized + 'a;

    /// Encode the given item as bytes
    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError>;
}

/// A trait that represents a decoding structure.
pub trait BytesDecode<'a> {
    /// The type to decode
    type DItem: 'a;

    /// Decode the given bytes as DItem
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError>;
}

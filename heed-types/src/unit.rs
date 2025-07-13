use std::convert::Infallible;
use std::{error, fmt};

use heed_traits::{BoxedError, BytesDecode, BytesEncode};

/// Describes the unit `()` type.
pub enum Unit {}

impl BytesEncode<'_> for Unit {
    type EItem = ();

    type ReturnBytes = [u8; 0];

    type Error = Infallible;

    fn bytes_encode(&(): &Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        Ok([])
    }
}

impl BytesDecode<'_> for Unit {
    type DItem = ();

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(NonEmptyError.into())
        }
    }
}

/// The slice of bytes is non-empty and therefore is not a unit `()` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NonEmptyError;

impl fmt::Display for NonEmptyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("the slice of bytes is non-empty and therefore is not a unit `()` type")
    }
}

impl error::Error for NonEmptyError {}

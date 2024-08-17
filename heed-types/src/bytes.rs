use std::convert::Infallible;

use heed_traits::{BoxedError, BytesDecode, BytesEncode};

/// Describes a byte slice `[u8]` that is totally borrowed and doesn't depend on
/// any [memory alignment].
///
/// [memory alignment]: std::mem::align_of()
pub enum Bytes {}

impl<'a> BytesEncode<'a> for Bytes {
    type EItem = [u8];

    type ReturnBytes = &'a [u8];

    type Error = Infallible;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        Ok(item)
    }
}

impl<'a> BytesDecode<'a> for Bytes {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(bytes)
    }
}

/// Like [`Bytes`], but always contains exactly `N` (the generic parameter) bytes.
pub enum FixedSizeBytes<const N: usize> {}

impl<'a, const N: usize> BytesEncode<'a> for FixedSizeBytes<N> {
    type EItem = [u8; N];

    type ReturnBytes = &'a [u8; N];

    type Error = Infallible;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        Ok(item)
    }
}

impl<'a, const N: usize> BytesDecode<'a> for FixedSizeBytes<N> {
    type DItem = &'a [u8; N];

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        bytes.try_into().map_err(Into::into)
    }
}

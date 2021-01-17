use std::borrow::Cow;
use heed_traits::{BytesDecode, BytesEncode};

/// Describes the `()` type.
pub struct Unit;

impl BytesEncode for Unit {
    type EItem = ();

    fn bytes_encode(_item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(&[]))
    }
}

impl BytesDecode<'_> for Unit {
    type DItem = ();

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        if bytes.is_empty() {
            Some(())
        } else {
            None
        }
    }
}

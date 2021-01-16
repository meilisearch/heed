use std::borrow::Cow;
use heed_traits::{BytesDecode, BytesEncode};

/// Describes the `()` type.
pub struct Unit;

impl BytesEncode for Unit {
    type EItem<'a> = ();

    fn bytes_encode<'a, 'b>(_item: &'b Self::EItem<'a>) -> Option<Cow<'a, [u8]>> {
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

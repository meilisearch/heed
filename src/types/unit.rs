use crate::{BytesDecode, BytesEncode};
use std::borrow::Cow;

/// Describes the [unit `()`] type.
///
/// [unit `()`]: https://doc.rust-lang.org/std/primitive.unit.html
pub struct Unit;

impl BytesEncode for Unit {
    type EItem = ();

    fn bytes_encode(_item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(&[]))
    }
}

impl BytesDecode<'_> for Unit {
    type DItem = ();

    fn bytes_decode(_bytes: &[u8]) -> Option<Self::DItem> {
        Some(())
    }
}

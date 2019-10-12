use std::borrow::Cow;
use crate::{BytesEncode, BytesDecode};

pub struct Ignore;

impl BytesEncode for Ignore {
    type EItem = ();

    fn bytes_encode(_item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(&[]))
    }
}

impl BytesDecode<'_> for Ignore {
    type DItem = ();

    fn bytes_decode(_bytes: &[u8]) -> Option<Self::DItem> {
        Some(())
    }
}

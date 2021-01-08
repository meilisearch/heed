use std::borrow::Cow;
use std::cmp::Ordering;

pub trait BytesEncode<'a> {
    type EItem: ?Sized + 'a;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>>;
}

pub trait BytesDecode<'a> {
    type DItem: 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem>;
}

pub trait CustomKeyCmp<'a> {
    type Key: 'a;

    fn compare(a: Self::Key, b: Self::Key) -> Ordering;
}

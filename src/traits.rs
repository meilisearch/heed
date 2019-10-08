use std::borrow::Cow;

pub trait BytesEncode {
    type Item: ?Sized;

    fn bytes_encode(item: &Self::Item) -> Option<Cow<[u8]>>;
}

pub trait BytesDecode<'a> {
    type Item: ToOwned + ?Sized + 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Cow<'a, Self::Item>>;
}

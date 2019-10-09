use std::borrow::Cow;

pub trait BytesEncode {
    type EItem: ?Sized;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>>;
}

pub trait BytesDecode<'a> {
    type DItem: ToOwned + ?Sized + 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Cow<'a, Self::DItem>>;
}

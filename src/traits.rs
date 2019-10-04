use std::borrow::Cow;
use zerocopy::AsBytes;

pub trait EPAsBytes {
    fn as_bytes(&self) -> Cow<[u8]>;
}

pub trait EPFromBytes {
    type Output: ToOwned + ?Sized;

    fn from_bytes(bytes: &[u8]) -> Option<Cow<Self::Output>>;
}

impl<T: AsBytes + ?Sized> EPAsBytes for T {
    fn as_bytes(&self) -> Cow<[u8]> {
        Cow::Borrowed(<T as AsBytes>::as_bytes(self))
    }
}

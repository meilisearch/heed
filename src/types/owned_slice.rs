use std::borrow::Cow;

use zerocopy::{AsBytes, FromBytes};

use crate::{BytesEncode, BytesDecode};
use crate::types::CowSlice;

pub struct OwnedSlice<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for OwnedSlice<T> where T: AsBytes {
    type EItem = [T];

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<[T] as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for OwnedSlice<T> where T: FromBytes + Copy {
    type DItem = Vec<T>;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        CowSlice::<T>::bytes_decode(bytes).map(Cow::into_owned)
    }
}

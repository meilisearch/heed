use std::borrow::Cow;

use zerocopy::{AsBytes, FromBytes};
use crate::{BytesEncode, BytesDecode};
use crate::types::CowType;

pub struct OwnedType<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for OwnedType<T> where T: AsBytes {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for OwnedType<T> where T: FromBytes + Copy {
    type DItem = T;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        CowType::<T>::bytes_decode(bytes).map(Cow::into_owned)
    }
}

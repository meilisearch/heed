use std::borrow::Cow;

use zerocopy::{LayoutVerified, AsBytes, FromBytes, Unaligned};
use crate::{BytesEncode, BytesDecode};

pub struct UnalignedType<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for UnalignedType<T> where T: AsBytes + Unaligned {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for UnalignedType<T> where T: FromBytes + Unaligned + Copy {
    type DItem = &'a T;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        LayoutVerified::<_, T>::new_unaligned(bytes).map(LayoutVerified::into_ref)
    }
}

use std::borrow::Cow;

use zerocopy::{LayoutVerified, AsBytes, FromBytes, Unaligned};
use crate::{BytesEncode, BytesDecode};

/// Describes a slice that is totally borrowed and doesn't
/// depends on any [memory alignment].
///
/// If you need to store a slice that does depend on memory alignment
/// and that can be big it is recommended to use the [`CowType`].
///
/// To store slices, you must look at the [`CowSlice`],
/// [`OwnedSlice`] or [`UnalignedSlice`] types.
///
/// [memory alignment]: https://doc.rust-lang.org/std/mem/fn.align_of.html
/// [`CowType`]: crate::types::CowType
/// [`UnalignedSlice`]: crate::types::UnalignedSlice
/// [`OwnedSlice`]: crate::types::OwnedSlice
/// [`CowSlice`]: crate::types::CowSlice
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

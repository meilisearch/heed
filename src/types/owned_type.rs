use std::borrow::Cow;

use crate::types::CowType;
use crate::{BytesDecode, BytesEncode};
use zerocopy::{AsBytes, FromBytes};

/// Describes a type that is totally owned (doesn't
/// hold any reference to the original slice).
///
/// If you need to store a type that doesn't depends on any
/// [memory alignment] and that can be big it is recommended
/// to use the [`UnalignedType`].
///
/// The [`CowType`] is recommended for borrowed types (types that holds
/// references to the original slice).
///
/// To store slices, you must look at the [`CowSlice`],
/// [`OwnedSlice`] or [`UnalignedSlice`] types.
///
/// [memory alignment]: https://doc.rust-lang.org/std/mem/fn.align_of.html
/// [`UnalignedType`]: crate::types::UnalignedType
/// [`CowType`]: crate::types::CowType
/// [`UnalignedSlice`]: crate::types::UnalignedSlice
/// [`OwnedSlice`]: crate::types::OwnedSlice
/// [`CowSlice`]: crate::types::CowSlice
pub struct OwnedType<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for OwnedType<T>
where
    T: AsBytes,
{
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for OwnedType<T>
where
    T: FromBytes + Copy,
{
    type DItem = T;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        CowType::<T>::bytes_decode(bytes).map(Cow::into_owned)
    }
}

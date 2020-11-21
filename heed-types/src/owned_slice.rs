use std::borrow::Cow;

use zerocopy::{AsBytes, FromBytes};

use crate::CowSlice;
use heed_traits::{BytesDecode, BytesEncode};

/// Describes a [`Vec`] of types that are totally owned (doesn't
/// hold any reference to the original slice).
///
/// If you need to store a type that doesn't depends on any
/// [memory alignment] and that can be big it is recommended
/// to use the [`UnalignedSlice`].
///
/// The [`CowType`] is recommended for borrowed types (types that holds
/// references to the original slice).
///
/// [memory alignment]: std::mem::align_of()
/// [`UnalignedSlice`]: crate::UnalignedSlice
/// [`CowType`]: crate::CowType
pub struct OwnedSlice<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> BytesEncode<'a> for OwnedSlice<T>
where
    T: AsBytes,
{
    type EItem = [T];

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<[T] as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for OwnedSlice<T>
where
    T: FromBytes + Copy,
{
    type DItem = Vec<T>;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        CowSlice::<T>::bytes_decode(bytes).map(Cow::into_owned)
    }
}

unsafe impl<T> Send for OwnedSlice<T> {}

unsafe impl<T> Sync for OwnedSlice<T> {}

use std::borrow::Cow;
use std::error::Error;

use bytemuck::Pod;
use heed_traits::{BytesDecode, BytesEncode};

use crate::CowSlice;

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
pub struct OwnedSlice<'a, T>(std::marker::PhantomData<&'a T>);

impl<'a, T: Pod> BytesEncode for OwnedSlice<'a, T> {
    type EItem = &'a [T];

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn Error>> {
        CowSlice::bytes_encode(item)
    }
}

impl<'a, T: Pod + 'a> BytesDecode<'a> for OwnedSlice<'_, T> {
    type DItem = Vec<T>;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, Box<dyn Error>> {
        CowSlice::bytes_decode(bytes).map(Cow::into_owned)
    }
}

unsafe impl<T> Send for OwnedSlice<'_, T> {}

unsafe impl<T> Sync for OwnedSlice<'_, T> {}

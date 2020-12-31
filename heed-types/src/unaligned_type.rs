use std::borrow::Cow;
use std::error::Error;

use heed_traits::{BytesDecode, BytesEncode};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned};

/// Describes a slice that is totally borrowed and doesn't
/// depends on any [memory alignment].
///
/// If you need to store a slice that does depend on memory alignment
/// and that can be big it is recommended to use the [`CowType`].
///
/// To store slices, you must look at the [`CowSlice`],
/// [`OwnedSlice`] or [`UnalignedSlice`] types.
///
/// [memory alignment]: std::mem::align_of()
/// [`CowType`]: crate::CowType
/// [`UnalignedSlice`]: crate::UnalignedSlice
/// [`OwnedSlice`]: crate::OwnedSlice
/// [`CowSlice`]: crate::CowSlice
pub struct UnalignedType<T>(std::marker::PhantomData<T>);

impl<'a, T: 'a> BytesEncode<'a> for UnalignedType<T>
where
    T: AsBytes + Unaligned,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<[u8]>, Box<dyn Error>> {
        Ok(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for UnalignedType<T>
where
    T: FromBytes + Unaligned,
{
    type DItem = &'a T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        Ok(LayoutVerified::<_, T>::new_unaligned(bytes)
            .map(LayoutVerified::into_ref)
            .ok_or("The provided bytes do not satisfy the alignment requirements.")?)
    }
}

unsafe impl<T> Send for UnalignedType<T> {}

unsafe impl<T> Sync for UnalignedType<T> {}

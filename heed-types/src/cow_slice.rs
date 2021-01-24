use std::borrow::Cow;
use std::error::Error;

use bytemuck::{Pod, PodCastError, try_cast_slice, pod_collect_to_vec};
use heed_traits::{BytesDecode, BytesEncode};

/// Describes a slice that must be [memory aligned] and
/// will be reallocated if it is not.
///
/// A [`Cow`] type is returned to represent this behavior.
///
/// If you need to store a slice that doesn't depends on any
/// memory alignment it is recommended to use the [`UnalignedSlice`].
///
/// if you don't want to be bored with the [`Cow`] type you can
/// use the [`OwnedSlice`].
///
/// [memory aligned]: std::mem::align_of()
/// [`Cow`]: std::borrow::Cow
/// [`UnalignedSlice`]: crate::UnalignedSlice
/// [`OwnedSlice`]: crate::OwnedSlice
pub struct CowSlice<'a, T>(std::marker::PhantomData<&'a T>);

impl<'a, T: Pod> BytesEncode for CowSlice<'a, T> {
    type EItem = &'a [T];

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, Box<dyn Error>> {
        try_cast_slice(item).map(Cow::Borrowed).map_err(Into::into)
    }
}

impl<'a, T: Pod> BytesDecode<'a> for CowSlice<'_, T> {
    type DItem = Cow<'a, [T]>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error>> {
        match try_cast_slice(bytes) {
            Ok(items) => Ok(Cow::Borrowed(items)),
            Err(PodCastError::AlignmentMismatch) => Ok(Cow::Owned(pod_collect_to_vec(bytes))),
            Err(e) => Err(e.into()),
        }
    }
}

unsafe impl<T> Send for CowSlice<'_, T> {}

unsafe impl<T> Sync for CowSlice<'_, T> {}

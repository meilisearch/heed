use std::borrow::Cow;

use bytemuck::{pod_collect_to_vec, try_cast_slice, AnyBitPattern, NoUninit, PodCastError};
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
pub struct CowSlice<T>(std::marker::PhantomData<T>);

impl<'a, T: NoUninit> BytesEncode<'a> for CowSlice<T> {
    type EItem = [T];
    type Err = PodCastError;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        try_cast_slice(item).map(Cow::Borrowed)
    }
}

impl<'a, T: AnyBitPattern + NoUninit> BytesDecode<'a> for CowSlice<T> {
    type DItem = Cow<'a, [T]>;
    type Err = PodCastError;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err> {
        match try_cast_slice(bytes) {
            Ok(items) => Ok(Cow::Borrowed(items)),
            Err(PodCastError::AlignmentMismatch) => Ok(Cow::Owned(pod_collect_to_vec(bytes))),
            Err(error) => Err(error),
        }
    }
}

unsafe impl<T> Send for CowSlice<T> {}

unsafe impl<T> Sync for CowSlice<T> {}

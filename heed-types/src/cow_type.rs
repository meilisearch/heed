use std::borrow::Cow;
use std::convert::Infallible;

use bytemuck::{bytes_of, bytes_of_mut, try_from_bytes, AnyBitPattern, NoUninit, PodCastError};
use heed_traits::{BytesDecode, BytesEncode};

/// Describes a type that must be [memory aligned] and
/// will be reallocated if it is not.
///
/// A [`Cow`] type is returned to represent this behavior.
///
/// If you need to store a type that doesn't depends on any
/// memory alignment it is recommended to use the [`UnalignedType`].
///
/// If you don't want to be bored with the [`Cow`] type you can
/// use the [`OwnedType`].
///
/// To store slices, you must look at the [`CowSlice`],
/// [`OwnedSlice`] or [`UnalignedSlice`] types.
///
/// [memory aligned]: std::mem::align_of()
/// [`Cow`]: std::borrow::Cow
/// [`UnalignedType`]: crate::UnalignedType
/// [`OwnedType`]: crate::OwnedType
/// [`UnalignedSlice`]: crate::UnalignedSlice
/// [`OwnedSlice`]: crate::OwnedSlice
/// [`CowSlice`]: crate::CowSlice
pub struct CowType<T>(std::marker::PhantomData<T>);

impl<'a, T: NoUninit> BytesEncode<'a> for CowType<T> {
    type EItem = T;
    type Err = Infallible;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<[u8]>, Self::Err> {
        Ok(Cow::Borrowed(bytes_of(item)))
    }
}

impl<'a, T: AnyBitPattern + NoUninit> BytesDecode<'a> for CowType<T> {
    type DItem = Cow<'a, T>;
    type Err = PodCastError;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Self::Err> {
        match try_from_bytes(bytes) {
            Ok(item) => Ok(Cow::Borrowed(item)),
            Err(PodCastError::TargetAlignmentGreaterAndInputNotAligned) => {
                let mut item = T::zeroed();
                bytes_of_mut(&mut item).copy_from_slice(bytes);
                Ok(Cow::Owned(item))
            }
            Err(error) => Err(error),
        }
    }
}

unsafe impl<T> Send for CowType<T> {}

unsafe impl<T> Sync for CowType<T> {}

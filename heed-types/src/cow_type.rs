use std::borrow::Cow;
use std::{mem, ptr};

use crate::aligned_to;
use heed_traits::{BytesDecode, BytesEncode};
use zerocopy::{AsBytes, FromBytes, LayoutVerified};

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

impl<'a, T: 'a> BytesEncode<'a> for CowType<T>
where
    T: AsBytes,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for CowType<T>
where
    T: FromBytes + Copy,
{
    type DItem = Cow<'a, T>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        match LayoutVerified::<_, T>::new(bytes) {
            Some(layout) => Some(Cow::Borrowed(layout.into_ref())),
            None => {
                let len = bytes.len();
                let elem_size = mem::size_of::<T>();

                // ensure that it is the alignment that is wrong
                // and the length is valid
                if len == elem_size && !aligned_to(bytes, mem::align_of::<T>()) {
                    let mut data = mem::MaybeUninit::<T>::uninit();

                    unsafe {
                        let dst = data.as_mut_ptr() as *mut u8;
                        ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                        return Some(Cow::Owned(data.assume_init()));
                    }
                }

                None
            }
        }
    }
}

unsafe impl<T> Send for CowType<T> {}

unsafe impl<T> Sync for CowType<T> {}

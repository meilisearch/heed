use std::borrow::Cow;
use std::{mem, ptr};

use crate::aligned_to;
use heed_traits::{BytesDecode, BytesEncode};
use zerocopy::{AsBytes, FromBytes, LayoutVerified};

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

impl<'a, T: 'a> BytesEncode<'a> for CowSlice<T>
where
    T: AsBytes,
{
    type EItem = [T];

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<[T] as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for CowSlice<T>
where
    T: FromBytes + Copy,
{
    type DItem = Cow<'a, [T]>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        match LayoutVerified::<_, [T]>::new_slice(bytes) {
            Some(layout) => Some(Cow::Borrowed(layout.into_slice())),
            None => {
                let len = bytes.len();
                let elem_size = mem::size_of::<T>();

                // ensure that it is the alignment that is wrong
                // and the length is valid
                if len % elem_size == 0 && !aligned_to(bytes, mem::align_of::<T>()) {
                    let elems = len / elem_size;
                    let mut vec = Vec::<T>::with_capacity(elems);

                    unsafe {
                        let dst = vec.as_mut_ptr() as *mut u8;
                        ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
                        vec.set_len(elems);
                    }

                    return Some(Cow::Owned(vec));
                }

                None
            }
        }
    }
}

unsafe impl<T> Send for CowSlice<T> {}

unsafe impl<T> Sync for CowSlice<T> {}

use std::borrow::Cow;
use std::{mem, ptr};

use zerocopy::{LayoutVerified, AsBytes, FromBytes};
use crate::{BytesEncode, BytesDecode};
use crate::types::aligned_to;

pub struct CowSlice<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for CowSlice<T> where T: AsBytes {
    type EItem = [T];

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<[T] as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for CowSlice<T> where T: FromBytes + Copy {
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

                    return Some(Cow::Owned(vec))
                }

                None
            },
        }
    }
}

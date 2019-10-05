use std::borrow::Cow;
use std::{mem, ptr};

use zerocopy::{LayoutVerified, AsBytes, FromBytes};
use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{BytesEncode, BytesDecode};

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}


pub struct Type<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for Type<T> where T: AsBytes {
    type Item = T;

    fn bytes_encode(item: &Self::Item) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<T> BytesDecode for Type<T> where T: FromBytes + Copy {
    type Item = T;

    fn bytes_decode(bytes: &[u8]) -> Option<Cow<Self::Item>> {
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
                        return Some(Cow::Owned(data.assume_init()))
                    }
                }

                None
            },
        }
    }
}



pub struct Slice<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for Slice<T> where T: AsBytes {
    type Item = [T];

    fn bytes_encode(item: &Self::Item) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<[T] as AsBytes>::as_bytes(item)))
    }
}

impl<T> BytesDecode for Slice<T> where T: FromBytes + Copy {
    type Item = [T];

    fn bytes_decode(bytes: &[u8]) -> Option<Cow<Self::Item>> {
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


pub struct Str;

impl BytesEncode for Str {
    type Item = str;

    fn bytes_encode(item: &Self::Item) -> Option<Cow<[u8]>> {
        Slice::<u8>::bytes_encode(item.as_bytes())
    }
}

impl BytesDecode for Str {
    type Item = str;

    fn bytes_decode(bytes: &[u8]) -> Option<Cow<Self::Item>> {
        std::str::from_utf8(bytes).map(Cow::Borrowed).ok()
    }
}



pub struct Ignore;

impl BytesEncode for Ignore {
    type Item = ();

    fn bytes_encode(item: &Self::Item) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(&[]))
    }
}

impl BytesDecode for Ignore {
    type Item = ();

    fn bytes_decode(bytes: &[u8]) -> Option<Cow<Self::Item>> {
        Some(Cow::Owned(()))
    }
}



pub struct Serde<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for Serde<T> where T: Serialize {
    type Item = T;

    fn bytes_encode(item: &Self::Item) -> Option<Cow<[u8]>> {
        bincode::serialize(item).map(Cow::Owned).ok()
    }
}

impl<T> BytesDecode for Serde<T> where T: DeserializeOwned + Clone {
    type Item = T;

    fn bytes_decode(bytes: &[u8]) -> Option<Cow<Self::Item>> {
        bincode::deserialize(bytes).map(Cow::Owned).ok()
    }
}

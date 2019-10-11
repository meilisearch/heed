use std::borrow::Cow;
use std::{mem, ptr};

use zerocopy::{LayoutVerified, AsBytes, FromBytes};
use crate::{BytesEncode, BytesDecode};

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}


pub struct CowType<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for CowType<T> where T: AsBytes {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for CowType<T> where T: FromBytes + Copy {
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
                        return Some(Cow::Owned(data.assume_init()))
                    }
                }

                None
            },
        }
    }
}




pub struct Type<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for Type<T> where T: AsBytes {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for Type<T> where T: FromBytes {
    type DItem = &'a T;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        LayoutVerified::<_, T>::new(bytes).map(LayoutVerified::into_ref)
    }
}



pub struct OwnedType<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for OwnedType<T> where T: AsBytes {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<T as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for OwnedType<T> where T: FromBytes + Copy {
    type DItem = T;

    fn bytes_decode(bytes: &[u8]) -> Option<Self::DItem> {
        CowType::<T>::bytes_decode(bytes).map(Cow::into_owned)
    }
}





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




pub struct Slice<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for Slice<T> where T: AsBytes {
    type EItem = [T];

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(<[T] as AsBytes>::as_bytes(item)))
    }
}

impl<'a, T: 'a> BytesDecode<'a> for Slice<T> where T: FromBytes {
    type DItem = &'a [T];

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        LayoutVerified::<_, [T]>::new_slice(bytes).map(LayoutVerified::into_slice)
    }
}




pub struct Str;

impl BytesEncode for Str {
    type EItem = str;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        Slice::<u8>::bytes_encode(item.as_bytes())
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        std::str::from_utf8(bytes).ok()
    }
}



pub struct Ignore;

impl BytesEncode for Ignore {
    type EItem = ();

    fn bytes_encode(_item: &Self::EItem) -> Option<Cow<[u8]>> {
        Some(Cow::Borrowed(&[]))
    }
}

impl BytesDecode<'_> for Ignore {
    type DItem = ();

    fn bytes_decode(_bytes: &[u8]) -> Option<Self::DItem> {
        Some(())
    }
}



#[cfg(feature = "serde")]
use serde::{Serialize, Deserialize};


#[cfg(feature = "serde")]
pub struct Serde<T>(std::marker::PhantomData<T>);

#[cfg(feature = "serde")]
impl<T> BytesEncode for Serde<T> where T: Serialize {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        bincode::serialize(item).map(Cow::Owned).ok()
    }
}

#[cfg(feature = "serde")]
impl<'a, T: 'a> BytesDecode<'a> for Serde<T> where T: Deserialize<'a> + Clone {
    type DItem = Cow<'a, T>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        bincode::deserialize(bytes).map(Cow::Owned).ok()
    }
}

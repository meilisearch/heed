use std::{marker, str, borrow::Cow};
use zerocopy::{LayoutVerified, AsBytes, FromBytes};
use crate::{EPAsBytes, EPFromBytes};

pub struct Type<T>(marker::PhantomData<T>);

impl<T: FromBytes + Copy> EPFromBytes for Type<T> {
    type Output = T;

    fn from_bytes(bytes: &[u8]) -> Option<Cow<Self::Output>> {
        LayoutVerified::new(bytes).map(LayoutVerified::into_ref).map(Cow::Borrowed)
    }
}



pub struct Slice<T>(marker::PhantomData<T>);

impl<T: FromBytes + Copy> EPFromBytes for Slice<T> {
    type Output = [T];

    fn from_bytes(bytes: &[u8]) -> Option<Cow<Self::Output>> {
        LayoutVerified::new_slice(bytes).map(LayoutVerified::into_slice).map(Cow::Borrowed)
    }
}



pub struct Str;

impl EPFromBytes for Str {
    type Output = str;

    fn from_bytes(bytes: &[u8]) -> Option<Cow<Self::Output>> {
        str::from_utf8(bytes).map(Cow::Borrowed).ok()
    }
}



pub struct Ignore;

impl EPFromBytes for Ignore {
    type Output = ();

    fn from_bytes(bytes: &[u8]) -> Option<Cow<Self::Output>> {
        Some(Cow::Owned(()))
    }
}

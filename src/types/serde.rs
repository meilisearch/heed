use std::borrow::Cow;
use serde::{Serialize, Deserialize};
use crate::{BytesEncode, BytesDecode};

pub struct Serde<T>(std::marker::PhantomData<T>);

impl<T> BytesEncode for Serde<T> where T: Serialize {
    type EItem = T;

    fn bytes_encode(item: &Self::EItem) -> Option<Cow<[u8]>> {
        bincode::serialize(item).map(Cow::Owned).ok()
    }
}

impl<'a, T: 'a> BytesDecode<'a> for Serde<T> where T: Deserialize<'a> + Clone {
    type DItem = Cow<'a, T>;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        bincode::deserialize(bytes).map(Cow::Owned).ok()
    }
}

use std::borrow::Cow;
use zerocopy::{LayoutVerified, AsBytes, FromBytes};
use serde::{Serialize, Deserialize, de::DeserializeOwned};

pub trait BytesEncode {
    type Item: ?Sized;

    fn bytes_encode(item: &Self::Item) -> Option<Cow<[u8]>>;
}

pub trait BytesDecode {
    type Item: ToOwned + ?Sized;

    fn bytes_decode(bytes: &[u8]) -> Option<Cow<Self::Item>>;
}

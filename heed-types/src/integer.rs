use std::convert::Infallible;
use std::marker::PhantomData;
use std::mem::size_of;

use byteorder::{ByteOrder, ReadBytesExt};
use heed_traits::{BoxedError, BytesDecode, BytesEncode};

/// Encodable version of [`u8`].
pub struct U8;

impl BytesEncode<'_> for U8 {
    type EItem = u8;

    type ReturnBytes = [u8; 1];

    type Error = Infallible;

    fn bytes_encode(item: &Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        Ok([*item])
    }
}

impl BytesDecode<'_> for U8 {
    type DItem = u8;

    fn bytes_decode(mut bytes: &'_ [u8]) -> Result<Self::DItem, BoxedError> {
        bytes.read_u8().map_err(Into::into)
    }
}

/// Encodable version of [`i8`].
pub struct I8;

impl BytesEncode<'_> for I8 {
    type EItem = i8;

    type ReturnBytes = [u8; 1];

    type Error = Infallible;

    fn bytes_encode(item: &Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
        Ok([*item as u8])
    }
}

impl BytesDecode<'_> for I8 {
    type DItem = i8;

    fn bytes_decode(mut bytes: &'_ [u8]) -> Result<Self::DItem, BoxedError> {
        bytes.read_i8().map_err(Into::into)
    }
}

macro_rules! define_type {
    ($name:ident, $native:ident, $read_method:ident, $write_method:ident) => {
        #[doc = "Encodable version of [`"]
        #[doc = stringify!($native)]
        #[doc = "`]."]

        pub struct $name<O>(PhantomData<O>);

        impl<O: ByteOrder> BytesEncode<'_> for $name<O> {
            type EItem = $native;

            type ReturnBytes = [u8; size_of::<$native>()];

            type Error = Infallible;

            fn bytes_encode(item: &Self::EItem) -> Result<Self::ReturnBytes, Self::Error> {
                let mut buf = [0; size_of::<$native>()];
                O::$write_method(&mut buf, *item);
                Ok(buf)
            }
        }

        impl<O: ByteOrder> BytesDecode<'_> for $name<O> {
            type DItem = $native;

            fn bytes_decode(mut bytes: &'_ [u8]) -> Result<Self::DItem, BoxedError> {
                bytes.$read_method::<O>().map_err(Into::into)
            }
        }
    };
}

define_type!(U16, u16, read_u16, write_u16);
define_type!(U32, u32, read_u32, write_u32);
define_type!(U64, u64, read_u64, write_u64);
define_type!(U128, u128, read_u128, write_u128);
define_type!(I16, i16, read_i16, write_i16);
define_type!(I32, i32, read_i32, write_i32);
define_type!(I64, i64, read_i64, write_i64);
define_type!(I128, i128, read_i128, write_i128);

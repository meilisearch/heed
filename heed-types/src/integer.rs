// Copyright 2019 The Fuchsia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the Fushia_LICENSE file.

//! Byte order-aware numeric primitives.
//!
//! This module contains equivalents of the native multi-byte integer types with
//! no alignment requirement and supporting byte order conversions.
//!
//! For each native multi-byte integer type - `u16`, `i16`, `u32`, etc - an
//! equivalent type is defined by this module - [`U16`], [`I16`], [`U32`], etc.
//! Unlike their native counterparts, these types have alignment 1, and take a
//! type parameter specifying the byte order in which the bytes are stored in
//! memory. Each type implements the [`Zeroable`], and [`Pod`] traits.
//!
//! These two properties, taken together, make these types very useful for
//! defining data structures whose memory layout matches a wire format such as
//! that of a network protocol or a file format. Such formats often have
//! multi-byte values at offsets that do not respect the alignment requirements
//! of the equivalent native types, and stored in a byte order not necessarily
//! the same as that of the target platform.

use std::fmt::{self, Binary, Debug, Display, Formatter, LowerHex, Octal, UpperHex};
use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable};
use byteorder::ByteOrder;

macro_rules! impl_fmt_trait {
    ($name:ident, $native:ident, $trait:ident) => {
        impl<O: ByteOrder> $trait for $name<O> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                $trait::fmt(&self.get(), f)
            }
        }
    };
}

macro_rules! doc_comment {
    ($x:expr, $($tt:tt)*) => {
        #[doc = $x]
        $($tt)*
    };
}

macro_rules! define_type {
    ($article:ident, $name:ident, $native:ident, $bits:expr, $bytes:expr, $read_method:ident, $write_method:ident, $sign:ident) => {
        doc_comment! {
            concat!("A ", stringify!($bits), "-bit ", stringify!($sign), " integer
stored in `O` byte order.

`", stringify!($name), "` is like the native `", stringify!($native), "` type with
two major differences: First, it has no alignment requirement (its alignment is 1).
Second, the endianness of its memory layout is given by the type parameter `O`.

", stringify!($article), " `", stringify!($name), "` can be constructed using
the [`new`] method, and its contained value can be obtained as a native
`",stringify!($native), "` using the [`get`] method, or updated in place with
the [`set`] method. In all cases, if the endianness `O` is not the same as the
endianness of the current platform, an endianness swap will be performed in
order to uphold the invariants that a) the layout of `", stringify!($name), "`
has endianness `O` and that, b) the layout of `", stringify!($native), "` has
the platform's native endianness.

`", stringify!($name), "` implements [`Zeroable`], and [`Pod`],
making it useful for parsing and serialization.

[`new`]: crate::integer::", stringify!($name), "::new
[`get`]: crate::integer::", stringify!($name), "::get
[`set`]: crate::integer::", stringify!($name), "::set
[`Zeroable`]: bytemuck::Zeroable
[`Pod`]: bytemuck::Pod"),
            #[derive(Default, Copy, Clone, Eq, PartialEq, Hash)]
            #[repr(transparent)]
            pub struct $name<O: ByteOrder>([u8; $bytes], PhantomData<O>);
        }

        unsafe impl<O: ByteOrder> Zeroable for $name<O> {
            fn zeroed() -> $name<O> {
                $name([0u8; $bytes], PhantomData)
            }
        }

        unsafe impl<O: 'static + ByteOrder> Pod for $name<O> {}

        impl<O: ByteOrder> $name<O> {
            // TODO(joshlf): Make these const fns if the ByteOrder methods ever
            // become const fns.

            /// Constructs a new value, possibly performing an endianness swap
            /// to guarantee that the returned value has endianness `O`.
            pub fn new(n: $native) -> $name<O> {
                let mut out = $name::default();
                O::$write_method(&mut out.0[..], n);
                out
            }

            /// Returns the value as a primitive type, possibly performing an
            /// endianness swap to guarantee that the return value has the
            /// endianness of the native platform.
            pub fn get(self) -> $native {
                O::$read_method(&self.0[..])
            }

            /// Updates the value in place as a primitive type, possibly
            /// performing an endianness swap to guarantee that the stored value
            /// has the endianness `O`.
            pub fn set(&mut self, n: $native) {
                O::$write_method(&mut self.0[..], n);
            }
        }

        // NOTE: The reasoning behind which traits to implement here is a) only
        // implement traits which do not involve implicit endianness swaps and,
        // b) only implement traits which won't cause inference issues. Most of
        // the traits which would cause inference issues would also involve
        // endianness swaps anyway (like comparison/ordering with the native
        // representation or conversion from/to that representation). Note that
        // we make an exception for the format traits since the cost of
        // formatting dwarfs cost of performing an endianness swap, and they're
        // very useful.

        impl<O: ByteOrder> From<$name<O>> for [u8; $bytes] {
            fn from(x: $name<O>) -> [u8; $bytes] {
                x.0
            }
        }

        impl<O: ByteOrder> From<[u8; $bytes]> for $name<O> {
            fn from(bytes: [u8; $bytes]) -> $name<O> {
                $name(bytes, PhantomData)
            }
        }

        impl<O: ByteOrder> AsRef<[u8; $bytes]> for $name<O> {
            fn as_ref(&self) -> &[u8; $bytes] {
                &self.0
            }
        }

        impl<O: ByteOrder> AsMut<[u8; $bytes]> for $name<O> {
            fn as_mut(&mut self) -> &mut [u8; $bytes] {
                &mut self.0
            }
        }

        impl<O: ByteOrder> PartialEq<$name<O>> for [u8; $bytes] {
            fn eq(&self, other: &$name<O>) -> bool {
                self.eq(&other.0)
            }
        }

        impl<O: ByteOrder> PartialEq<[u8; $bytes]> for $name<O> {
            fn eq(&self, other: &[u8; $bytes]) -> bool {
                self.0.eq(other)
            }
        }

        impl_fmt_trait!($name, $native, Display);
        impl_fmt_trait!($name, $native, Octal);
        impl_fmt_trait!($name, $native, LowerHex);
        impl_fmt_trait!($name, $native, UpperHex);
        impl_fmt_trait!($name, $native, Binary);

        impl<O: ByteOrder> Debug for $name<O> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                // This results in a format like "U16(42)"
                write!(f, concat!(stringify!($name), "({})"), self.get())
            }
        }
    };
}

define_type!(A, U16, u16, 16, 2, read_u16, write_u16, unsigned);
define_type!(A, U32, u32, 32, 4, read_u32, write_u32, unsigned);
define_type!(A, U64, u64, 64, 8, read_u64, write_u64, unsigned);
define_type!(A, U128, u128, 128, 16, read_u128, write_u128, unsigned);
define_type!(An, I16, i16, 16, 2, read_i16, write_i16, signed);
define_type!(An, I32, i32, 32, 4, read_i32, write_i32, signed);
define_type!(An, I64, i64, 64, 8, read_i64, write_i64, signed);
define_type!(An, I128, i128, 128, 16, read_i128, write_i128, signed);

#[cfg(test)]
mod tests {
    use bytemuck::{bytes_of, bytes_of_mut, Pod};
    use byteorder::NativeEndian;

    use super::*;

    // A native integer type (u16, i32, etc)
    trait Native: Pod + Copy + Eq + Debug {
        fn rand() -> Self;
    }

    trait ByteArray: Pod + Copy + AsRef<[u8]> + AsMut<[u8]> + Debug + Default + Eq {
        /// Invert the order of the bytes in the array.
        fn invert(self) -> Self;
    }

    trait ByteOrderType: Pod + Copy + Eq + Debug {
        type Native: Native;
        type ByteArray: ByteArray;

        fn new(native: Self::Native) -> Self;
        fn get(self) -> Self::Native;
        fn set(&mut self, native: Self::Native);
        fn from_bytes(bytes: Self::ByteArray) -> Self;
        fn into_bytes(self) -> Self::ByteArray;
    }

    macro_rules! impl_byte_array {
        ($bytes:expr) => {
            impl ByteArray for [u8; $bytes] {
                fn invert(mut self) -> [u8; $bytes] {
                    self.reverse();
                    self
                }
            }
        };
    }

    impl_byte_array!(2);
    impl_byte_array!(4);
    impl_byte_array!(8);
    impl_byte_array!(16);

    macro_rules! impl_traits {
        ($name:ident, $native:ident, $bytes:expr, $sign:ident) => {
            impl Native for $native {
                fn rand() -> $native {
                    rand::random()
                }
            }

            impl<O: 'static + ByteOrder> ByteOrderType for $name<O> {
                type Native = $native;
                type ByteArray = [u8; $bytes];

                fn new(native: $native) -> $name<O> {
                    $name::new(native)
                }

                fn get(self) -> $native {
                    $name::get(self)
                }

                fn set(&mut self, native: $native) {
                    $name::set(self, native)
                }

                fn from_bytes(bytes: [u8; $bytes]) -> $name<O> {
                    $name::from(bytes)
                }

                fn into_bytes(self) -> [u8; $bytes] {
                    <[u8; $bytes]>::from(self)
                }
            }
        };
    }

    impl_traits!(U16, u16, 2, unsigned);
    impl_traits!(U32, u32, 4, unsigned);
    impl_traits!(U64, u64, 8, unsigned);
    impl_traits!(U128, u128, 16, unsigned);
    impl_traits!(I16, i16, 2, signed);
    impl_traits!(I32, i32, 4, signed);
    impl_traits!(I64, i64, 8, signed);
    impl_traits!(I128, i128, 16, signed);

    macro_rules! call_for_all_types {
        ($fn:ident, $byteorder:ident) => {
            $fn::<U16<$byteorder>>();
            $fn::<U32<$byteorder>>();
            $fn::<U64<$byteorder>>();
            $fn::<U128<$byteorder>>();
            $fn::<I16<$byteorder>>();
            $fn::<I32<$byteorder>>();
            $fn::<I64<$byteorder>>();
            $fn::<I128<$byteorder>>();
        };
    }

    #[cfg(target_endian = "big")]
    type NonNativeEndian = byteorder::LittleEndian;
    #[cfg(target_endian = "little")]
    type NonNativeEndian = byteorder::BigEndian;

    #[test]
    fn test_native_endian() {
        fn test_native_endian<T: ByteOrderType>() {
            for _ in 0..1024 {
                let native = T::Native::rand();
                let mut bytes = T::ByteArray::default();
                bytes_of_mut(&mut bytes).copy_from_slice(bytes_of(&native));
                let mut from_native = T::new(native);
                let from_bytes = T::from_bytes(bytes);
                assert_eq!(from_native, from_bytes);
                assert_eq!(from_native.get(), native);
                assert_eq!(from_bytes.get(), native);
                assert_eq!(from_native.into_bytes(), bytes);
                assert_eq!(from_bytes.into_bytes(), bytes);

                let updated = T::Native::rand();
                from_native.set(updated);
                assert_eq!(from_native.get(), updated);
            }
        }

        call_for_all_types!(test_native_endian, NativeEndian);
    }

    #[test]
    fn test_non_native_endian() {
        fn test_non_native_endian<T: ByteOrderType>() {
            for _ in 0..1024 {
                let native = T::Native::rand();
                let mut bytes = T::ByteArray::default();
                bytes_of_mut(&mut bytes).copy_from_slice(bytes_of(&native));
                bytes = bytes.invert();
                let mut from_native = T::new(native);
                let from_bytes = T::from_bytes(bytes);
                assert_eq!(from_native, from_bytes);
                assert_eq!(from_native.get(), native);
                assert_eq!(from_bytes.get(), native);
                assert_eq!(from_native.into_bytes(), bytes);
                assert_eq!(from_bytes.into_bytes(), bytes);

                let updated = T::Native::rand();
                from_native.set(updated);
                assert_eq!(from_native.get(), updated);
            }
        }

        call_for_all_types!(test_non_native_endian, NonNativeEndian);
    }
}

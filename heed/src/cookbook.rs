//! A cookbook of examples on how to use heed.
//!
//! # Implement a custom codec with `BytesEncode`/`BytesDecode`
//!
//! With heed you can store any kind of data and serialize it the way you want.
//! To do so you'll need to create a codec by usin the [`BytesEncode`] and [`BytesDecode`] traits.
//!
//! ```
//! use std::borrow::Cow;
//! use heed::{BoxedError, BytesEncode, BytesDecode};
//!
//! pub enum MyCounter<'a> {
//!   One,
//!   Two,
//!   WhatIsThat(&'a [u8]),
//! }
//!
//! pub struct MyCounterCodec;
//!
//! impl<'a> BytesEncode<'a> for MyCounterCodec {
//!     type EItem = MyCounter<'a>;
//!
//!     fn bytes_encode(my_counter: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
//!         let mut output = Vec::new();
//!
//!         match my_counter {
//!             MyCounter::One => output.push(1),
//!             MyCounter::Two => output.push(2),
//!             MyCounter::WhatIsThat(bytes) => {
//!                 output.push(u8::MAX);
//!                 output.extend_from_slice(bytes);
//!             },
//!         }
//!
//!         Ok(Cow::Owned(output))
//!     }
//! }
//!
//! impl<'a> BytesDecode<'a> for MyCounterCodec {
//!     type DItem = MyCounter<'a>;
//!
//!     fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
//!         match bytes[0] {
//!             1 => Ok(MyCounter::One),
//!             2 => Ok(MyCounter::One),
//!             u8::MAX => Ok(MyCounter::WhatIsThat(&bytes[1..])),
//!             _ => Err("invalid input".into()),
//!         }
//!     }
//! }
//! ```
//!
//!
//!
//!
//!
//!
//!
//!
//!
//!
//!
//!
//!
//!
//!

// To let cargo generate doc links
#![allow(unused_imports)]

use crate::{BytesDecode, BytesEncode};

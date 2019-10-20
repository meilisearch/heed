//!
//! How to choose the right type to store things in this database?
//! For specific types you can choose:
//!   - [`Str`] to store [`str`]s
//!   - [`Unit`] to store [unit `()`] types
//!   - or [`SerdeBincode`] or [`SerdeJson`] to store [`Serialize`]/[`Deserialize`] types
//!
//! But if you want to store big types that can be efficiently deserialized then
//! here is a little table to help you in your quest:
//!
//! | Available types    | Encoding type | Decoding type | allocations                                              |
//! |--------------------|:-------------:|:-------------:|----------------------------------------------------------|
//! | [`CowSlice`]       | `&[T]`        | `Cow<[T]>`    | will allocate if memory is miss-aligned                  |
//! | [`CowType`]        | `&T`          | `Cow<T>`      | will allocate if memory is miss-aligned                  |
//! | [`OwnedSlice`]     | `&[T]`        | `Vec<T>`      | will _always_ allocate                                   |
//! | [`OwnedType`]      | `&T`          | `T`           | will _always_ allocate                                   |
//! | [`UnalignedSlice`] | `&[T]`        | `&[T]`        | will _never_ allocate because alignement is always valid |
//! | [`UnalignedType`]  | `&T`          | `&T`          | will _never_ allocate because alignement is always valid |
//!
//! Note that **all** those types above must implement [`AsBytes`] and [`FromBytes`]. <br/>
//! The `UnalignedSlice/Type` types also need to implement the [`Unaligned`] trait.
//!
//! If you don't want to/cannot deal with `AsBytes`, `Frombytes` or `Unaligned` requirements
//! we recommend you to use the `SerdeBincode` or `SerdeJson` types and deal with the `Serialize`/`Deserialize` traits.
//!
//! [`AsBytes`]: zerocopy::AsBytes
//! [`FromBytes`]: zerocopy::FromBytes
//! [`Unaligned`]: zerocopy::Unaligned
//!
//! [`Str`]: crate::types::Str
//! [unit `()`]: https://doc.rust-lang.org/std/primitive.unit.html
//! [`Unit`]: crate::types::Unit
//! [`SerdeBincode`]: crate::types::SerdeBincode
//! [`SerdeJson`]: crate::types::SerdeJson
//! [`Serialize`]: serde::Serialize
//! [`Deserialize`]: serde::Deserialize
//!
//! [`CowSlice`]: crate::types::CowSlice
//! [`CowType`]: crate::types::CowType
//! [`OwnedSlice`]: crate::types::OwnedSlice
//! [`OwnedType`]: crate::types::OwnedType
//! [`UnalignedSlice`]: crate::types::UnalignedSlice
//! [`UnalignedType`]: crate::types::UnalignedType

mod cow_slice;
mod cow_type;
mod unit;
mod owned_slice;
mod owned_type;
mod str;
mod unaligned_slice;
mod unaligned_type;

#[cfg(feature = "serde-bincode")]
mod serde_bincode;

#[cfg(feature = "serde-json")]
mod serde_json;

pub use self::cow_slice::CowSlice;
pub use self::cow_type::CowType;
pub use self::unit::Unit;
pub use self::owned_slice::OwnedSlice;
pub use self::owned_type::OwnedType;
pub use self::str::Str;
pub use self::unaligned_slice::UnalignedSlice;
pub use self::unaligned_type::UnalignedType;

/// Describes a slice of bytes `[u8]` that is totally
/// borrowed and doesn't depends on any [memory alignment].
///
/// [memory alignment]: https://doc.rust-lang.org/std/mem/fn.align_of.html
pub type ByteSlice = UnalignedSlice<u8>;

#[cfg(feature = "serde-bincode")]
pub use self::serde_bincode::SerdeBincode;

#[cfg(feature = "serde-json")]
pub use self::serde_json::SerdeJson;

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

mod cow_slice;
mod cow_type;
mod ignore;
mod owned_slice;
mod owned_type;
mod str;
mod unaligned_slice;
mod unaligned_type;

#[cfg(feature = "serde")]
mod serde;

pub use self::cow_slice::CowSlice;
pub use self::cow_type::CowType;
pub use self::ignore::Ignore;
pub use self::owned_slice::OwnedSlice;
pub use self::owned_type::OwnedType;
pub use self::str::Str;
pub use self::unaligned_slice::UnalignedSlice;
pub use self::unaligned_type::UnalignedType;

pub type ByteSlice = UnalignedSlice<u8>;

#[cfg(feature = "serde")]
pub use self::serde::Serde;

fn aligned_to(bytes: &[u8], align: usize) -> bool {
    (bytes as *const _ as *const () as usize) % align == 0
}

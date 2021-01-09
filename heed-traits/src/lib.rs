use std::borrow::Cow;
use std::cmp::Ordering;

pub trait BytesEncode<'a> {
    type EItem: ?Sized + 'a;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>>;
}

pub trait BytesDecode<'a> {
    type DItem: 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem>;
}

/// Define a custom key comparison function for a database.
///
/// The comparison function is called whenever it is necessary to compare a key specified
/// by the application with a key currently stored in the database. If no comparison function
/// is specified, and no special key flags were specified, the keys are compared lexically,
/// with shorter keys collating before longer keys.
pub trait CustomKeyCmp {
    /// Compares the raw bytes representation of two keys.
    ///
    /// # Safety
    ///
    /// This function must never crash, this is the reason why it takes raw bytes as parameter,
    /// to let you define the recovery method you want in case of a decoding error.
    fn compare(a: &[u8], b: &[u8]) -> Ordering;
}

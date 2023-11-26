use heed_traits::BoxedError;

/// A convenient struct made to ignore the type when decoding it.
///
/// It is appropriate to be used to count keys for example
/// or to ensure that an entry exist for example.
pub enum DecodeIgnore {}

impl heed_traits::BytesDecode<'_> for DecodeIgnore {
    type DItem = ();

    fn bytes_decode(_bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Ok(())
    }
}

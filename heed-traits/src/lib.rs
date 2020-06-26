use std::borrow::Cow;
use std::mem;

pub trait BytesEncode<'a> {
    type EItem: ?Sized + 'a;

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>>;
}

pub trait BytesDecode<'a> {
    type DItem: 'a;

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem>;
}

impl<'a, A, B> BytesEncode<'a> for (A, B)
where
    A: BytesEncode<'a>,
    A::EItem: Sized,
    B: BytesEncode<'a>,
{
    type EItem = (A::EItem, B::EItem);

    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<[u8]>> {
        let first_bytes = A::bytes_encode(&item.0)?;
        let second_bytes = B::bytes_encode(&item.1)?;
        let mut buffer = Vec::with_capacity(
            mem::size_of::<usize>()
            + &first_bytes.len()
            + &second_bytes.len());
        // signify the length of the first item
        buffer.extend_from_slice(&first_bytes.len().to_be_bytes());
        buffer.extend_from_slice(&first_bytes);
        buffer.extend_from_slice(&second_bytes);
        Some(Cow::Owned(buffer))
    }
}

impl<'a, A, B> BytesDecode<'a> for (A, B)
where
    A: BytesDecode<'a>,
    B: BytesDecode<'a>,
{
    type DItem = (A::DItem, B::DItem);

    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let mut size_buf = [0; 8];
        size_buf.copy_from_slice(bytes.get(0..8)?);
        // decode size of the first item from the bytes
        let first_size = usize::from_be_bytes(size_buf);
        let first_item = A::bytes_decode(bytes.get(8..(8 + first_size))?)?;
        let second_item = B::bytes_decode(bytes.get((8 + first_size)..)?)?;
        Some((first_item, second_item))
    }
}

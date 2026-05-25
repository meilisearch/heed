use std::error::Error;

use heed::types::Str;
use heed::{Database, EnvOpenOptions};
use rand::{Rng, RngCore, SeedableRng};

struct Seed(u64);

impl Seed {
    fn rng(&self) -> rand::rngs::StdRng {
        rand::rngs::StdRng::seed_from_u64(self.0)
    }

    fn length(&self) -> usize {
        Self::length_with_rng(&mut self.rng())
    }

    fn length_with_rng(rng: &mut rand::rngs::StdRng) -> usize {
        rng.random_range(0..40_000) * std::mem::size_of::<u32>()
    }
}

struct SeedCodec;

impl<'a> heed::BytesEncode<'a> for SeedCodec {
    type EItem = Seed;

    fn writer_size_hint(item: &'a Self::EItem) -> Option<usize> {
        Some(item.length())
    }

    fn bytes_encode(item: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>, heed::BoxedError> {
        let mut rng = item.rng();
        let length: usize = Seed::length_with_rng(&mut rng);
        let count = length / std::mem::size_of::<u32>();
        let mut vec = Vec::<u8>::with_capacity(length);

        for _ in 0..count {
            vec.extend_from_slice(&rng.next_u32().to_ne_bytes()[..]);
        }

        Ok(std::borrow::Cow::Owned(vec))
    }

    fn bytes_encode_into_writer<W: std::io::Write>(
        item: &'a Self::EItem,
        mut writer: W,
    ) -> Result<(), heed::BoxedError> {
        let mut rng = item.rng();
        let count: usize = Seed::length_with_rng(&mut rng) / std::mem::size_of::<u32>();

        for _ in 0..count {
            let byte = rng.next_u32();
            writer.write_all(&byte.to_le_bytes()).unwrap();
        }

        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = tempfile::tempdir()?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(1000 * 1024 * 1024) // 10MB
            .open(path)?
    };

    let mut wtxn = env.write_txn()?;
    let db: Database<Str, SeedCodec> = env.open_database(&mut wtxn, None)?.unwrap();

    for i in 0..2500 {
        let seed = Seed(i);
        db.put(&mut wtxn, &i.to_string(), &seed)?;
    }

    wtxn.commit()?;

    Ok(())
}

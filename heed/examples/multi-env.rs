use std::error::Error;

use byteorder::BE;
use heed::types::*;
use heed::{AbortOrCommit, Database, Env, EnvOpenOptions, OnCommit, RwTxn};

type BEU32 = U32<BE>;

fn main() -> Result<(), Box<dyn Error>> {
    let env1_path = tempfile::tempdir()?;
    let env1 = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(env1_path)?
    };

    let env2_path = tempfile::tempdir().unwrap();
    let env2 = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(env2_path)
            .unwrap()
    };

    let mut wtxn1 = env1.write_txn()?;
    let mut wtxn2 = env2.write_txn()?;

    let db: Database<'static, Str, Bytes> = env1
        .create_and_commit_databases(wtxn1, |xxx| {
            let database = xxx.create_database(Some("hello"))?;
            Ok(AbortOrCommit::Commit(Kiki { env2: &env2, wtxn2, db: database }))
        })?
        .unwrap_commit();

    struct Kiki<'e2, 't, KC, DC, T> {
        env2: &'e2 Env<T>,
        wtxn2: RwTxn<'e2>,
        db: Database<'t, KC, DC>,
    }

    impl<'e2, 't, KC, DC, T> OnCommit for Kiki<'e2, 't, KC, DC, T>
    where
        KC: 'static,
        DC: 'static,
    {
        type Committed = Database<'static, KC, DC>;

        fn on_commit<'l>(self, token: &heed::CommitToken<'l>) -> Self::Committed
        where
            Self: 'l,
        {
            let mut validate_lol = None;
            let _ = self
                .env2
                .create_and_commit_databases(self.wtxn2, |xxx| {
                    let database = xxx.create_database(Some("cool"))?;
                    validate_lol = Some(database.on_commit(&token));
                    Ok(AbortOrCommit::<Database<Unit, Unit>>::Abort)
                })
                .unwrap();

            validate_lol.unwrap()
        }
    }

    // let db2: Database<BEU32, BEU32> = env2.create_database(&mut wtxn2, Some("hello"))?;

    // // clear db
    // db1.clear(&mut wtxn1)?;
    // wtxn1.commit()?;

    // // clear db
    // db2.clear(&mut wtxn2)?;
    // wtxn2.commit()?;

    // // -----

    // let mut wtxn1 = env1.write_txn()?;

    // db1.put(&mut wtxn1, "what", &[4, 5][..])?;
    // db1.get(&wtxn1, "what")?;
    // wtxn1.commit()?;

    // let rtxn2 = env2.read_txn()?;
    // let ret = db2.last(&rtxn2)?;
    // assert_eq!(ret, None);

    Ok(())
}

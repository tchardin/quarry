use anyhow::Result;
use bincode::{deserialize, serialize};
use cid::{
    multihash::{Code, MultihashDigest},
    Cid,
};
use marble::Marble;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

type ObjectId = u64;

const INDEX_OBJECT_ID: ObjectId = 1;

#[derive(Serialize, Deserialize, Debug)]
struct Index {
    pages: BTreeMap<Vec<u8>, ObjectId>,
    last_pid: u64,
}

impl Default for Index {
    fn default() -> Self {
        Index {
            pages: Default::default(),
            last_pid: INDEX_OBJECT_ID + 1,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Page {
    hi: Option<Vec<u8>>,
    lo: Vec<u8>,
    kvs: BTreeMap<Vec<u8>, Vec<u8>>,
}

pub struct Quarry {
    heap: Marble,
    index: Index,
}

impl Quarry {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Quarry> {
        let heap = marble::open(path)?;

        let index: Index = if let Some(data) = heap.read(INDEX_OBJECT_ID)? {
            deserialize(&data)?
        } else {
            Index::default()
        };

        let mut qry = Quarry { index, heap };

        if qry.index.pages.is_empty() {
            let init_page = Page {
                hi: None,
                lo: vec![],
                kvs: BTreeMap::new(),
            };

            qry.allocate_page(init_page)?;
        }

        Ok(qry)
    }

    fn allocate_page(&mut self, page: Page) -> Result<()> {
        self.index.last_pid += 1;
        let object_id = self.index.last_pid;

        let previous = self.index.pages.insert(page.lo.clone(), object_id);
        assert!(previous.is_none());

        let batch: HashMap<ObjectId, Option<Vec<u8>>> = [
            (object_id, Some(serialize(&page)?)),
            (INDEX_OBJECT_ID, Some(serialize(&self.index)?)),
        ]
        .into_iter()
        .collect();

        self.heap.write_batch(batch)?;

        Ok(())
    }

    fn pid_for_key(&self, key: Vec<u8>) -> ObjectId {
        *self.index.pages.range(..=key).next_back().unwrap().1
    }

    fn mutate(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        let object_id = self.pid_for_key(key.clone());
        let leaf_data = self.heap.read(object_id)?.unwrap();
        let mut leaf: Page = deserialize(&leaf_data)?;
        let ret = if let Some(v) = value {
            // TODO Page split logic when it becomes large
            leaf.kvs.insert(key, v)
        } else {
            // TODO Page merge logic when it becomes small
            leaf.kvs.remove(&key)
        };

        let write_batch = [(object_id, Some(serialize(&leaf).unwrap()))];

        self.heap.write_batch(write_batch)?;

        let stats = self.heap.stats();

        if stats.dead_objects > stats.live_objects {
            self.heap.maintenance()?;
        }

        Ok(ret)
    }
}

impl Blockstore for Quarry {
    fn delete_block(&self, k: &Cid) -> Result<()> {
        let kd = k.to_bytes();
        self.mutate(kd, None)?;
        Ok(())
    }
    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>> {
        let kd = k.to_bytes();
        let object_id = self.pid_for_key(kd.clone());
        let page_data = self.heap.read(object_id)?.unwrap();
        let page: Page = deserialize(&page_data)?;
        Ok(page.kvs.get(&kd).cloned())
    }
    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
        let kd = k.to_bytes();
        self.mutate(kd, Some(block.to_vec()))?;
        Ok(())
    }
}

/// Layer of abstraction for block-centered methods over a datastore.
pub trait Blockstore {
    /// Delete a block from the blockstore.
    fn delete_block(&self, k: &Cid) -> Result<()>;

    /// Gets the block from the blockstore.
    fn get(&self, k: &Cid) -> Result<Option<Vec<u8>>>;

    /// Put a block with a pre-computed cid.
    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()>;

    /// Checks if the blockstore has the specified block.
    fn has(&self, k: &Cid) -> Result<bool> {
        Ok(self.get(k)?.is_some())
    }

    /// Bulk-put pre-keyed blocks into the blockstore.
    ///
    /// By default, this defers to put_keyed.
    fn put_many_keyed<D, I>(&self, blocks: I) -> Result<()>
    where
        Self: Sized,
        D: AsRef<[u8]>,
        I: IntoIterator<Item = (Cid, D)>,
    {
        for (c, b) in blocks {
            self.put_keyed(&c, b.as_ref())?
        }
        Ok(())
    }
}

pub trait Buffered: Blockstore {
    fn flush(&self, root: &Cid) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

    const TEST_DIR: &str = "test_dir";

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn with_instance<F: FnOnce(Quarry)>(f: F) {
        let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, SeqCst));
        let path = std::path::Path::new(TEST_DIR).join(subdir);

        let _ = fs::remove_dir_all(&path);

        let quarry = Quarry::open(&path).unwrap();

        f(quarry);

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn put_get() {
        with_instance(|quarry| {
            let content: &[u8; 17] = b"morrocan mint tea";
            let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(content));

            quarry.put_keyed(&cid, content).unwrap();

            let result = quarry.get(&cid).unwrap();
            assert_eq!(result, Some(content.to_vec()));
        });
    }
}

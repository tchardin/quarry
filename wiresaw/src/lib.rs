use anyhow::Result;
use cid::{
    multihash::{Code, Multihash, MultihashDigest},
    Cid,
};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub const DAG_CBOR: u64 = 0x71;

const DEFAULT_CHUNK_SIZE: usize = 1 << 18;

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    #[serde(with = "serde_bytes", skip_serializing_if = "Option::is_none")]
    data: Option<Vec<u8>>,
    links: Vec<Link>,
}

impl Node {
    pub fn with_links_cap(size: usize) -> Node {
        Node {
            data: None,
            links: Vec::with_capacity(size),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Link {
    cid: Cid,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
}

impl From<Cid> for Link {
    fn from(cid: Cid) -> Link {
        Link {
            cid,
            name: None,
            size: None,
        }
    }
}

pub struct ChunkReader<R> {
    inner: R,
    content_size: u64,
    chunk_size: usize,
    rem_size: u64,
}

impl<R: Read> ChunkReader<R> {
    /// Creates a new `ChunkReader<R>` with a default chunk size.
    pub fn new(inner: R) -> ChunkReader<R> {
        ChunkReader::with_chunk_size(DEFAULT_CHUNK_SIZE, inner)
    }

    /// Creates a new `ChunkReader<R>` with a given chunk size.
    pub fn with_chunk_size(size: usize, inner: R) -> ChunkReader<R> {
        ChunkReader {
            inner,
            chunk_size: size,
            content_size: 0,
            rem_size: 0,
        }
    }

    /// Changes the chunk size of the reader.
    pub fn set_chunk_size(&mut self, size: usize) {
        self.chunk_size = size;
    }

    /// Changes the content size.
    pub fn set_content_size(&mut self, size: u64) {
        self.content_size = size;
        self.rem_size = size;
    }
}

impl<R: Read> ChunkReader<R> {
    /// Opens a chunk reader from a file path.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<ChunkReader<File>> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let mut r = ChunkReader::new(file);
        r.set_content_size(metadata.len());
        Ok(r)
    }
}

impl<R: Read> Iterator for ChunkReader<R> {
    type Item = Vec<u8>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = vec![0u8; self.chunk_size];

        if let Ok(n) = self.inner.read(&mut chunk) {
            if n == 0 {
                self.rem_size = 0;
                return None;
            }
            if n != self.chunk_size {
                chunk.resize(n, 0);
            }
            self.rem_size -= n as u64;
            return Some(chunk);
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.rem_size == 0 {
            return (0, Some(0));
        }
        let size = (self.rem_size / self.chunk_size as u64) as usize;
        (size, Some(size))
    }
}

pub trait Storer {
    fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()>;
}

pub struct DagBuilder<C, S> {
    chunks: C,
    store: S,
    max_links: usize,
}

impl<C, S> DagBuilder<C, S>
where
    C: Iterator<Item = Vec<u8>>,
    S: Storer,
{
    pub fn new(chunks: C, store: S) -> DagBuilder<C, S> {
        DagBuilder {
            chunks,
            store,
            max_links: 11,
        }
    }

    pub fn trickle(&mut self) -> Result<DagInfo> {
        let mut node = Node::with_links_cap(self.max_links);
        while let Some(data) = self.chunks.next() {
            let hash: Multihash = Code::Sha2_256.digest(&data);
            let cid = Cid::new_v1(0x55, hash);
            self.store.put_keyed(&cid, &data)?;
            node.links.push(cid.into());
        }
        let enc = serde_ipld_dagcbor::to_vec(&node)?;
        let root = Cid::new_v1(DAG_CBOR, Code::Sha2_256.digest(&enc));
        self.store.put_keyed(&root, &enc)?;
        Ok(DagInfo {
            root,
            leaves: node.links.len(),
            root_size: enc.len(),
        })
    }
}

#[derive(Debug)]
pub struct DagInfo {
    pub root: Cid,
    pub leaves: usize,
    pub root_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs::File;

    #[derive(Debug, Default, Clone)]
    struct MemoryBlockstore {
        blocks: RefCell<HashMap<Cid, Vec<u8>>>,
    }

    impl MemoryBlockstore {
        pub fn new() -> Self {
            Self::default()
        }
    }

    impl Storer for MemoryBlockstore {
        fn put_keyed(&self, k: &Cid, block: &[u8]) -> Result<()> {
            self.blocks.borrow_mut().insert(*k, block.into());
            Ok(())
        }
    }

    #[test]
    fn chunk_file() {
        let dir = env!("CARGO_MANIFEST_DIR");
        let mut reader =
            ChunkReader::<File>::from_file(format!("{}/src/fixture.txt", dir)).unwrap();
        reader.set_chunk_size(1 << 10);
        for chunk in reader {
            println!("length {}", chunk.len());
        }
    }

    #[test]
    fn chunk_slice() {
        let mut bytes = vec![0u8; 1 << 20];

        thread_rng().fill(&mut bytes[..]);

        let mut reader = ChunkReader::new(&bytes[..]);
        reader.set_content_size(bytes.len() as u64);
        for chunk in reader {
            println!("length {}", chunk.len());
        }
    }

    #[test]
    fn build_trickle() {
        let mut bytes = vec![0u8; 1 << 20];

        thread_rng().fill(&mut bytes[..]);

        let mut reader = ChunkReader::new(&bytes[..]);
        reader.set_content_size(bytes.len() as u64);

        let store = MemoryBlockstore::new();

        let mut dag = DagBuilder::new(reader, store);
        let root = dag.trickle().expect("failed to compute trickle dag");
        println!("root {:?}", root);
    }
}

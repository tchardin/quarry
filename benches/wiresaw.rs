use anyhow::Result;
use cid::Cid;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main, BatchSize, Throughput};
use rand::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use wiresaw::{ChunkReader, DagBuilder, Storer};

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

fn prepare_rand_data(size: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; size];
    thread_rng().fill(&mut bytes[..]);
    bytes
}

fn bench_dag_builder(c: &mut Criterion) {
    static MB: usize = 1024 * 1024;

    let mut group = c.benchmark_group("dag_builder");
    for size in [MB, 4 * MB, 15 * MB, 60 * MB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("data", size), size, |b, &size| {
            b.iter_batched(
                || prepare_rand_data(size),
                |data| {
                    let mut reader = ChunkReader::new(&data[..]);
                    reader.set_content_size(size as u64);

                    let store = MemoryBlockstore::new();
                    let mut dag = DagBuilder::new(reader, store);
                    dag.trickle().expect("failed to compute dag root");
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_dag_builder);
criterion_main!(benches);

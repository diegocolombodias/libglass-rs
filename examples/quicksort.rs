//! Parallel quicksort, based on the demo of the C++ library: the master splits a
//! vector around pivots, each worker sorts one part, and the master joins them.
//!
//! Each node is a thread here, but they only talk through the network, so they
//! could be on different machines.
//!
//! ```text
//! cargo run --example quicksort -- [elements] [workers]
//! ```

use libglass::{Barrier, GlassClient, GlassServer, Messenger, Mode, NodeId};
use std::error::Error;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(60);

/// Moves the elements smaller than a pivot to its left, and the rest to its right.
/// It returns where the pivot ended up.
fn partition(data: &mut [i64]) -> usize {
    let last = data.len() - 1;
    data.swap(last / 2, last);
    let mut store = 0;
    for i in 0..last {
        if data[i] < data[last] {
            data.swap(i, store);
            store += 1;
        }
    }
    data.swap(store, last);
    store
}

fn quicksort(data: &mut [i64]) {
    if data.len() > 1 {
        let pivot = partition(data);
        let (left, right) = data.split_at_mut(pivot);
        quicksort(left);
        quicksort(&mut right[1..]);
    }
}

/// Splits into `parts` slices by partitioning the largest one until there are enough.
fn split(data: &mut [i64], parts: usize) -> Vec<Vec<i64>> {
    let mut pieces = vec![data.to_vec()];
    while pieces.len() < parts {
        let largest = (0..pieces.len())
            .max_by_key(|&i| pieces[i].len())
            .expect("there is always a piece");
        let mut piece = pieces.remove(largest);
        if piece.len() < 2 {
            pieces.insert(largest, piece);
            pieces.push(Vec::new());
            continue;
        }
        let pivot = partition(&mut piece);
        let right = piece.split_off(pivot);
        pieces.insert(largest, right);
        pieces.insert(largest, piece);
    }
    pieces
}

fn worker(addr: SocketAddr, name: String) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = GlassClient::connect(addr, &name)?;
    let messenger = Messenger::<Vec<i64>>::new(&client, "sort", Mode::Local)?;
    let barrier = Barrier::new(&client, 0, 0, true, Mode::Local)?;
    thread::sleep(Duration::from_millis(300));
    barrier.sync()?;

    let (master, mut part) = messenger.get_timeout(TIMEOUT)?;
    quicksort(&mut part);
    messenger.send(master, &part)?;
    barrier.sync()?;
    Ok(())
}

fn master(
    addr: SocketAddr,
    elements: usize,
    workers: usize,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = GlassClient::connect(addr, "master")?;
    let messenger = Messenger::<Vec<i64>>::new(&client, "sort", Mode::Local)?;
    let barrier = Barrier::new(&client, 0, 0, true, Mode::Local)?;
    thread::sleep(Duration::from_millis(300));
    barrier.sync()?;

    let mut data = generate(elements);
    let started = Instant::now();

    let others: Vec<NodeId> = client
        .client_list()
        .into_keys()
        .filter(|&id| id != client.id())
        .collect();
    assert_eq!(others.len(), workers, "every worker must be connected");

    let parts = split(&mut data, workers);
    for (&id, part) in others.iter().zip(&parts) {
        messenger.send(id, part)?;
    }

    // The pieces are in order, so the answers are joined in the order they went.
    let mut sorted = Vec::with_capacity(elements);
    for &id in &others {
        sorted.extend(messenger.get_from_timeout(id, TIMEOUT)?);
    }
    barrier.sync()?;

    assert_eq!(sorted.len(), elements, "no element may be lost");
    assert!(sorted.windows(2).all(|w| w[0] <= w[1]), "it must be sorted");
    println!(
        "sorted {elements} elements with {workers} workers in {:?}",
        started.elapsed()
    );
    Ok(())
}

/// Numbers in a fixed pseudorandom sequence, so that every run is the same.
fn generate(elements: usize) -> Vec<i64> {
    let mut state = 42u64;
    (0..elements)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as i64
        })
        .collect()
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let elements: usize = args.next().map_or(Ok(100_000), |a| a.parse())?;
    let workers: usize = args.next().map_or(Ok(2), |a| a.parse())?;
    if workers == 0 {
        return Err("at least one worker is needed".into());
    }

    let server = GlassServer::bind("127.0.0.1:0")?;
    let addr = server.local_addr();

    // The master comes first, so that it has the lowest id.
    let master_thread = thread::spawn(move || master(addr, elements, workers));
    let threads: Vec<_> = (0..workers)
        .map(|i| thread::spawn(move || worker(addr, format!("worker{i}"))))
        .collect();

    for thread in threads {
        thread.join().expect("a worker panicked")?;
    }
    master_thread.join().expect("the master panicked")
}

//! Memory shared by processes on the same machine, without going through the
//! network. This is the equivalent of `Interprocess` in the C++ library.

use crate::error::{Error, Result};
use memmap2::{MmapMut, MmapOptions};
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;

/// A named memory region shared among processes.
///
/// It is backed by a file in the temporary directory, which is mapped into each
/// process. Access is by copy and under a lock between processes, so a reader
/// never sees a write in progress.
///
/// The region outlives the processes that use it. Call
/// [`remove`](SharedRegion::remove) when it is no longer needed.
pub struct SharedRegion {
    map: MmapMut,
    file: File,
    name: String,
}

fn path_of(name: &str) -> Result<PathBuf> {
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !valid {
        return Err(Error::InvalidName);
    }
    Ok(std::env::temp_dir().join(format!("libglass-shm-{name}")))
}

impl SharedRegion {
    /// Opens a region, creating it with zeros if it does not exist. All who open
    /// it get the same memory. Names can have letters, digits, `_` and `-`.
    ///
    /// An existing region with a different size is an error.
    pub fn open(name: &str, size: usize) -> Result<SharedRegion> {
        if size == 0 {
            return Err(Error::InvalidSize(size));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_of(name)?)?;

        // Two processes creating it at the same time must agree on its size.
        file.lock()?;
        let existing = file.metadata()?.len();
        let result = if existing == 0 {
            file.set_len(size as u64)
        } else if existing == size as u64 {
            Ok(())
        } else {
            file.unlock()?;
            return Err(Error::InvalidSize(size));
        };
        file.unlock()?;
        result?;

        // SAFETY: the map is only accessed through this type, by copy and under
        // the file lock, and always within its bounds. The file is never resized
        // after it has been created, unless something outside of this library
        // changes it, which the name in the temporary directory makes unlikely.
        let map = unsafe { MmapOptions::new().len(size).map_mut(&file)? };
        Ok(SharedRegion {
            map,
            file,
            name: name.to_owned(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> usize {
        self.map.len()
    }

    fn check(&self, offset: usize, len: usize) -> Result<()> {
        match offset.checked_add(len) {
            Some(end) if end <= self.map.len() => Ok(()),
            _ => Err(Error::InvalidSize(offset.saturating_add(len))),
        }
    }

    /// Copies a part of the region.
    pub fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        self.check(offset, len)?;
        self.file.lock_shared()?;
        let data = self.map[offset..offset + len].to_vec();
        self.file.unlock()?;
        Ok(data)
    }

    /// Writes to a part of the region.
    pub fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        self.check(offset, data.len())?;
        self.file.lock()?;
        self.map[offset..offset + data.len()].copy_from_slice(data);
        self.file.unlock()?;
        Ok(())
    }

    /// Changes the region based on its current content, with no other process
    /// accessing it in the meantime.
    pub fn update<R>(&mut self, change: impl FnOnce(&mut [u8]) -> R) -> Result<R> {
        self.file.lock()?;
        let result = change(&mut self.map);
        self.file.unlock()?;
        Ok(result)
    }

    /// Deletes a region. Processes that have it open keep their access, but new
    /// ones get a new region.
    pub fn remove(name: &str) -> Result<()> {
        fs::remove_file(path_of(name)?)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique(name: &str) -> String {
        format!("test-{name}-{}", std::process::id())
    }

    #[test]
    fn two_handles_see_the_same_memory() {
        let name = unique("same");
        let mut a = SharedRegion::open(&name, 16).unwrap();
        let b = SharedRegion::open(&name, 16).unwrap();
        assert_eq!(b.read(0, 4).unwrap(), [0, 0, 0, 0]);
        a.write(1, &[1, 2]).unwrap();
        assert_eq!(b.read(0, 4).unwrap(), [0, 1, 2, 0]);
        drop((a, b));
        SharedRegion::remove(&name).unwrap();
    }

    #[test]
    fn invalid_arguments_are_rejected() {
        assert!(matches!(
            SharedRegion::open("../evil", 16),
            Err(Error::InvalidName)
        ));
        assert!(matches!(
            SharedRegion::open("", 16),
            Err(Error::InvalidName)
        ));

        let name = unique("sizes");
        assert!(matches!(
            SharedRegion::open(&name, 0),
            Err(Error::InvalidSize(0))
        ));
        let mut region = SharedRegion::open(&name, 8).unwrap();
        assert!(matches!(
            SharedRegion::open(&name, 16),
            Err(Error::InvalidSize(16))
        ));
        assert!(region.read(4, 8).is_err());
        assert!(region.write(usize::MAX, &[1, 2]).is_err());
        assert!(region.write(8, &[1]).is_err());
        region.write(7, &[1]).unwrap();
        drop(region);
        SharedRegion::remove(&name).unwrap();
    }

    #[test]
    fn update_is_atomic_between_handles() {
        let name = unique("atomic");
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let name = name.clone();
                std::thread::spawn(move || {
                    let mut region = SharedRegion::open(&name, 8).unwrap();
                    for _ in 0..500 {
                        region
                            .update(|bytes| {
                                let value = u64::from_ne_bytes((&*bytes).try_into().unwrap());
                                bytes.copy_from_slice(&(value + 1).to_ne_bytes());
                            })
                            .unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let region = SharedRegion::open(&name, 8).unwrap();
        let bytes = region.read(0, 8).unwrap();
        assert_eq!(u64::from_ne_bytes(bytes.try_into().unwrap()), 2000);
        drop(region);
        SharedRegion::remove(&name).unwrap();
    }
}

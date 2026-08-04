use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use crate::format::{PAGE_SIZE, PAGE_SIZE_U64};
use crate::{Error, Result};

/// Positioned, synchronous storage used by the database engine.
///
/// Reads and writes may complete only part of the supplied buffer. actdb
/// retries short transfers until the buffer is complete, or returns a
/// structured I/O error if a backend stops making progress. Implementations
/// must provide exclusive ownership for the lifetime of an open database.
/// A successful [`Storage::sync_all`] must make all preceding writes and length
/// changes durable according to the backend's documented lifecycle contract.
pub trait Storage: Send + Sync + 'static {
    /// Reads bytes beginning at `offset`, returning the transferred byte count.
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize>;
    /// Writes bytes beginning at `offset`, returning the transferred byte count.
    fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize>;
    /// Returns the current storage length in bytes.
    fn len(&self) -> io::Result<u64>;
    /// Reports whether the storage currently contains no bytes.
    fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|length| length == 0)
    }
    /// Changes the storage length to `length` bytes.
    fn set_len(&self, length: u64) -> io::Result<()>;
    /// Makes preceding data and metadata changes durable.
    fn sync_all(&self) -> io::Result<()>;
}

/// Native file-backed storage with an exclusive advisory lock.
///
/// The lock remains held until this value is dropped. Custom storage backends
/// are responsible for providing equivalent exclusive ownership themselves.
#[derive(Debug)]
pub struct FileStorage {
    file: File,
    path: PathBuf,
    created: bool,
}

impl FileStorage {
    pub(crate) fn open(path: &Path, create: bool) -> crate::Result<Self> {
        let existed = path.exists();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(create);
        let file = options.open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(crate::Error::Locked),
            Err(std::fs::TryLockError::Error(error)) => return Err(crate::Error::Io(error)),
        }
        Ok(Self {
            file,
            path: path.to_path_buf(),
            created: !existed,
        })
    }

    pub(crate) fn sync_parent_if_created(&self) -> crate::Result<()> {
        if self.created {
            sync_parent(&self.path)?;
        }
        Ok(())
    }
}

pub(crate) fn sync_parent(path: &Path) -> crate::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

impl Storage for FileStorage {
    #[cfg(unix)]
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(&self.file, buffer, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(&self.file, buffer, offset)
    }

    #[cfg(unix)]
    fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::write_at(&self.file, buffer, offset)
    }

    #[cfg(windows)]
    fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_write(&self.file, buffer, offset)
    }

    fn len(&self) -> io::Result<u64> {
        self.file.metadata().map(|metadata| metadata.len())
    }

    fn set_len(&self, length: u64) -> io::Result<()> {
        self.file.set_len(length)
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

impl Storage for File {
    #[cfg(unix)]
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buffer, offset)
    }
    #[cfg(windows)]
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(self, buffer, offset)
    }
    #[cfg(unix)]
    fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::write_at(self, buffer, offset)
    }
    #[cfg(windows)]
    fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_write(self, buffer, offset)
    }
    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }
    fn set_len(&self, length: u64) -> io::Result<()> {
        File::set_len(self, length)
    }
    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }
}

pub(crate) fn read_page<I: Storage + ?Sized>(io: &I, page_id: u64) -> Result<[u8; PAGE_SIZE]> {
    let mut page = [0_u8; PAGE_SIZE];
    read_exact_at(
        io,
        &mut page,
        page_id
            .checked_mul(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Corrupt("page offset overflow".into()))?,
    )?;
    Ok(page)
}

pub(crate) fn write_page<I: Storage + ?Sized>(
    io: &I,
    page_id: u64,
    page: &[u8; PAGE_SIZE],
) -> Result<()> {
    write_all_at(
        io,
        page,
        page_id
            .checked_mul(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Corrupt("page offset overflow".into()))?,
    )
}

pub(crate) fn len<I: Storage + ?Sized>(io: &I) -> Result<u64> {
    io.len().map_err(Error::from)
}

pub(crate) fn set_len<I: Storage + ?Sized>(io: &I, length: u64) -> Result<()> {
    io.set_len(length).map_err(Error::from)
}

pub(crate) fn sync_all<I: Storage + ?Sized>(io: &I) -> Result<()> {
    io.sync_all().map_err(Error::from)
}

fn read_exact_at<I: Storage + ?Sized>(io: &I, mut buffer: &mut [u8], mut offset: u64) -> Result<()> {
    while !buffer.is_empty() {
        let read = io.read_at(buffer, offset)?;
        if read == 0 {
            return Err(Error::Io(io::ErrorKind::UnexpectedEof.into()));
        }
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| Error::Corrupt("read offset overflow".into()))?;
        buffer = &mut buffer[read..];
    }
    Ok(())
}

fn write_all_at<I: Storage + ?Sized>(io: &I, mut buffer: &[u8], mut offset: u64) -> Result<()> {
    while !buffer.is_empty() {
        let written = io.write_at(buffer, offset)?;
        if written == 0 {
            return Err(Error::Io(io::ErrorKind::WriteZero.into()));
        }
        offset = offset
            .checked_add(written as u64)
            .ok_or_else(|| Error::Corrupt("write offset overflow".into()))?;
        buffer = &buffer[written..];
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod fault {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct State {
        durable: Vec<u8>,
        volatile: Vec<u8>,
        operations: usize,
        fail_at: Option<usize>,
        maximum_transfer: Option<usize>,
        pending_writes: Vec<(usize, Vec<u8>)>,
    }

    #[derive(Default)]
    pub(crate) struct FaultDisk {
        state: Mutex<State>,
    }

    impl FaultDisk {
        pub(crate) fn from_durable(bytes: Vec<u8>) -> Self {
            Self {
                state: Mutex::new(State {
                    durable: bytes.clone(),
                    volatile: bytes,
                    ..State::default()
                }),
            }
        }

        pub(crate) fn durable_image(&self) -> Vec<u8> {
            self.state.lock().unwrap().durable.clone()
        }

        pub(crate) fn fail_at(&self, operation: usize) {
            let mut state = self.state.lock().unwrap();
            state.fail_at = Some(state.operations + operation);
        }

        pub(crate) fn limit_transfers(&self, bytes: usize) {
            self.state.lock().unwrap().maximum_transfer = Some(bytes);
        }

        pub(crate) fn crash(&self) {
            let mut state = self.state.lock().unwrap();
            state.volatile = state.durable.clone();
            state.pending_writes.clear();
            state.operations = 0;
            state.fail_at = None;
        }

        pub(crate) fn crash_with_torn_last_write(&self, persisted_bytes: usize) {
            let mut state = self.state.lock().unwrap();
            if let Some((offset, bytes)) = state.pending_writes.last().cloned() {
                let count = persisted_bytes.min(bytes.len());
                let end = offset + count;
                let length = state.durable.len().max(end);
                state.durable.resize(length, 0);
                state.durable[offset..end].copy_from_slice(&bytes[..count]);
            }
            state.volatile = state.durable.clone();
            state.pending_writes.clear();
            state.operations = 0;
            state.fail_at = None;
        }

        pub(crate) fn crash_with_reordered_write(&self, write_index: usize) {
            let mut state = self.state.lock().unwrap();
            if let Some((offset, bytes)) = state.pending_writes.get(write_index).cloned() {
                let end = offset + bytes.len();
                let length = state.durable.len().max(end);
                state.durable.resize(length, 0);
                state.durable[offset..end].copy_from_slice(&bytes);
            }
            state.volatile = state.durable.clone();
            state.pending_writes.clear();
            state.operations = 0;
            state.fail_at = None;
        }

        fn operation(state: &mut State) -> io::Result<()> {
            let operation = state.operations;
            state.operations += 1;
            if state.fail_at == Some(operation) {
                return Err(io::Error::other("injected I/O failure"));
            }
            Ok(())
        }
    }

    impl Storage for FaultDisk {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            let mut state = self.state.lock().unwrap();
            Self::operation(&mut state)?;
            let start = usize::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
            if start >= state.volatile.len() {
                return Ok(0);
            }
            let count = buffer
                .len()
                .min(state.volatile.len() - start)
                .min(state.maximum_transfer.unwrap_or(usize::MAX));
            buffer[..count].copy_from_slice(&state.volatile[start..start + count]);
            Ok(count)
        }

        fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize> {
            let mut state = self.state.lock().unwrap();
            Self::operation(&mut state)?;
            let start = usize::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
            let count = buffer
                .len()
                .min(state.maximum_transfer.unwrap_or(usize::MAX));
            let end = start
                .checked_add(count)
                .ok_or(io::ErrorKind::InvalidInput)?;
            let length = state.volatile.len().max(end);
            state.volatile.resize(length, 0);
            state.volatile[start..end].copy_from_slice(&buffer[..count]);
            state.pending_writes.push((start, buffer[..count].to_vec()));
            Ok(count)
        }

        fn len(&self) -> io::Result<u64> {
            Ok(self.state.lock().unwrap().volatile.len() as u64)
        }

        fn set_len(&self, length: u64) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            Self::operation(&mut state)?;
            state.volatile.resize(length as usize, 0);
            Ok(())
        }

        fn sync_all(&self) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            Self::operation(&mut state)?;
            state.durable = state.volatile.clone();
            state.pending_writes.clear();
            Ok(())
        }
    }

    #[test]
    fn crash_should_discard_unsynchronized_writes() -> Result<()> {
        let disk = FaultDisk::default();
        disk.set_len(PAGE_SIZE_U64)?;
        disk.sync_all()?;
        write_page(&disk, 0, &[7; PAGE_SIZE])?;
        disk.crash();
        assert_eq!(read_page(&disk, 0)?, [0; PAGE_SIZE]);
        Ok(())
    }

    #[test]
    fn page_io_should_complete_short_transfers_and_surface_failures() -> Result<()> {
        let disk = FaultDisk::default();
        disk.limit_transfers(17);
        write_page(&disk, 0, &[9; PAGE_SIZE])?;
        assert_eq!(read_page(&disk, 0)?, [9; PAGE_SIZE]);
        disk.fail_at(0);
        assert!(matches!(read_page(&disk, 0), Err(Error::Io(_))));
        Ok(())
    }

    #[test]
    fn crash_should_support_torn_and_reordered_persistence() -> Result<()> {
        let torn = FaultDisk::default();
        torn.set_len(PAGE_SIZE_U64)?;
        torn.sync_all()?;
        torn.write_at(&[3; 32], 64)?;
        torn.crash_with_torn_last_write(7);
        let mut bytes = [0; 32];
        torn.read_at(&mut bytes, 64)?;
        assert_eq!(&bytes[..7], &[3; 7]);
        assert_eq!(&bytes[7..], &[0; 25]);

        let reordered = FaultDisk::default();
        reordered.set_len(PAGE_SIZE_U64)?;
        reordered.sync_all()?;
        reordered.write_at(&[1; 8], 0)?;
        reordered.write_at(&[2; 8], 16)?;
        reordered.crash_with_reordered_write(1);
        let mut bytes = [0; 24];
        reordered.read_at(&mut bytes, 0)?;
        assert_eq!(&bytes[..8], &[0; 8]);
        assert_eq!(&bytes[16..], &[2; 8]);
        Ok(())
    }
}

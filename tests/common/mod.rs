//! Independent raw-file helpers shared by persistent-format tests.

#![allow(
    dead_code,
    reason = "each integration-test crate uses a different subset of helpers"
)]

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub const CHECKSUM_OFFSET: usize = PAGE_SIZE - 4;

pub fn open_rw(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

#[cfg(unix)]
pub fn read_page(file: &File, page_id: u64) -> io::Result<[u8; PAGE_SIZE]> {
    use std::os::unix::fs::FileExt;

    let mut page = [0_u8; PAGE_SIZE];
    file.read_exact_at(&mut page, page_id * PAGE_SIZE as u64)?;
    Ok(page)
}

#[cfg(windows)]
pub fn read_page(file: &File, page_id: u64) -> io::Result<[u8; PAGE_SIZE]> {
    use std::os::windows::fs::FileExt;

    let mut page = [0_u8; PAGE_SIZE];
    let mut read = 0;
    while read < PAGE_SIZE {
        let count = file.seek_read(&mut page[read..], page_id * PAGE_SIZE as u64 + read as u64)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        read += count;
    }
    Ok(page)
}

#[cfg(unix)]
pub fn write_page(file: &File, page_id: u64, page: &[u8; PAGE_SIZE]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.write_all_at(page, page_id * PAGE_SIZE as u64)
}

#[cfg(windows)]
pub fn write_page(file: &File, page_id: u64, page: &[u8; PAGE_SIZE]) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    let mut written = 0;
    while written < PAGE_SIZE {
        let count = file.seek_write(
            &page[written..],
            page_id * PAGE_SIZE as u64 + written as u64,
        )?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        written += count;
    }
    Ok(())
}

pub fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap())
}

pub fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

pub fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

pub fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

pub fn finish_page(page: &mut [u8; PAGE_SIZE]) {
    let checksum = crc32fast::hash(&page[..CHECKSUM_OFFSET]);
    put_u32(page, CHECKSUM_OFFSET, checksum);
}

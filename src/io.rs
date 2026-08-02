use std::fs::File;

use crate::format::{PAGE_SIZE, PAGE_SIZE_U64};
use crate::{Error, Result};

pub(crate) fn read_page(file: &File, page_id: u64) -> Result<[u8; PAGE_SIZE]> {
    let mut page = [0_u8; PAGE_SIZE];
    read_exact_at(
        file,
        &mut page,
        page_id
            .checked_mul(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Corrupt("page offset overflow".into()))?,
    )?;
    Ok(page)
}

pub(crate) fn write_page(file: &File, page_id: u64, page: &[u8; PAGE_SIZE]) -> Result<()> {
    write_all_at(
        file,
        page,
        page_id
            .checked_mul(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Corrupt("page offset overflow".into()))?,
    )
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buffer, offset).map_err(Error::from)
}

#[cfg(unix)]
fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buffer, offset).map_err(Error::from)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;
    while !buffer.is_empty() {
        let read = file.seek_read(buffer, offset)?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            )));
        }
        offset += read as u64;
        buffer = &mut buffer[read..];
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buffer: &[u8], mut offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;
    while !buffer.is_empty() {
        let written = file.seek_write(buffer, offset)?;
        if written == 0 {
            return Err(Error::Io(std::io::Error::from(
                std::io::ErrorKind::WriteZero,
            )));
        }
        offset += written as u64;
        buffer = &buffer[written..];
    }
    Ok(())
}

use std::io;

/// Errors returned by actdb.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An operating-system I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// Another process already owns the database file.
    #[error("database is locked by another process")]
    Locked,
    /// The file is not an actdb database.
    #[error("invalid actdb file: {0}")]
    InvalidFormat(&'static str),
    /// The database uses a newer or otherwise unsupported format.
    #[error("unsupported actdb format version {0}")]
    UnsupportedVersion(u16),
    /// A persisted checksum, pointer, or structural invariant is invalid.
    #[error("database corruption: {0}")]
    Corrupt(String),
    /// A key exceeds the format limit.
    #[error("key is {actual} bytes; maximum is {maximum}")]
    KeyTooLarge {
        /// Supplied key length.
        actual: usize,
        /// Maximum accepted key length.
        maximum: usize,
    },
    /// A value exceeds the format limit.
    #[error("value is {actual} bytes; maximum is {maximum}")]
    ValueTooLarge {
        /// Supplied value length.
        actual: usize,
        /// Maximum accepted value length.
        maximum: usize,
    },
    /// An option is outside its accepted range.
    #[error("invalid option: {0}")]
    InvalidOption(&'static str),
    /// A transaction cannot continue after an earlier failure.
    #[error("transaction is no longer usable")]
    TransactionClosed,
    /// A scan cannot continue after an earlier traversal or seek failure.
    #[error("scan is no longer usable")]
    ScanClosed,
}

/// A result returned by actdb.
pub type Result<T> = std::result::Result<T, Error>;

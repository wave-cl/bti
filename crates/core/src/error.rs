/// What can go wrong in bti.
///
/// **Two variants are boxed and four are not**, which looks inconsistent and is
/// not: `redb::Error` and `redb::TransactionError` are 160 bytes each, and they
/// set the size of every `Result` in this crate — 176 bytes carried on every
/// success as well as every failure, on functions that return one from a hot
/// loop. The other four redb variants are 24 to 88 bytes and cost nothing worth
/// an indirection.
///
/// The `From` impls for the boxed pair are written out because `#[from]` would
/// generate a conversion from `Box<redb::Error>`, which is not the type `?`
/// produces at a call site.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[source] Box<redb::Error>),
    #[error("storage: {0}")]
    DatabaseError(#[from] redb::DatabaseError),
    #[error("storage: {0}")]
    TableError(#[from] redb::TableError),
    #[error("storage: {0}")]
    TransactionError(#[source] Box<redb::TransactionError>),
    #[error("storage: {0}")]
    CommitError(#[from] redb::CommitError),
    #[error("storage: {0}")]
    StorageError(#[from] redb::StorageError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("quinn write: {0}")]
    QuinnWrite(#[from] quinn::WriteError),
    #[error("quinn read: {0}")]
    QuinnReadExact(#[from] quinn::ReadExactError),
    #[error("invalid data: {0}")]
    InvalidData(String),
}

impl From<redb::Error> for Error {
    fn from(e: redb::Error) -> Error {
        Error::Storage(Box::new(e))
    }
}

impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Error {
        Error::TransactionError(Box::new(e))
    }
}

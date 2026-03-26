#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] redb::Error),
    #[error("storage: {0}")]
    DatabaseError(#[from] redb::DatabaseError),
    #[error("storage: {0}")]
    TableError(#[from] redb::TableError),
    #[error("storage: {0}")]
    TransactionError(#[from] redb::TransactionError),
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

/// An aborted or a committed value.
pub enum AbortOrCommit<T> {
    /// `Abort`ed transaction.
    Abort,
    /// `Commit`ted transaction with the data inside.
    Commit(T),
}

impl<T> AbortOrCommit<T> {
    /// Converts from `AbortOrCommit<T, E>` to `Option<T>`.
    #[inline]
    pub fn commit(self) -> Option<T> {
        match self {
            AbortOrCommit::Commit(data) => Some(data),
            AbortOrCommit::Abort => None,
        }
    }

    /// Returns `true` if the option is a `Abort` value.
    #[inline]
    pub fn is_abort(&self) -> bool {
        matches!(self, AbortOrCommit::Abort)
    }

    /// Returns the contained `Commit`ted value, consuming the self value.
    #[inline]
    #[track_caller]
    pub fn unwrap_commit(self) -> T {
        match self {
            AbortOrCommit::Commit(data) => data,
            AbortOrCommit::Abort => {
                panic!("called `CommitOrAbort::unwrap_commit()` on an `Abort` value")
            }
        }
    }
}

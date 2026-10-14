use std::fmt;

/// An aborted or a committed value.
pub enum CommitOrAbort<T, A> {
    /// `Commit`ted transaction with the data inside.
    Commit(T),
    /// `Abort`ed transaction.
    Abort(A),
}

impl<T, A> CommitOrAbort<T, A> {
    /// Converts from `CommitOrAbort<T, E>` to `Option<T>`.
    #[inline]
    pub fn commit(self) -> Option<T> {
        match self {
            CommitOrAbort::Commit(data) => Some(data),
            CommitOrAbort::Abort(_) => None,
        }
    }

    /// Returns `true` if the option is a `Abort` value.
    #[inline]
    pub fn abort(self) -> Option<A> {
        match self {
            CommitOrAbort::Commit(_) => None,
            CommitOrAbort::Abort(data) => Some(data),
        }
    }

    /// Returns `true` if the option is a `Abort` value.
    #[inline]
    pub fn is_abort(&self) -> bool {
        matches!(self, CommitOrAbort::Abort(_))
    }

    /// Returns the contained `Commit`ted value, consuming the self value.
    #[inline]
    #[track_caller]
    pub fn unwrap_commit(self) -> T
    where
        A: fmt::Debug,
    {
        match self {
            CommitOrAbort::Commit(data) => data,
            CommitOrAbort::Abort(abort) => {
                panic!("called `CommitOrAbort::unwrap_commit()` on an `Abort` value: {abort:?}")
            }
        }
    }
}

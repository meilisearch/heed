use std::fmt;

/// An aborted or a committed value.
pub enum CommitOrAbort<T, A> {
    /// `Commit`ted transaction with the data inside.
    Commit(T),
    /// `Abort`ed transaction.
    Abort(A),
}

impl<T, A> CommitOrAbort<T, A> {
    /// Returns `Some` if the `CommitOrAbort` is a `Commit` value.
    #[inline]
    pub fn commit(self) -> Option<T> {
        match self {
            CommitOrAbort::Commit(data) => Some(data),
            CommitOrAbort::Abort(_) => None,
        }
    }

    /// Returns `Some` if the `CommitOrAbort` is an `Abort` value.
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
    ///
    /// # Panics
    ///
    /// Panics if the self value equals `Abort`.
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

    /// Returns the contained `Abort`ted value, consuming the self value.
    ///
    /// # Panics
    ///
    /// Panics if the self value equals `Commit`.
    #[inline]
    #[track_caller]
    pub fn unwrap_abort(self) -> A
    where
        T: fmt::Debug,
    {
        match self {
            CommitOrAbort::Commit(data) => {
                panic!("called `CommitOrAbort::unwrap_abort()` on a `Commit` value: {data:?}")
            }
            CommitOrAbort::Abort(abort) => abort,
        }
    }
}

impl<T, E> From<Result<T, E>> for CommitOrAbort<T, E> {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(data) => CommitOrAbort::Commit(data),
            Err(error) => CommitOrAbort::Abort(error),
        }
    }
}

impl<T, E> From<CommitOrAbort<T, E>> for Result<T, E> {
    fn from(coa: CommitOrAbort<T, E>) -> Self {
        match coa {
            CommitOrAbort::Commit(data) => Ok(data),
            CommitOrAbort::Abort(abort) => Err(abort),
        }
    }
}

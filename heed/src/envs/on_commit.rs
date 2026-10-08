use crate::Env;

/// A provided token used to make sure databases
/// can be used outside of the current transaction.
pub struct CommitToken {
    /// This is the env identifier to make sure that the env
    /// used to commit a database
    env_ident: usize,
}

impl CommitToken {
    pub(crate) fn new<T>(env: &Env<T>) -> CommitToken {
        CommitToken { env_ident: env.env_mut_ptr().as_ptr() as _ }
    }

    pub(crate) fn env_ident(&self) -> usize {
        self.env_ident
    }
}

/// A trait that must be implemented to be able to
/// convert databases from local ones to static ones.
pub trait OnCommit {
    /// The output struct after a commit use successful.
    type Committed;

    /// Convert a struct after the commit is successful.
    fn on_commit(self, token: &CommitToken) -> Self::Committed;
}

impl OnCommit for () {
    type Committed = ();

    fn on_commit(self, _token: &CommitToken) -> Self::Committed {
        ()
    }
}

impl<A: OnCommit> OnCommit for Option<A> {
    type Committed = Option<A::Committed>;

    fn on_commit(self, token: &CommitToken) -> Self::Committed {
        self.map(|a| a.on_commit(token))
    }
}

macro_rules! impl_on_commit_for_tuple {
    ( $( $name:ident )+ ) => {
        impl<$($name: OnCommit),+> OnCommit for ($($name,)+) {
            type Committed = ($($name::Committed,)+);

            // Allow non snake case identifier as we use the
            // struct names, i.e. A, B, to decompose the tuple.
            #[allow(non_snake_case)]
            fn on_commit(self, token: &crate::CommitToken) -> Self::Committed {
                let ($($name,)+) = self;
                ($($name.on_commit(token),)+)
            }
        }
    };
}

impl_on_commit_for_tuple! { A }
impl_on_commit_for_tuple! { A B }
impl_on_commit_for_tuple! { A B C }
impl_on_commit_for_tuple! { A B C D }
impl_on_commit_for_tuple! { A B C D E }
impl_on_commit_for_tuple! { A B C D E F }
impl_on_commit_for_tuple! { A B C D E F G }
impl_on_commit_for_tuple! { A B C D E F G H }
impl_on_commit_for_tuple! { A B C D E F G H I }
impl_on_commit_for_tuple! { A B C D E F G H I J }
impl_on_commit_for_tuple! { A B C D E F G H I J K }
impl_on_commit_for_tuple! { A B C D E F G H I J K L }

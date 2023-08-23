//! The set of possible iteration method for the different iterators.

use crate::cursor::MoveOperation;

/// The trait used to define the way iterators behaves.
pub trait IterationMethod {
    /// The internal operation to move the cursor throught entries.
    const MOVE_OPERATION: MoveOperation;
}

/// Moves to the next or previous key if there
/// is no more values associated to the current key.
#[derive(Debug, Clone, Copy)]
pub enum MoveThroughDuplicateValues {}

impl IterationMethod for MoveThroughDuplicateValues {
    const MOVE_OPERATION: MoveOperation = MoveOperation::Any;
}

/// Moves between keys and ignore the duplicate values of keys.
#[derive(Debug, Clone, Copy)]
pub enum MoveBetweenKeys {}

impl IterationMethod for MoveBetweenKeys {
    const MOVE_OPERATION: MoveOperation = MoveOperation::NoDup;
}

/// Moves only on the duplicate values of a given key and ignore other keys.
#[derive(Debug, Clone, Copy)]
pub enum MoveOnCurrentKeyDuplicates {}

impl IterationMethod for MoveOnCurrentKeyDuplicates {
    const MOVE_OPERATION: MoveOperation = MoveOperation::Dup;
}

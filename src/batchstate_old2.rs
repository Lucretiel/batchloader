use futures_timer::Delay;
use std::cmp::{Ord, Ordering};
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::hash::Hash;
use std::marker::Unpin;
use std::pin::Pin;
use std::sync::Weak;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::data::{KeySet, Token, ValueSet};
use futures::future::{FutureExt, Shared};
use futures::ready;

use Poll::Ready;

/// The result of trying to add a key to a BatchState. This operation will
/// fail if the batchstate has already launched and is InProgress or Done. If
/// the BatchState is still accumulating keys, it will succeed, but if it hits
/// the key limit, it will immediately launch the request and not accept any
/// more keys, indicated by AddedLast.
#[derive(Debug)]
pub(crate) enum AddKeyResult<Key> {
    Added(Token),
    AddedLast(Token),
    Fail(Key),
}

#[derive(Debug)]
enum BatchStateInner<Key, Fut: Future> {
    Accumulating(KeySet<Key>),
    InProgress(Shared<Fut>),
}

#[derive(Debug)]
pub(crate) struct BatchState<Key, Fut: Future> {
    inner: BatchStateInner<Key, Fut>,
}

impl<Key: Hash + Eq, Fut: Future> BatchState<Key, Fut> {
    /// Create a new BatchState for a set of keys.
    ///
    /// Note that it is a logic error (resulting in a panic) to poll a batch
    /// state before any keys have been added to it with add_key.
    #[inline]
    pub(crate) fn new(load: &'a Load, duration: Duration) -> Self {
        Self {
            inner: BatchStateInner::Accumulating(KeySet::new()),
        }
    }

    /// Attempt to add a key to an accumulating batch state. Returns the result
    /// of the addition:
    ///
    /// - If the state is not accumulating, return a failure.
    /// - If the state is accumulating but there are already at the maximum
    /// number of keys, return a failire
    ///   - If we're over the max, panic.
    /// - If the key was added and the max was reached, return AddedLast.
    ///   - In this case, the internal timer will be reset so that the batch
    ///      is launched immediately.
    /// - If the key was added but there's still room, return Added.
    ///
    /// This method will panic if it somehow managed to add a key above
    /// max_keys, because doing so is almost certainly a logic error (probably
    /// a max_keys that is too small (0 or 1), or somehow an initial keyset
    /// was added with too many keys.). The easiest way to avoid this panic
    /// is to drop your reference to a BatchState as soon as you see an
    /// AddedLast or
    pub(crate) fn add_key(&mut self, key: Key, max_keys: usize) -> AddKeyResult<Key> {
        use AddKeyResult::*;
        use BatchStateInner::*;

        match self.inner {
        	Accumulating(ref mut keys) =>
        }

        match self.inner {
            Accumulating {
                ref mut keys,
                ref mut delay,
                ..
            } => match keys.len().cmp(&max_keys) {
                Ordering::Greater => panic!("There are many keys in this BatchState"),
                Ordering::Equal => Fail(key),
                Ordering::Less => {
                    let token = keys.add_key(key);
                    match keys.len().cmp(&max_keys) {
                        Ordering::Greater => unreachable!(),
                        Ordering::Equal => {
                            // Reset the delay, so that the future is re-polled
                            // immediately.
                            delay.reset(Duration::from_secs(0));
                            AddedLast(token)
                        }
                        Ordering::Less => Added(token),
                    }
                }
            },
            _ => AddKeyResult::Fail(key),
        }
    }
}

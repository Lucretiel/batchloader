use futures_timer::Delay;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::hash::Hash;
use std::marker::Unpin;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::cmp::{Ord, Ordering};

use crate::data::{KeySet, Token, ValueSet};
use futures::future::FutureExt;
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

enum BatchStateInner<'a, Key, Value, Error, Load, Fut> {
    /// We're still in the window where new requests are coming in
    Accumulating {
        load: &'a Load,
        keys: KeySet<Key>,
        delay: Delay,
    },

    /// The request has been sent as is pending
    InProgress(Fut),

    /// The request completed
    Done(Result<ValueSet<Value>, Error>),
}

impl<'a, Key, Value, Error, Load, Fut> Debug for BatchStateInner<'a, Key, Value, Error, Load, Fut>
where
    Key: Debug + Eq + Hash,
    Value: Debug,
    Error: Debug,
    Fut: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use BatchStateInner::*;

        match self {
            Accumulating { keys, delay, .. } => f
                .debug_struct("Accumulating")
                .field("keys", &keys)
                .field("delay", &delay)
                .field("load:", &"<closure>")
                .finish(),
            InProgress(fut) => f.debug_tuple("InProgress").field(&fut).finish(),
            Done(result) => f.debug_tuple("Done").field(&result).finish(),
        }
    }
}

/// A BatchState is a Future-like object that encodes the state of a single
/// collection of keys through its lifespan of accumulating keys, issuing
/// a single batched request, and distributing the results to the individual
/// futures.
///
/// A set of BatchFutures shares ownerhip of a single BatchState. There is
/// no background execution; all the polling is driven by the individual
/// futures.
pub(crate) struct BatchState<'a, Key, Value, Error, Load, Fut> {
    inner: BatchStateInner<'a, Key, Value, Error, Load, Fut>,
}

impl<'a, Key, Value, Error, Load, Fut> Debug for BatchState<'a, Key, Value, Error, Load, Fut>
where
    Key: Debug + Eq + Hash,
    Value: Debug,
    Error: Debug,
    Fut: Debug,
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("BatchState")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<'a, Key, Value, Error, Load, Fut> BatchState<'a, Key, Value, Error, Load, Fut>
where
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    /// Create a new BatchState for a set of keys.
    ///
    /// Note that the `duration` timer will start as soon as this method is
    /// called; it does not wait until an .await to start the countdown.
    ///
    /// Note that it is a logic error (resulting in a panic) to poll a batch
    /// state before any keys have been added to it with add_key.
    #[inline]
    pub(crate) fn new(load: &'a Load, duration: Duration) -> Self {
        Self {
            inner: BatchStateInner::Accumulating {
                load,
                keys: KeySet::new(),
                delay: Delay::new(duration),
            },
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
        use BatchStateInner::*;
        use AddKeyResult::*;

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
                        Ordering::Less => Added(token)
                    }
                }
            },
            _ => AddKeyResult::Fail(key),
        }
    }

    // TODO: remove_token function, for when a Future drops. This is a pretty
    // minor optimization, because the arc takes care of dropping the state
    // entirely, so it only helps with:
    //
    // - Removing a key before a future is polled
    // - Preventing undue clones from the returned result.
    //
    // It would be easy but annoying to implement, because we have to track
    // dropped keys all throughout the life cycle of the state.

    /// Execute a poll This is pretty straightforward: wait for the timer, then
    /// use `load` to launch the request, then wait for the response. Ensure
    /// that `self` is updated appropriately throughout this process.
    pub(crate) fn poll<'s>(
        &'s mut self
        ctx: &mut Context,
    ) -> Poll<&'s mut Result<ValueSet<Value>, Error>> {
        // TODO: find a way to make this an async fn. The trouble is that our
        // clients need to be able to modify keys while we're in the accumulating
        // state.
        use BatchStateInner::*;

        let inner = &mut self.inner;

        if let Accumulating {
            ref mut keys,
            ref mut delay,
            load,
        } = *inner
        {
            assert_ne!(keys.len(), 0, "polled a BatchState with no keys");
            ready!(delay.poll_unpin(ctx));
            let keys = keys.take();
            let fut = load(keys);
            // This is where the pin happens, implicitly
            *inner = InProgress(fut);
        }

        if let InProgress(ref mut fut) = *inner {
            // Safety:
            let fut = unsafe { Pin::new_unchecked(fut) };
            let batch_result = ready!(fut.poll(ctx));

            *inner = Done(batch_result);
        }

        // This return of a reference is safe because the Result is known to
        // be Unpin
        if let Done(ref mut batch_result) = *inner {
            return Ready(batch_result);
        }

        unreachable!("Batch state was in an impossible state (not one of the enum variants)")
    }
}

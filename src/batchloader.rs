use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_timer::Delay;

use Poll::{Pending, Ready};

/// A batch future is a request for a single Key-Value lookup, which shares
/// its request with several other batch futures such that the request can
/// be Batched as [Keys] -> [Values]. It is created from a `Dispatcher`,
/// and when awaited, it will wait along with its other group of futures until
/// the window has passed, then execute the request and return the Value for
/// the specific key.
pub struct BatchFuture<'a, Key, Value, Error, Load, Fut> {
    token: Token,
    state: SharedBatchState<'a, Key, Value, Error, Load, Fut>,
}

impl<'a, Key, Value, Error, Load, Fut> BatchFuture<'a, Key, Value, Error, Load, Fut>
where
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    /// Note: make sure the BatchState invariants are upheld before calling
    /// this method. In paricular, each BatchFuture is guaranteed by the
    /// contract of this library to have an associated key in the BatchState.
    fn new(token: Token, state: Arc<Mutex<BatchState<'a, Key, Value, Error, Load, Fut>>>) {
        Self {
            token,
            state: Some(state),
        }
    }
}

impl<'a, Key, Value, Error, Load, Fut> Future for BatchFuture<'a, Key, Value, Error, Load, Fut>
where
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(KeySet<Key>) -> Fut,
    Fut: Future<Output = Result<ValueSet<Value>, Error>>,
{
    type Output = Result<Value, Error>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        match self.state.poll_token(self.token) {
            Pending => Pending,
            Ready(result) => {
                self.state.reset();
                result
            }
        }
    }
}

/// A set of configuration rules for a batcher. This defines the batch loading
/// async fn, as well as the durating of time to wait for keys
pub struct BatchRules<
    Key: Hash,
    Value: Clone,
    Error: Clone,
    Load: Fn(HashMap<Key, u32>) -> Fut,
    Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
> {
    max_keys: u32,
    window: Duration,
    load: Load,
}

impl<
        Key: Hash,
        Value: Clone,
        Error: Clone,
        Load: Fn(HashMap<Key, u32>) -> Fut,
        Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
    > BatchRules<Key, Value, Error, Load, Fut>
{
    fn new(max_keys: usize, window: Duration, load: Load) -> Self {
        Self {
            max_keys,
            window,
            load,
        }
    }

    fn dispatcher<'a>(&'a self) -> Dispatcher<'a, Key, Value, Error, Load, Fut> {
        Dispatcher {
            rules: self,
            state: Mutex::new(SharedBatchState { state: None }),
        }
    }
}

/// A dispatcher is the entry point for creating BatchFutures. It maintains
/// a "currently accumulating" state, and each time you call Dispatcher::load,
/// the key is added to that state, until:
///
/// - the states accumulation timer runs out (this is usually very short)
/// - the state reaches its maxumum keys
///
/// At this point the state will be "launched"– that is, it will be detached
/// from this dispatcher and replaced with a fresh ones. The futures associated
/// with the old state share ownership of it and drive it to completion,
/// independent of the dispatcher.
pub struct Dispatcher<
    'a,
    Key: Hash + Eq,
    Value: Clone,
    Error: Clone,
    Load: Fn(HashMap<Key, u32>) -> Fut,
    Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
> {
    rules: &'a BatchRules<Key, Value, Error, Load, Fut>,

    // TODO: replace this with an atomic pointer. Also, probably make it weak?
    // If all the futures drop their state references, there's no reason for
    // dispatcher to keep it around.
    state: Mutex<SharedBatchState<'a, Key, Value, Error, Load, Fut>>,
}

impl<
        'a,
        Key: Hash + Eq,
        Value: Clone,
        Error: Clone,
        Load: Fn(HashMap<Key, u32>) -> Fut,
        Fut: Future<Output = Result<HashMap<Key, Value>, Error>>,
    > Dispatcher<'a, Key, Value, Error, Load, Fut>
{
    fn load(&self, key: Key) -> BatchFuture<'a, Key, Value, Error, Load, Fut> {
        let state_lock = self.state.lock().unwrap();

        state_lock.add_key(key, self.rules)
    }
}
